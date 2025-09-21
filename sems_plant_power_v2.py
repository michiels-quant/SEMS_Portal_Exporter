# Copyright 2025 Steven Michiels
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#     http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


#!/usr/bin/env python3
from __future__ import annotations

import base64
import csv
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from pathlib import Path
from dotenv import load_dotenv  # pip install python-dotenv

ENV_FILE = os.getenv("example.env")  # optional override
if ENV_FILE:
    load_dotenv(ENV_FILE, override=False)
else:
    load_dotenv(Path(".env"), override=False)

# ========= Configuration (env) =========
ACCOUNT   = os.getenv("SEMS_ACCOUNT")
PASSWORD  = os.getenv("SEMS_PASSWORD")
PLANT_ID  = os.getenv("SEMS_STATION_ID")      # UUID from DevTools
START     = os.getenv("SEMS_START", "2025-09-01")
END       = os.getenv("SEMS_END",   dt.date.today().isoformat())
TZ_OFFSET = os.getenv("SEMS_TZ_OFFSET", "+08:00")  # keep what your portal used
OUTDIR    = Path(os.getenv("SEMS_OUT", "json_export"))
BASE_V2   = os.getenv("SEMS_BASE", "https://eu.semsportal.com/api/").rstrip("/") + "/"

SLEEP_SECONDS   = float(os.getenv("SEMS_SLEEP_SECONDS", "1.0"))
MAX_RETRIES     = int(os.getenv("SEMS_MAX_RETRIES", "2"))
RETRY_BASE      = 8
RETRY_MAX_DELAY = 120

UA = (
    "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Mobile Safari/537.36"
)
COMMON_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "en-GB,en;q=0.6",
    "Origin": "https://www.semsportal.com",
    "Referer": "https://www.semsportal.com/",
    "User-Agent": UA,
    "Content-Type": "application/json",
}

if not (ACCOUNT and PASSWORD and PLANT_ID):
    sys.exit("Set SEMS_ACCOUNT, SEMS_PASSWORD, SEMS_STATION_ID (and optionally SEMS_START/SEMS_END).")

OUTDIR.mkdir(exist_ok=True)

# ========= Small utils =========
def save_json(name: str, obj) -> None:
    (OUTDIR / name).write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

def save_text(name: str, txt: str) -> None:
    (OUTDIR / name).write_text(txt, encoding="utf-8")

def resolve_end(end_str: str) -> str:
    if end_str.strip().lower() in ("latest", "today", "now"):
        return dt.date.today().isoformat()
    return end_str

def daterange(start_d: dt.date, end_d: dt.date):
    d = start_d
    while d <= end_d:
        yield d
        d += dt.timedelta(days=1)

def ensure_date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)

# ========= Auth: v2 =========
def crosslogin_v2(account: str, password: str) -> Tuple[str, str]:
    url = "https://eu.semsportal.com/api/v2/Common/CrossLogin"
    h = dict(COMMON_HEADERS)
    r = requests.post(url, headers=h, json={"account": account, "pwd": password}, timeout=20)
    save_text("auth_v2_status.txt", f"{r.status_code}\n{r.text[:2000]}")
    r.raise_for_status()
    d = r.json()
    if d.get("hasError") or str(d.get("code")) != "0":
        raise RuntimeError(f"v2 CrossLogin failed: {d}")
    api_base = (d.get("api") or BASE_V2).rstrip("/") + "/"
    tok = d.get("data", {}).get("token")
    if not isinstance(tok, str) or not tok:
        raise RuntimeError("v2 CrossLogin returned no token")
    return api_base, tok

# ========= Auth: v1 → synthesize v2 token (fallback) =========
def crosslogin_v1_make_v2token(account: str, password: str) -> Tuple[str, str]:
    url = "https://www.semsportal.com/api/v1/Common/CrossLogin"
    hdr = json.dumps({"version": "", "client": "web", "language": "en"})
    h = dict(COMMON_HEADERS); h["Token"] = hdr
    r = requests.post(url, headers=h, json={"account": account, "pwd": password}, timeout=20)
    save_text("auth_v1_status.txt", f"{r.status_code}\n{r.text[:2000]}")
    r.raise_for_status()
    d = r.json()
    if d.get("hasError") or str(d.get("code")) != "0":
        raise RuntimeError(f"v1 CrossLogin failed: {d}")
    api_base = (d.get("api") or BASE_V2).rstrip("/") + "/"
    td = d.get("data") or {}
    bundle = {
        "uid": td.get("uid", ""),
        "timestamp": td.get("timestamp", int(time.time() * 1000)),
        "token": td.get("token") or td.get("Token") or "",
        "client": "web",
        "version": "",
        "language": "en",
    }
    raw = json.dumps(bundle, separators=(",", ":")).encode("utf-8")
    v2_token = base64.urlsafe_b64encode(raw).decode("ascii")
    return api_base, v2_token

def auth_any() -> Tuple[str, str]:
    try:
        return crosslogin_v2(ACCOUNT, PASSWORD)
    except Exception:
        return crosslogin_v1_make_v2token(ACCOUNT, PASSWORD)

# ========= API call =========
def get_plant_power_day(api_base: str, v2_token: str, plant_id: str, day: dt.date) -> dict:
    url = api_base.rstrip("/") + "/v2/Charts/GetPlantPowerChart"
    headers = dict(COMMON_HEADERS); headers["token"] = v2_token

    dt_iso = f"{day.isoformat()}T00:00:00{TZ_OFFSET}"
    bodies = [
        {"id": plant_id, "date": dt_iso, "full_script": False},
        {"model": {"id": plant_id, "date": dt_iso, "full_script": False}},
    ]

    last_err = None
    for i, body in enumerate(bodies, 1):
        try:
            r = requests.post(url, headers=headers, json=body, timeout=25)
            save_text(f"raw_v2_{day}_try{i}.txt", f"{r.status_code}\n{r.text[:2000]}")
            r.raise_for_status()
            j = r.json()
            if not j.get("hasError") and str(j.get("code")) == "0":
                return j
            last_err = j
        except Exception as e:
            last_err = {"error": str(e)}
    return last_err or {}

# ========= Flatten
def flatten_lines_xy(payload: dict) -> List[dict]:
    rows: List[dict] = []
    data = payload.get("data") or {}

    # daily aggregates
    gen = data.get("generateData")
    if isinstance(gen, list):
        for g in gen:
            k, v = g.get("key"), g.get("value")
            if k is not None and v is not None:
                rows.append({"series": f"aggregate:{k}", "timestamp": "daily", "value": v})

    # time series
    lines = data.get("lines")
    if isinstance(lines, list):
        for line in lines:
            lkey = line.get("key") or "line"
            xy = line.get("xy") or []
            if isinstance(xy, list):
                for pt in xy:
                    x = pt.get("x"); y = pt.get("y")
                    if x is not None and y is not None:
                        rows.append({"series": lkey, "timestamp": x, "value": y})
    return rows

# ========= Main batch =========
def main():
    
    start = ensure_date(START)
    end   = ensure_date(resolve_end(END))

    if start > end:
        raise SystemExit("SEMS_START must be <= SEMS_END")

    print("[*] Auth (v2 preferred)…")
    api_base, v2_token = auth_any()
    print(f"    API base: {api_base}")
    save_text("v2_token_header.txt", v2_token)

    csv_path = OUTDIR / "plant_power_v2.csv"
    need_header = not csv_path.exists()

    with csv_path.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["day", "series", "timestamp", "value"])
        if need_header:
            w.writeheader()

        for day in daterange(start, end):
            print(f"  {day} …")
            attempt = 0
            while True:
                j = get_plant_power_day(api_base, v2_token, PLANT_ID, day)
                save_json(f"raw_v2_{day}.json", j)

                rows = flatten_lines_xy(j)
                if rows:
                    for r in rows:
                        r["day"] = day.isoformat()
                        w.writerow(r)
                    print(f"    ✓ {len(rows):,} rows")
                    break

                attempt += 1
                if attempt > MAX_RETRIES:
                    print("    ! no rows (even after retries) — check raw_v2 JSON")
                    break

                delay = min(RETRY_BASE * attempt, RETRY_MAX_DELAY)
                print(f"    retry in {delay}s…")
                time.sleep(delay)

            time.sleep(SLEEP_SECONDS)

    print(f"[✓] Done → {csv_path}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit("Interrupted")
    except Exception as e:
        sys.exit(f"Error: {e}")

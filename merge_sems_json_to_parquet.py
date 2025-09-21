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
"""Merge SEMS raw JSON exports into a single Parquet file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, List

try:
    import pandas as pd
except ModuleNotFoundError as exc:  # pragma: no cover - import guard for convenience
    pd = None
    _PANDAS_IMPORT_ERROR = exc
else:
    _PANDAS_IMPORT_ERROR = None


def flatten_payload(payload: dict) -> List[dict]:
    rows: List[dict] = []
    data = payload.get("data") or {}

    aggregates = data.get("generateData")
    if isinstance(aggregates, list):
        for item in aggregates:
            key = item.get("key")
            value = item.get("value")
            if key is not None and value is not None:
                rows.append({"series": f"aggregate:{key}", "timestamp": "daily", "value": value})

    lines = data.get("lines")
    if isinstance(lines, list):
        for line in lines:
            line_key = line.get("key") or "line"
            points = line.get("xy") or []
            if isinstance(points, list):
                for point in points:
                    x = point.get("x")
                    y = point.get("y")
                    if x is not None and y is not None:
                        rows.append({"series": line_key, "timestamp": x, "value": y})

    return rows


def iter_payload_rows(files: Iterable[Path]) -> List[dict]:
    merged: List[dict] = []
    skipped = 0
    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            print(f"    ! skip {path.name}: {exc}")
            skipped += 1
            continue

        rows = flatten_payload(payload)
        if not rows:
            continue

        day = path.stem.split("raw_v2_")[-1]
        for row in rows:
            record = dict(row)
            record["day"] = day
            record["source"] = path.name
            merged.append(record)
    if skipped:
        print(f'    • skipped {skipped} files due to JSON decode errors')
    return merged


def build_dataframe(rows: List[dict]):
    if pd is None:
        raise RuntimeError(f"pandas not available ({_PANDAS_IMPORT_ERROR}). Install pandas and pyarrow first.")
    if not rows:
        raise RuntimeError("No rows found in provided JSON files.")

    frame = pd.DataFrame(rows)
    frame["day"] = pd.to_datetime(frame["day"], errors="coerce")
    return frame


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "source",
        nargs="?",
        default="json_export_example",
        help="Directory containing raw_v2_*.json files (default: %(default)s)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="plant_power_v2.parquet",
        help="Destination Parquet filename (default: %(default)s)",
    )
    parser.add_argument(
        "--glob",
        default="raw_v2_*.json",
        help="Glob pattern for JSON files inside source directory (default: %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    src_dir = Path(args.source).expanduser().resolve()

    if not src_dir.exists() or not src_dir.is_dir():
        raise SystemExit(f"Source directory not found: {src_dir}")

    files = sorted(src_dir.glob(args.glob))
    if not files:
        raise SystemExit(f"No files matched pattern '{args.glob}' in {src_dir}")

    print(f"[*] Loading {len(files)} files from {src_dir}")

    rows = iter_payload_rows(files)
    if not rows:
        raise SystemExit("No data rows extracted; aborting Parquet export.")

    frame = build_dataframe(rows)

    output_path = src_dir / args.output if Path(args.output).name == args.output else Path(args.output)
    frame.to_parquet(output_path, index=False)

    print(f"[✓] Wrote {len(frame):,} rows to {output_path}")


if __name__ == "__main__":
    main()

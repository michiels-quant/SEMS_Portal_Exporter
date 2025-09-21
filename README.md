# SEMS Portal Exporter 

Export your GoodWe **SEMS Portal** data to local JSON/CSV and merge it into **Apache Parquet** for analysis—e.g., to estimate potential savings from installing a **home battery**.

This repository includes:
- `sems_plant_power_v2.py` — robust downloader for plant power/time-series (JSON + optional CSV).
- `merge_sems_json_to_parquet.py` — merges daily SEMS JSON payloads into a single **Parquet** file.
- `example.env` — template for credentials and runtime config (copy to `.env`).

> **Note:** This project is not affiliated with GoodWe/SEMS. Use responsibly; upstream endpoints and headers can change.

---

## Installation (Conda)

```bash
# Clone your repo, then create the environment
conda env create -f environment.yml
conda activate sems-exporter
```

If you prefer `pip`, use the included `requirements.txt` instead.

---

## Quick Start

```bash
# 1) Copy the env template and fill in your values
cp example.env .env
# edit .env with your SEMS credentials and IDs

# 2) Export data (writes daily JSON files in the output folder)
python sems_plant_power_v2.py

# 3) Merge JSON -> Parquet for fast analytics
python merge_sems_json_to_parquet.py --src json_export --output sems_plant.parquet
```

### Configuration (`.env`)

Copy `example.env` to `.env` and edit. **Never commit real credentials.**

| Variable | Description | Example |
|---|---|---|
| `SEMS_ACCOUNT` | SEMS login (email) | `you@example.com` |
| `SEMS_PASSWORD` | SEMS password | `********` |
| `SEMS_STATION_ID` | Plant/station UUID (see below) | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| `SEMS_START` | First day to export (YYYY-MM-DD) | `2023-10-01` |
| `SEMS_END` | Last day to export (`YYYY-MM-DD` or `latest`) | `latest` |
| `SEMS_TZ_OFFSET` | Portal timezone offset string as returned | `+08:00` |
| `SEMS_OUT` | Output folder for raw JSON files | `json_export` |
| `SEMS_LANG` (opt) | Portal language header | `zh_CN` |
| `SEMS_VER` (opt) | App version header | `v2.2.0` |
| `SEMS_USER_NAME` (opt) | Friendly label (metadata only) | `John Doe` |
| `SEMS_INVERTER_ID` (opt) | Inverter serial (metadata only) | `54200DST233Wxxxx` |

**Tips**
- You can set `SEMS_END=latest` to always include data up to **today**.
- Keep `SEMS_TZ_OFFSET` identical to what the portal returns to avoid shifted timestamps.
- The exporter writes raw JSON per day under `SEMS_OUT` (default `json_export/`).

### How to find your `SEMS_STATION_ID`
1. Log into **semsportal.com**, open your plant overview.  
2. Open **Developer Tools → Network** in your browser.  
3. Click into the charts/monitor page; inspect requests to find the station/plant UUID.  
4. Put that UUID into `.env` as `SEMS_STATION_ID`.

---

## Usage Details

### Exporter
```bash
python sems_plant_power_v2.py
```
- Reads `.env`.  
- Fetches day windows `SEMS_START` → `SEMS_END`.  
- Stores **raw daily JSON** responses (and optionally CSV if enabled in the script).  
- Includes retry/backoff for intermittent `401/403` responses.

### Merger (JSON → Parquet)
```bash
python merge_sems_json_to_parquet.py --src json_export --output sems_plant.parquet
```
- Scans `--src` for `raw_v2_YYYY-MM-DD.json` files.  
- Flattens day-level payloads into one **columnar** dataset.  
- Writes `--output` (default example: `sems_plant.parquet`).

---

## Data Model (Typical Columns)

Depending on your account/endpoints, flattened rows may include:
- Timestamps (`timestamp` / `time_local`)  
- Instantaneous power (e.g., PV generation, load/consumption)  
- Optional grid **import/export** series, if exposed by your account  
- `day` and `source` are added during merge for provenance

---

## Troubleshooting

- **401/403 errors:** Re-run; the exporter refreshes auth headers and retries. Ensure `.env` values are correct.  
- **Empty days / missing columns:** Inspect the raw JSON for the exact series keys used by your account. Update your mapping accordingly.  
- **Parquet engine issues:** Install `pyarrow` (or `fastparquet`) as provided by the environment.  
- **Timezone drift:** Keep `SEMS_TZ_OFFSET` aligned with the portal’s offset in the JSON responses.

---

## Security & Privacy

- Credentials live only in `.env` and are **never** committed.  
- Raw JSON may contain device/plant metadata. Share with care.

---


## License

This project is licensed under the **Apache License 2.0**. See [LICENSE](LICENSE) for details.

---

## Disclaimer

This project is not affiliated with GoodWe/SEMS. Use at your own risk; APIs and payloads can change without notice.


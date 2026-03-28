#!/usr/bin/env python3
"""
Nightly scraper: fetch today's CBOE data, append to data/cboe_pc_ratios.csv,
and regenerate data/cboe_pc_ratios.json for the frontend.
"""
import csv
import json
import sys
from datetime import date
from io import StringIO
from pathlib import Path
from typing import Optional

import requests

DATA_DIR   = Path(__file__).parent.parent / "data"
CSV_PATH   = DATA_DIR / "cboe_pc_ratios.csv"
JSON_PATH  = DATA_DIR / "cboe_pc_ratios.json"
FIELDNAMES = ["date", "total_pc", "index_pc", "etp_pc", "equity_pc", "vix"]

# Dates manually excluded from the dataset due to known bad CBOE data.
# These are dropped from the CSV and skipped if the nightly scraper encounters them.
KNOWN_BAD_DATES = {
    "2025-01-09": "CBOE reported etp_pc=0.0 and equity_pc=0.0 — clearly missing data, not a real zero",
}

DAILY_API = "https://cdn.cboe.com/data/us/options/market_statistics/daily/{date}_daily_options"
VIX_URL   = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
HEADERS   = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cboe.com/"}

RATIO_MAP = {
    "total_pc":  "TOTAL PUT/CALL RATIO",
    "index_pc":  "INDEX PUT/CALL RATIO",
    "etp_pc":    "EXCHANGE TRADED PRODUCTS PUT/CALL RATIO",
    "equity_pc": "EQUITY PUT/CALL RATIO",
}


def existing_dates() -> set:
    if not CSV_PATH.exists():
        return set()
    with open(CSV_PATH) as f:
        return {row["date"] for row in csv.DictReader(f)}


def fetch_ratios(date_str: str) -> Optional[dict]:
    url = DAILY_API.format(date=date_str)
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        data = r.json()
        ratios = {item["name"]: item["value"] for item in data.get("ratios", [])}
        result = {}
        for col, api_name in RATIO_MAP.items():
            val = ratios.get(api_name)
            if val is None:
                return None
            result[col] = round(float(val), 4)
        return result
    except Exception as e:
        print(f"WARNING: {e}", file=sys.stderr)
        return None


def fetch_vix_today(date_str: str) -> Optional[float]:
    try:
        r = requests.get(VIX_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        for line in reversed(r.text.splitlines()):
            parts = line.split(",")
            if len(parts) >= 5 and parts[0].strip() == date_str:
                return round(float(parts[4].strip()), 4)
        # Fallback: last row
        lines = [l for l in r.text.splitlines() if l.strip() and not l.startswith("DATE")]
        if lines:
            parts = lines[-1].split(",")
            return round(float(parts[4].strip()), 4)
    except Exception as e:
        print(f"WARNING: VIX fetch failed: {e}", file=sys.stderr)
    return None


def csv_to_json():
    rows = []
    with open(CSV_PATH) as f:
        for row in csv.DictReader(f):
            rows.append({
                "date":      row["date"],
                "total_pc":  float(row["total_pc"]),
                "index_pc":  float(row["index_pc"]),
                "etp_pc":    float(row["etp_pc"]),
                "equity_pc": float(row["equity_pc"]),
                "vix":       float(row["vix"]),
            })
    rows.sort(key=lambda r: r["date"])
    JSON_PATH.write_text(json.dumps(rows, indent=2))
    print(f"Regenerated {JSON_PATH} ({len(rows)} rows)")


def main():
    today = date.today().isoformat()

    if today in KNOWN_BAD_DATES:
        print(f"Skipping {today}: {KNOWN_BAD_DATES[today]}")
        sys.exit(0)

    if today in existing_dates():
        print(f"Already have {today}. Regenerating JSON.")
        csv_to_json()
        sys.exit(0)

    print(f"Fetching {today}...")
    ratios = fetch_ratios(today)
    if ratios is None:
        print(f"No data for {today} (weekend/holiday?). Skipping.")
        sys.exit(0)

    vix = fetch_vix_today(today)
    if vix is None:
        print(f"WARNING: Could not get VIX for {today}. Skipping.")
        sys.exit(0)

    row = {"date": today, **ratios, "vix": vix}

    write_header = not CSV_PATH.exists()
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    print(f"Appended: {row}")

    csv_to_json()


if __name__ == "__main__":
    main()

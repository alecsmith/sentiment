#!/usr/bin/env python3
"""
CBOE put-call ratio scraper.

Usage:
  python cboe_scrape.py                  # try today and yesterday
  python cboe_scrape.py --days-back 1 2  # try yesterday and 2 days ago

The workflow runs this twice daily (UTC):
  21:30 UTC (5:30pm ET)  — checks today and yesterday:    --days-back 0 1
  07:30 UTC (3:30am ET)  — checks yesterday and 2 days ago: --days-back 1 2
"""
import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests

DATA_DIR   = Path(__file__).parent.parent / "data"
CSV_PATH   = DATA_DIR / "cboe_pc_ratios.csv"
JSON_PATH  = DATA_DIR / "cboe_pc_ratios.json"
FIELDNAMES = ["date", "total_pc", "index_pc", "etp_pc", "equity_pc", "vix"]

# Dates manually excluded due to known bad CBOE data.
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


def fetch_vix(date_str: str) -> Optional[float]:
    try:
        r = requests.get(VIX_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        for line in reversed(r.text.splitlines()):
            parts = line.split(",")
            if len(parts) >= 5 and parts[0].strip() == date_str:
                return round(float(parts[4].strip()), 4)
        # Fallback: last row of CSV
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


def fetch_and_append(date_str: str) -> bool:
    """Fetch one date and append to CSV. Returns True if a row was added."""
    ratios = fetch_ratios(date_str)
    if ratios is None:
        print(f"  {date_str}: no data (not yet published or holiday/weekend)")
        return False

    vix = fetch_vix(date_str)
    if vix is None:
        print(f"  {date_str}: could not get VIX, skipping")
        return False

    row = {"date": date_str, **ratios, "vix": vix}
    write_header = not CSV_PATH.exists()
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if write_header:
            writer.writeheader()
        writer.writerow(row)
    print(f"  {date_str}: appended")
    return True


def main():
    if "--days-back" in sys.argv:
        idx = sys.argv.index("--days-back")
        days_back = [int(x) for x in sys.argv[idx + 1:]]
    else:
        days_back = [0, 1]

    known = existing_dates()
    added = 0

    for n in days_back:
        d = date.today() - timedelta(days=n)
        date_str = d.isoformat()

        if date_str in KNOWN_BAD_DATES:
            print(f"  {date_str}: known bad data, skipping")
            continue
        if date_str in known:
            print(f"  {date_str}: already have this date")
            continue

        if fetch_and_append(date_str):
            known.add(date_str)
            added += 1

    if added > 0:
        csv_to_json()
    else:
        print("No new data added.")


if __name__ == "__main__":
    main()

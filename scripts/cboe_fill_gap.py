#!/usr/bin/env python3
"""
One-time script: fill the gap from 2019-10-07 to yesterday.

Uses the discovered CBOE daily API:
  https://cdn.cboe.com/data/us/options/market_statistics/daily/{date}_daily_options

VIX index levels come from the CBOE VIX History CSV.

Run after seed_historical.py.
"""
import csv
import sys
import time
from datetime import date, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

OUT_PATH = Path(__file__).parent.parent / "data" / "cboe_pc_ratios.csv"
FIELDNAMES = ["date", "total_pc", "index_pc", "etp_pc", "equity_pc", "vix"]

DAILY_API = "https://cdn.cboe.com/data/us/options/market_statistics/daily/{date}_daily_options"
VIX_URL   = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"
HEADERS   = {"User-Agent": "Mozilla/5.0", "Referer": "https://www.cboe.com/"}

# Map our column names to the name strings in the API response
RATIO_MAP = {
    "total_pc":  "TOTAL PUT/CALL RATIO",
    "index_pc":  "INDEX PUT/CALL RATIO",
    "etp_pc":    "EXCHANGE TRADED PRODUCTS PUT/CALL RATIO",
    "equity_pc": "EQUITY PUT/CALL RATIO",
}


def trading_days(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def existing_dates() -> set:
    if not OUT_PATH.exists():
        return set()
    with open(OUT_PATH) as f:
        return {row["date"] for row in csv.DictReader(f)}


def load_vix() -> dict:
    """Download VIX History CSV, return dict of {date_str: close}."""
    r = requests.get(VIX_URL, headers=HEADERS, timeout=20)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    df.columns = [c.strip() for c in df.columns]
    df["DATE"] = pd.to_datetime(df["DATE"], errors="coerce")
    df = df.dropna(subset=["DATE"])
    return {row["DATE"].strftime("%Y-%m-%d"): float(row["CLOSE"])
            for _, row in df.iterrows()}


def fetch_ratios(date_str: str) -> dict:
    """Fetch put-call ratios for a single date. Returns {} on failure."""
    url = DAILY_API.format(date=date_str)
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return {}
        data = r.json()
        ratios = {item["name"]: item["value"] for item in data.get("ratios", [])}
        result = {}
        for col, api_name in RATIO_MAP.items():
            val = ratios.get(api_name)
            if val is None:
                return {}
            result[col] = round(float(val), 4)
        return result
    except Exception:
        return {}


def main():
    known = existing_dates()
    if not known:
        print("No existing data. Run seed_historical.py first.")
        sys.exit(1)

    print(f"Existing data: {len(known)} dates")

    gap_start = date(2019, 10, 7)
    gap_end   = date.today() - timedelta(days=1)
    missing   = [d for d in trading_days(gap_start, gap_end)
                 if d.isoformat() not in known]

    if not missing:
        print("No gap to fill — already up to date.")
        sys.exit(0)

    print(f"Need to fill {len(missing)} dates ({missing[0]} to {missing[-1]})")
    print("Downloading VIX history...")
    vix_data = load_vix()
    print(f"  {len(vix_data)} VIX dates loaded")

    rows = []
    skipped = []

    for i, d in enumerate(missing):
        date_str = d.isoformat()
        ratios = fetch_ratios(date_str)
        vix = vix_data.get(date_str)

        if ratios and vix is not None:
            rows.append({"date": date_str, **ratios, "vix": round(vix, 4)})
        else:
            skipped.append(date_str)

        if (i + 1) % 100 == 0 or (i + 1) == len(missing):
            print(f"  {i+1}/{len(missing)}: {date_str}  ({len(rows)} rows collected)")

        time.sleep(0.3)

    if rows:
        with open(OUT_PATH, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            for row in rows:
                writer.writerow(row)
        print(f"\nAppended {len(rows)} rows to {OUT_PATH}")

    print(f"Skipped {len(skipped)} dates (weekends/holidays/no data)")


if __name__ == "__main__":
    main()

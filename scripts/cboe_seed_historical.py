#!/usr/bin/env python3
"""
One-time script: download CBOE historical put-call CSVs and VIX,
merge them, and write data/cboe_pc_ratios.csv from 2019-01-01 to 2019-10-04.

Run once to seed the data store before running fill_gap.py.
"""
import sys
from io import StringIO
from pathlib import Path

import pandas as pd
import requests

REFERER = "https://www.cboe.com/us/options/market_statistics/historical_data/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
    "Referer": REFERER,
}
BASE = "https://cdn.cboe.com/resources/options/volume_and_call_put_ratios/{}.csv"
VIX_URL = "https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv"

OUT_PATH = Path(__file__).parent.parent / "data" / "cboe_pc_ratios.csv"
START_DATE = "2019-01-01"
END_DATE   = "2019-10-04"

PC_SERIES = {
    "total_pc":  "totalpc",
    "index_pc":  "indexpc",
    "etp_pc":    "etppc",
    "equity_pc": "equitypc",
}


def fetch_pc(name: str) -> pd.DataFrame:
    r = requests.get(BASE.format(name), headers=HEADERS, timeout=20)
    r.raise_for_status()
    lines = r.text.splitlines()
    # Find the header row containing "Date" or "DATE"
    skip = 0
    for i, line in enumerate(lines):
        if "date" in line.lower():
            skip = i
            break
    df = pd.read_csv(StringIO("\n".join(lines[skip:])))
    df.columns = [c.strip() for c in df.columns]
    # The P/C ratio column is always the last one
    date_col = df.columns[0]
    ratio_col = df.columns[-1]
    df = df[[date_col, ratio_col]].copy()
    df.columns = ["date", "ratio"]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["ratio"] = pd.to_numeric(df["ratio"], errors="coerce")
    df = df.dropna()
    return df.set_index("date")


def fetch_vix() -> pd.DataFrame:
    r = requests.get(VIX_URL, timeout=20)
    r.raise_for_status()
    df = pd.read_csv(StringIO(r.text))
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={"DATE": "date", "CLOSE": "vix"})[["date", "vix"]]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["vix"] = pd.to_numeric(df["vix"], errors="coerce")
    df = df.dropna().set_index("date")
    return df


def main():
    print("Fetching put-call series...")
    frames = {}
    for col, filename in PC_SERIES.items():
        print(f"  {filename}...", end=" ", flush=True)
        frames[col] = fetch_pc(filename)
        print(f"{len(frames[col])} rows")

    print("Fetching VIX...")
    vix = fetch_vix()
    print(f"  {len(vix)} rows")

    merged = pd.DataFrame(index=frames["total_pc"].index)
    for col, df in frames.items():
        merged[col] = df["ratio"]
    merged["vix"] = vix["vix"]

    merged = merged.dropna()
    merged.index = pd.to_datetime(merged.index)
    merged = merged.loc[START_DATE:END_DATE].sort_index()
    merged.index.name = "date"
    merged = merged.round(4)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUT_PATH)
    print(f"\nWrote {len(merged)} rows ({merged.index[0].date()} to {merged.index[-1].date()}) → {OUT_PATH}")


if __name__ == "__main__":
    main()

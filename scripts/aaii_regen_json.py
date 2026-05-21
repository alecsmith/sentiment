"""Regenerate data/aaii_sentiment.json from data/aaii_sentiment.csv.

CSV format: date,bullish,neutral,bearish (YYYY-MM-DD, decimal 0-1).
Reused by the weekly scraper after it appends new rows to the CSV.
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "data" / "aaii_sentiment.csv"
JSON_PATH = ROOT / "data" / "aaii_sentiment.json"


def load_rows():
    rows = []
    with CSV_PATH.open(encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            rows.append({
                "date": r["date"],
                "bullish": float(r["bullish"]),
                "neutral": float(r["neutral"]),
                "bearish": float(r["bearish"]),
            })
    rows.sort(key=lambda r: r["date"])
    return rows


def main():
    rows = load_rows()
    JSON_PATH.write_text(json.dumps(rows, separators=(",", ":")) + "\n")
    print(f"Wrote {len(rows)} rows to {JSON_PATH.relative_to(ROOT)}")
    print(f"Range: {rows[0]['date']} -> {rows[-1]['date']}")


if __name__ == "__main__":
    main()

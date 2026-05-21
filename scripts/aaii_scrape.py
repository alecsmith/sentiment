"""Scrape latest AAII sentiment survey readings and update CSV + JSON.

Source: https://www.aaii.com/sentimentsurvey/sent_results
  - Static HTML table, ~22 weekly rows (newest first), no JS/auth.
  - Date column is month-day only ("May 20"); year is inferred from context.
  - Survey closes Wednesday; results posted Thursday morning ET.
  - The reported date is the Wednesday survey close (recent rows). Older
    historical rows used Thursday — we just store whatever AAII publishes.

Idempotent: only appends dates not already in data/aaii_sentiment.csv.
"""

import csv
import re
import sys
from datetime import date
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = ROOT / "data" / "aaii_sentiment.csv"
URL = "https://www.aaii.com/sentimentsurvey/sent_results"
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

MONTHS = {m: i + 1 for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
)}

ROW_RE = re.compile(
    r"<td[^>]*>\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2})\s*</td>\s*"
    r"<td[^>]*>\s*(\d+(?:\.\d+)?)\s*%\s*</td>\s*"
    r"<td[^>]*>\s*(\d+(?:\.\d+)?)\s*%\s*</td>\s*"
    r"<td[^>]*>\s*(\d+(?:\.\d+)?)\s*%\s*</td>",
    re.S,
)


def parse_page(html):
    """Return list of (date, bullish, neutral, bearish) newest first."""
    matches = ROW_RE.findall(html)
    today = date.today()
    year = today.year
    prev_month = None
    out = []
    for mon_str, day_str, bull, neut, bear in matches:
        m = MONTHS[mon_str]
        d = int(day_str)
        if prev_month is not None and m > prev_month:
            year -= 1  # crossed Jan -> Dec going backwards in time
        cur = date(year, m, d)
        if prev_month is None and cur > today:
            year -= 1
            cur = date(year, m, d)
        out.append((cur, float(bull) / 100, float(neut) / 100, float(bear) / 100))
        prev_month = m
    return out


def existing_dates():
    """Return sorted list of date objects in the CSV."""
    if not CSV_PATH.exists():
        return []
    out = []
    with CSV_PATH.open(encoding="utf-8", newline="") as f:
        for r in csv.DictReader(f):
            y, m, d = (int(x) for x in r["date"].split("-"))
            out.append(date(y, m, d))
    out.sort()
    return out


def same_survey_week(d, existing):
    """True if any existing date is within +/- 3 days of d (same weekly survey)."""
    for e in existing:
        if abs((e - d).days) <= 3:
            return True
    return False


def append_rows(new_rows):
    new_rows.sort(key=lambda r: r[0])
    with CSV_PATH.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        for d, bull, neut, bear in new_rows:
            w.writerow([d.isoformat(), round(bull, 6), round(neut, 6), round(bear, 6)])


def main():
    resp = requests.get(URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    page_rows = parse_page(resp.text)
    if not page_rows:
        print("ERROR: parsed 0 rows from page", file=sys.stderr)
        sys.exit(1)
    print(f"Parsed {len(page_rows)} rows from page; latest = {page_rows[0][0]}")

    have = existing_dates()
    new = [r for r in page_rows if not same_survey_week(r[0], have)]
    if not new:
        print("No new dates — CSV is up to date.")
        return

    print(f"Appending {len(new)} new row(s): {[str(r[0]) for r in new]}")
    append_rows(new)

    from aaii_regen_json import main as regen_main
    regen_main()


if __name__ == "__main__":
    main()

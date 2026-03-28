# Market Sentiment Dashboard

## Goal
A self-updating GitHub Pages site that displays daily put-call ratios and VIX with historical charts and smoothed (20-day) moving averages.

## Data Source
All data scraped from: https://www.cboe.com/us/options/market_statistics/daily/

Series to capture:
- Total put-call ratio
- Index put-call ratio
- Exchange-traded product put-call ratio
- Equity put-call ratio
- VIX

## Display
- Charts with multiple time horizon options (e.g. 3mo, YTD, 1y, 5y, max)
- 20-day smoothed average overlaid on each chart
- Static HTML page hosted on GitHub Pages

## Automation
- GitHub Actions cron job to scrape and update daily (midnight)
- GitHub repo: alecsmith account, accessible at alecsmith.github.io/market-dashboard

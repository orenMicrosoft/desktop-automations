# cef-screen

Quantitative screener for U.S. Closed-End Funds. Local-first, single source of truth = CEFConnect.

## What it does

Every day:
1. Pulls the full CEF universe from CEFConnect (`/api/v3/DailyPricing`).
2. Picks the **30 lowest 1Y Z-score** funds as the gatekeeper universe.
3. Refreshes price history, discount history, and per-distribution NII/ROC composition for those 30 (+ any held positions).
4. Computes a 4-factor composite score (Statistical Discount / Capital Resilience / Distribution Sustainability / Sector Peer) with an **asymmetric geometric penalty** so bad funds sink to the bottom but never silently disappear.
5. Renders BUY / SELL / CONFIG tabs in a local Flask dashboard at `http://localhost:8100`.

## Quick start

```powershell
# One-time install (editable)
pip install -e C:\Users\orenhorowitz\Code\CefScreen

# Start the web dashboard
cef-screen serve

# CLI mode — print today's BUY candidates to the terminal
cef-screen buy

# Drill down on a ticker
cef-screen inspect PFL

# Manage positions
cef-screen position add PFL --shares 1200 --cost-basis 8.45
cef-screen position list
cef-screen sell        # urgency-ranked sell signals for held positions
```

Or just double-click `cef-screen.bat` on the desktop.

## Architecture

- `config.py`   — thresholds, weights, benchmark map, cache paths
- `ingest.py`   — CEFConnect HTTP client (Session + retries + UA)
- `cache.py`    — SQLite cache (WAL) in `%LOCALAPPDATA%\cef_screener\`
- `metrics.py`  — vectorised Z, drawdown, NAV total return, coverage, peer rank
- `scoring.py`  — 4-factor composite + asymmetric severity-weighted penalty
- `rules.py`    — gatekeeper, sell triggers, destructive-ROC flag
- `portfolio.py` — positions.json reader → sell alerts
- `engine.py`   — orchestrates the full pipeline
- `web.py`      — Flask app, BUY/SELL/CONFIG tabs (port 8100)
- `cli.py`      — argparse: serve | buy | sell | inspect | position | refresh

**Engine-purity rule**: `scoring.py` and `metrics.py` import only `numpy` and `pandas` — no network, no SQLite, no Flask. This is what lets the same engine power CLI / web / HTML export without rewrites.

## Disclaimer

This is a quantitative screen, not financial advice. It highlights funds worth reviewing, not automatic trades. Check suitability, taxes, fund documents, and current market conditions before acting.

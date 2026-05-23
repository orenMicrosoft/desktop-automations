"""Flask dashboard for the CEF Screener.

Routes
------
- ``GET /``                  → BUY tab (gatekeeper-scored top picks)
- ``GET /sell``              → SELL tab (holdings with sell-trigger evaluation)
- ``GET /config``            → CONFIG tab (thresholds, weights, last refresh)
- ``GET /inspect/<ticker>``  → per-ticker drill-down
- ``POST /api/refresh``      → Trigger a full refresh from CEFConnect
- ``GET /api/health``        → Liveness probe used by the hub

Pipeline results are cached for ``CACHE_TTL_SECONDS`` seconds to avoid
re-running the full gatekeeper + scoring loop on every request.
"""
from __future__ import annotations

import argparse
import dataclasses
import threading
import time
import webbrowser
from typing import Any

import pandas as pd
from flask import Flask, jsonify, render_template_string, request

from . import config, engine, cache, portfolio


CACHE_TTL_SECONDS = 5 * 60   # 5 minutes


# ---------------------------------------------------------------- cache layer
class _ResultCache:
    """Thread-safe holder for the most recent ``engine.RunResult``."""

    def __init__(self) -> None:
        self._result: engine.RunResult | None = None
        self._ts: float = 0.0
        self._lock = threading.Lock()

    def get(self, *, max_age: int = CACHE_TTL_SECONDS) -> engine.RunResult | None:
        with self._lock:
            if self._result is None:
                return None
            if time.time() - self._ts > max_age:
                return None
            return self._result

    def set(self, result: engine.RunResult) -> None:
        with self._lock:
            self._result = result
            self._ts = time.time()

    def clear(self) -> None:
        with self._lock:
            self._result = None
            self._ts = 0.0


_CACHE = _ResultCache()


def _get_result() -> engine.RunResult:
    """Return the cached result, recomputing if missing/stale."""
    cached = _CACHE.get()
    if cached is not None:
        return cached
    result = engine.run_pipeline()
    _CACHE.set(result)
    return result


# ---------------------------------------------------------------- templates
_BASE_CSS = """
body { font-family: -apple-system, Segoe UI, Roboto, sans-serif;
       background: #0e1117; color: #e6e6e6; margin: 0; padding: 0; }
header { background: #161b22; padding: 1rem 2rem; border-bottom: 1px solid #30363d; }
header h1 { margin: 0; font-size: 1.4rem; color: #58a6ff; }
nav { padding: 0.5rem 2rem; background: #161b22; border-bottom: 1px solid #30363d; }
nav a { color: #58a6ff; text-decoration: none; margin-right: 1.5rem; font-weight: 500; }
nav a:hover { text-decoration: underline; }
main { padding: 1.5rem 2rem; }
table { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
th, td { padding: 0.5rem 0.75rem; border-bottom: 1px solid #21262d; text-align: left; }
th { background: #161b22; color: #c9d1d9; font-weight: 600; }
tr:hover { background: #161b22; }
.tier-A { color: #3fb950; font-weight: 700; }
.tier-B { color: #d29922; font-weight: 600; }
.tier-C, .tier-PASS { color: #8b949e; }
.urgent-2, .urgent-3 { color: #f85149; font-weight: 700; }
.urgent-1 { color: #d29922; font-weight: 600; }
.urgent-0 { color: #8b949e; }
.warn { background: #4a2900; padding: 0.5rem 1rem; border-left: 3px solid #d29922;
        margin: 0.5rem 0; color: #f0a020; }
.kv { display: grid; grid-template-columns: max-content 1fr; gap: 0.25rem 1rem;
      max-width: 600px; }
.kv b { color: #8b949e; }
button { background: #238636; color: white; border: 0; padding: 0.4rem 1rem;
         border-radius: 4px; cursor: pointer; }
button:hover { background: #2ea043; }
"""


_NAV = """
<nav>
  <a href="/">BUY</a> <a href="/sell">SELL</a> <a href="/config">CONFIG</a>
  <a href="/api/refresh" onclick="event.preventDefault();
    fetch('/api/refresh',{method:'POST'}).then(()=>location.reload());">Refresh</a>
</nav>
"""


def _layout(title: str, body: str) -> str:
    return (f"<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{title} — CEF Screener</title>"
            f"<style>{_BASE_CSS}</style></head><body>"
            f"<header><h1>CEF Screener — {title}</h1></header>"
            f"{_NAV}<main>{body}</main></body></html>")


def _warnings_html(warnings: list[str]) -> str:
    return "".join(f'<div class="warn">{w}</div>' for w in warnings)


def _format_pct(v: Any, digits: int = 2) -> str:
    if v is None or (isinstance(v, float) and (v != v)):  # NaN check
        return "—"
    try:
        return f"{float(v):.{digits}f}"
    except (TypeError, ValueError):
        return "—"


# ---------------------------------------------------------------- app factory
def create_app() -> Flask:
    app = Flask(__name__)
    _register_routes(app)
    return app


def _register_routes(app: Flask) -> None:    # noqa: C901
    @app.route("/")
    def buy() -> str:
        result = _get_result()
        rows_html = ""
        if result.scored.empty:
            rows_html = "<p>No results — run a refresh.</p>"
        else:
            head = ("<tr><th>Ticker</th><th>Name</th><th>Category</th>"
                    "<th>Disc%</th><th>Z1Y</th><th>Composite</th>"
                    "<th>Trap</th><th>Buy</th></tr>")
            body_rows = []
            for _, r in result.scored.iterrows():
                tier = r["buy_label"]
                cls = f"tier-{tier}"
                body_rows.append(
                    f"<tr><td><a href='/inspect/{r['ticker']}' style='color:#58a6ff'>"
                    f"{r['ticker']}</a></td>"
                    f"<td>{r.get('name', '')}</td>"
                    f"<td>{r.get('category_name', '')}</td>"
                    f"<td>{_format_pct(r.get('current_discount_pct'))}</td>"
                    f"<td>{_format_pct(r.get('z1'))}</td>"
                    f"<td>{_format_pct(r.get('composite'), 1)}</td>"
                    f"<td>{r.get('trap_tier', '')}</td>"
                    f"<td class='{cls}'>{tier}</td></tr>"
                )
            rows_html = ("<table>" + head + "".join(body_rows) + "</table>")
        snap = result.snapshot_date or "—"
        body = (f"<p>Snapshot: <b>{snap}</b> · "
                f"Universe: {result.universe_size} · "
                f"Liquid: {result.liquid_universe_size}</p>"
                f"{_warnings_html(result.warnings)}{rows_html}")
        return _layout("BUY", body)

    @app.route("/sell")
    def sell() -> str:
        result = _get_result()
        if not result.holdings:
            body = "<p>No holdings tracked. Add a position via the CLI.</p>"
            return _layout("SELL", body)
        head = ("<tr><th>Ticker</th><th>Shares</th><th>Cost</th>"
                "<th>Price</th><th>Total Ret%</th><th>Urgency</th>"
                "<th>Reason</th></tr>")
        rows = []
        for h in result.holdings:
            pos = h["position"]
            ret = h["return"]
            sig = h["sell"]
            cls = f"urgent-{sig.get('urgency', 0)}"
            rows.append(
                f"<tr><td>{pos['ticker']}</td>"
                f"<td>{pos['shares']:g}</td>"
                f"<td>{pos['cost_basis']:.2f}</td>"
                f"<td>{_format_pct(ret.get('price_pct'), 4)}</td>"
                f"<td>{_format_pct(ret.get('total_pct'), 4)}</td>"
                f"<td class='{cls}'>{sig.get('action', 'HOLD')}</td>"
                f"<td>{sig.get('reason', '—')}</td></tr>"
            )
        body = (_warnings_html(result.warnings)
                + "<table>" + head + "".join(rows) + "</table>")
        return _layout("SELL", body)

    @app.route("/config")
    def cfg_view() -> str:
        result = _get_result()
        items = [
            ("Snapshot date", result.snapshot_date or "—"),
            ("Snapshot age (h)", _format_pct(result.snapshot_age_hours, 1)),
            ("Universe size", str(result.universe_size)),
            ("Liquid universe size", str(result.liquid_universe_size)),
            ("Gatekeeper size", str(config.GATEKEEPER_SIZE)),
            ("Composite weights", str(config.COMPOSITE_FACTOR_WEIGHTS)),
            ("Penalty base", f"{config.PENALTY_BASE}"),
            ("Buy Tier A min", f"{config.BUY_TIER_A_MIN}"),
            ("Buy Tier B min", f"{config.BUY_TIER_B_MIN}"),
            ("Sell Z1 hard", f"{config.SELL_Z1_HARD}"),
            ("Sell Z1 mean-revert", f"{config.SELL_Z1_MEAN_REVERT}"),
            ("Sell Z3 confirm", f"{config.SELL_Z3_MEAN_REVERT_CONFIRM}"),
            ("Target gain", f"{config.SELL_TARGET_GAIN_PCT}"),
            ("Stop loss", f"{config.SELL_STOP_LOSS_PCT}"),
            ("Cache dir", str(config.cache_dir())),
        ]
        rows = "".join(f"<b>{k}</b><span>{v}</span>" for k, v in items)
        return _layout("CONFIG", f"<div class='kv'>{rows}</div>")

    @app.route("/inspect/<ticker>")
    def inspect(ticker: str) -> str:
        result = _get_result()
        df = result.scored
        if df.empty or "ticker" not in df.columns:
            return _layout(f"Inspect {ticker}", "<p>No scored data — run refresh.</p>")
        match = df[df["ticker"].str.upper() == ticker.upper()]
        if match.empty:
            return _layout(f"Inspect {ticker}",
                           f"<p>Ticker <code>{ticker}</code> not in current scored set.</p>")
        r = match.iloc[0].to_dict()
        kvs = [
            ("Name", r.get("name", "—")),
            ("Category", r.get("category_name", "—")),
            ("Discount %", _format_pct(r.get("current_discount_pct"))),
            ("Median discount 5Y %", _format_pct(r.get("median_disc_5y"))),
            ("Z 1Y", _format_pct(r.get("z1"))),
            ("NAV CAGR 3Y", _format_pct(r.get("nav_cagr_3y"), 4)),
            ("NAV total return 3Y", _format_pct(r.get("nav_total_return_3y"), 4)),
            ("Distribution rate (NAV)", _format_pct(r.get("distribution_rate_on_nav"), 4)),
            ("Coverage", _format_pct(r.get("coverage"), 3)),
            ("Composite", _format_pct(r.get("composite"), 1)),
            ("Multiplier", _format_pct(r.get("multiplier"), 3)),
            ("S Disc", _format_pct(r.get("s_disc"), 1)),
            ("S Res", _format_pct(r.get("s_res"), 1)),
            ("S Sust", _format_pct(r.get("s_sust"), 1)),
            ("S Peer", _format_pct(r.get("s_peer"), 1)),
            ("Trap tier", r.get("trap_tier", "—")),
            ("Trap reason", r.get("trap_reason", "—") or "—"),
            ("Buy label", r.get("buy_label", "—")),
        ]
        rows = "".join(f"<b>{k}</b><span>{v}</span>" for k, v in kvs)
        return _layout(f"Inspect {ticker.upper()}", f"<div class='kv'>{rows}</div>")

    @app.route("/api/refresh", methods=["POST"])
    def api_refresh():
        try:
            summary = engine.refresh_universe()
            _CACHE.clear()
            return jsonify({"ok": True, "message": "Refresh complete",
                            "summary": summary})
        except Exception as e:    # pragma: no cover - integration-only path
            return jsonify({"ok": False, "message": str(e)}), 500

    @app.route("/api/health")
    def api_health():
        return jsonify({"ok": True})


# ---------------------------------------------------------------- CLI entry
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cef-screener-web",
                                     description="CEF Screener Flask dashboard")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8100)
    parser.add_argument("--no-browser", action="store_true",
                        help="Don't auto-open a browser tab")
    args = parser.parse_args(argv)
    app = create_app()
    if not args.no_browser:
        webbrowser.open(f"http://{args.host}:{args.port}/")  # pragma: no cover
    app.run(host=args.host, port=args.port, debug=False)     # pragma: no cover
    return 0


if __name__ == "__main__":    # pragma: no cover
    raise SystemExit(main())

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
import html
import json
import threading
import time
import webbrowser
from typing import Any

import pandas as pd
from flask import Flask, Response, jsonify, redirect, render_template_string, request

from . import config, engine, cache, portfolio, scoring


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
nav { padding: 0.5rem 2rem; background: #161b22; border-bottom: 1px solid #30363d;
      display: flex; align-items: center; gap: 1.5rem; }
nav a { color: #58a6ff; text-decoration: none; font-weight: 500; }
nav a:hover { text-decoration: underline; }
nav .spacer { flex: 1; }
main { padding: 1.5rem 2rem; }
table { border-collapse: collapse; width: 100%; font-size: 0.9rem; }
th, td { padding: 0.5rem 0.75rem; border-bottom: 1px solid #21262d; text-align: left; }
th { background: #161b22; color: #c9d1d9; font-weight: 600; }
tr:hover { background: #161b22; }
.label-buy-a { color: #3fb950; font-weight: 700; }
.label-buy-b { color: #d29922; font-weight: 600; }
.label-avoid { color: #f85149; font-weight: 700; }
.label-watch { color: #8b949e; }
.urgent-2, .urgent-3 { color: #f85149; font-weight: 700; }
.urgent-1 { color: #d29922; font-weight: 600; }
.urgent-0 { color: #8b949e; }
.warn { background: #4a2900; padding: 0.5rem 1rem; border-left: 3px solid #d29922;
        margin: 0.5rem 0; color: #f0a020; }
.ok { background: #033a16; padding: 0.5rem 1rem; border-left: 3px solid #3fb950;
      margin: 0.5rem 0; color: #7ee787; }
.err { background: #4a0010; padding: 0.5rem 1rem; border-left: 3px solid #f85149;
       margin: 0.5rem 0; color: #ff8b8b; }
.kv { display: grid; grid-template-columns: max-content 1fr; gap: 0.25rem 1rem;
      max-width: 600px; }
.kv b { color: #8b949e; }
button, .btn { background: #238636; color: white; border: 0; padding: 0.4rem 1rem;
         border-radius: 4px; cursor: pointer; font-size: 0.9rem; }
button:hover, .btn:hover { background: #2ea043; }
button[disabled] { background: #444; cursor: not-allowed; }
.muted { color: #8b949e; font-size: 0.85rem; }
input[type=number], input[type=text] { background: #0d1117; color: #e6e6e6;
       border: 1px solid #30363d; border-radius: 4px; padding: 0.3rem 0.5rem;
       font-family: inherit; width: 8rem; }
form.cfg label { display: grid; grid-template-columns: 14rem 1fr 1fr;
       gap: 0.5rem; align-items: center; padding: 0.3rem 0;
       border-bottom: 1px solid #21262d; }
form.cfg label b { color: #c9d1d9; font-weight: 500; }
form.cfg label .default { color: #8b949e; font-size: 0.8rem; }
form.cfg .actions { margin-top: 1rem; display: flex; gap: 0.5rem; }
.legend { background: #161b22; padding: 0.75rem 1rem; border-left: 3px solid #58a6ff;
       margin: 0.5rem 0 1rem 0; }
.legend summary { cursor: pointer; color: #58a6ff; font-weight: 500; }
.legend ul { margin: 0.5rem 0 0 1rem; padding: 0; }
.spinner { display: inline-block; width: 1em; height: 1em; border: 2px solid #fff;
       border-top-color: transparent; border-radius: 50%;
       animation: spin 0.8s linear infinite; vertical-align: middle;
       margin-left: 0.5rem; }
@keyframes spin { to { transform: rotate(360deg); } }
.lab-controls { display: grid; grid-template-columns: max-content 1fr 4rem;
       gap: 0.5rem 1rem; max-width: 500px; align-items: center; }
.lab-controls input[type=range] { width: 100%; }
.placeholder { background: #161b22; padding: 1rem; border-left: 3px solid #8b949e;
       margin: 0.5rem 0; color: #8b949e; font-style: italic; }
"""


_NAV = """
<nav>
  <a href="/">BUY</a>
  <a href="/sell">SELL</a>
  <a href="/lab">LAB</a>
  <a href="/config">CONFIG</a>
  <span class="spacer"></span>
  <button id="refresh-btn" type="button"
    onclick="(function(b){b.disabled=true;b.innerHTML='Refreshing&hellip;<span class=spinner></span>';
      fetch('/api/refresh',{method:'POST'}).then(r=>r.json()).then(j=>{
        b.innerHTML = j.ok ? '✓ refreshed' : ('✗ ' + (j.message||'failed'));
        setTimeout(()=>location.reload(), 600);
      }).catch(e=>{b.innerHTML='✗ error';b.disabled=false;});
    })(this)">Refresh snapshot</button>
</nav>
"""


def _layout(title: str, body: str) -> str:
    title_safe = html.escape(title)
    return (f"<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{title_safe} — CEF Screener</title>"
            f"<style>{_BASE_CSS}</style></head><body>"
            f"<header><h1>CEF Screener — {title_safe}</h1></header>"
            f"{_NAV}<main>{body}</main></body></html>")


def _warnings_html(warnings: list[str]) -> str:
    return "".join(f'<div class="warn">{html.escape(w)}</div>' for w in warnings)


def _format_pct(v: Any, digits: int = 2) -> str:
    if v is None or (isinstance(v, float) and (v != v)):  # NaN check
        return "—"
    try:
        return f"{float(v):.{digits}f}"
    except (TypeError, ValueError):
        return "—"


def _label_css_class(label: str) -> str:
    """Pick the colour class for a buy_label string."""
    if not label:
        return "label-watch"
    if label.startswith("BUY-A"):
        return "label-buy-a"
    if label.startswith("BUY-B"):
        return "label-buy-b"
    if label.startswith("AVOID"):
        return "label-avoid"
    return "label-watch"


_LEGEND_HTML = """
<details class="legend">
  <summary>What do these labels mean?</summary>
  <ul>
    <li><b class="label-buy-a">BUY-A</b> — high conviction; full screen passes
      and the trap detector is quiet.</li>
    <li><b class="label-buy-b">BUY-B</b> — worth a look; passes the gatekeeper
      but with a smaller margin.</li>
    <li><b class="label-watch">watchlist</b> / <b>trap suspected</b> —
      something looks off; investigate before committing.</li>
    <li><b class="label-avoid">AVOID — distribution trap</b> — distribution
      coverage looks unsustainable.</li>
    <li><i>sparse data</i> — limited price/distribution history; treat the
      score as provisional.</li>
  </ul>
</details>
"""


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
                label = str(r["buy_label"] or "")
                cls = _label_css_class(label)
                ticker = html.escape(str(r["ticker"]))
                body_rows.append(
                    f"<tr><td><a href='/inspect/{ticker}' style='color:#58a6ff'>"
                    f"{ticker}</a></td>"
                    f"<td>{html.escape(str(r.get('name', '') or ''))}</td>"
                    f"<td>{html.escape(str(r.get('category_name', '') or ''))}</td>"
                    f"<td>{_format_pct(r.get('current_discount_pct'))}</td>"
                    f"<td>{_format_pct(r.get('z1'))}</td>"
                    f"<td>{_format_pct(r.get('composite'), 1)}</td>"
                    f"<td>{html.escape(str(r.get('trap_tier', '') or ''))}</td>"
                    f"<td class='{cls}'>{html.escape(label)}</td></tr>"
                )
            rows_html = ("<table>" + head + "".join(body_rows) + "</table>")
        snap = html.escape(str(result.snapshot_date or "—"))
        body = (f"<p>Snapshot: <b>{snap}</b> · "
                f"Universe: {result.universe_size} · "
                f"Liquid: {result.liquid_universe_size}</p>"
                f"{_warnings_html(result.warnings)}"
                f"{_LEGEND_HTML}{rows_html}")
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
                f"<tr><td>{html.escape(str(pos['ticker']))}</td>"
                f"<td>{pos['shares']:g}</td>"
                f"<td>{pos['cost_basis']:.2f}</td>"
                f"<td>{_format_pct(ret.get('price_pct'), 4)}</td>"
                f"<td>{_format_pct(ret.get('total_pct'), 4)}</td>"
                f"<td class='{cls}'>{html.escape(str(sig.get('action', 'HOLD')))}</td>"
                f"<td>{html.escape(str(sig.get('reason', '—') or '—'))}</td></tr>"
            )
        body = (_warnings_html(result.warnings)
                + "<table>" + head + "".join(rows) + "</table>")
        return _layout("SELL", body)

    @app.route("/config")
    def cfg_view() -> str:
        result = _get_result()
        eff = config.effective_settings()
        defaults = config._DEFAULTS
        flash = ""
        status = request.args.get("status")
        if status == "saved":
            flash = "<div class='ok'>Configuration saved.</div>"
        elif status == "reset":
            flash = "<div class='ok'>Reverted to defaults.</div>"
        elif status == "bad":
            msg = html.escape(request.args.get("msg", "Validation failed"))
            flash = f"<div class='err'>{msg}</div>"
        fields = []
        for key in config.OVERRIDABLE:
            if key == "COMPOSITE_FACTOR_WEIGHTS":
                continue    # rendered separately
            cur = eff[key]
            default = defaults[key]
            fields.append(
                f"<label><b>{html.escape(key)}</b>"
                f"<input name='{html.escape(key)}' "
                f"type='number' step='any' value='{html.escape(str(cur))}'/>"
                f"<span class='default'>default: {html.escape(str(default))}</span>"
                f"</label>"
            )
        # Weights — 4 sliders + numeric inputs
        w = eff["COMPOSITE_FACTOR_WEIGHTS"]
        w_def = defaults["COMPOSITE_FACTOR_WEIGHTS"]
        for k in ("s_disc", "s_res", "s_sust", "s_peer"):
            fields.append(
                f"<label><b>weight: {html.escape(k)}</b>"
                f"<input name='w_{html.escape(k)}' "
                f"type='number' step='0.05' min='0' value='{html.escape(str(w.get(k, 0.25)))}'/>"
                f"<span class='default'>default: {html.escape(str(w_def.get(k, 0.25)))}</span>"
                f"</label>"
            )
        snap = html.escape(str(result.snapshot_date or "—"))
        body = (
            f"{flash}"
            f"<p>Snapshot: <b>{snap}</b> · "
            f"Cache dir: <code>{html.escape(str(config.cache_dir()))}</code></p>"
            f"<form class='cfg' method='post' action='/api/config'>"
            f"{''.join(fields)}"
            f"<div class='actions'>"
            f"<button type='submit'>Save</button>"
            f"<button type='submit' formaction='/api/config/reset'>Reset to defaults</button>"
            f"</div></form>"
        )
        return _layout("CONFIG", body)

    @app.route("/api/config", methods=["POST"])
    def api_config_save():
        form = request.form
        updates: dict[str, Any] = {}
        errors: list[str] = []
        # Scalar fields
        for key, (cast, _validator) in config.OVERRIDABLE.items():
            if key == "COMPOSITE_FACTOR_WEIGHTS":
                continue
            if key not in form:
                continue
            raw = form[key].strip()
            if raw == "":
                continue
            try:
                updates[key] = cast(raw)
            except (TypeError, ValueError):
                errors.append(f"{key}: not a {cast.__name__}")
        # Weights
        weights = {}
        for k in ("s_disc", "s_res", "s_sust", "s_peer"):
            raw = form.get(f"w_{k}", "").strip()
            if raw == "":
                weights = None
                break
            try:
                weights[k] = float(raw)
            except (TypeError, ValueError):
                errors.append(f"weight {k}: not a number")
                weights = None
                break
        if weights is not None and len(weights) == 4:
            updates["COMPOSITE_FACTOR_WEIGHTS"] = weights
        if errors:
            return redirect(f"/config?status=bad&msg={html.escape('; '.join(errors))}")
        accepted = config.save_overrides(updates)
        rejected = [k for k in updates if k not in accepted]
        _CACHE.clear()
        if rejected:
            return redirect(
                f"/config?status=bad&msg={html.escape('rejected: ' + ', '.join(rejected))}")
        return redirect("/config?status=saved")

    @app.route("/api/config/reset", methods=["POST"])
    def api_config_reset():
        config.reset_overrides()
        _CACHE.clear()
        return redirect("/config?status=reset")

    @app.route("/inspect/<ticker>")
    def inspect(ticker: str) -> str:
        result = _get_result()
        df = result.scored
        tkr_safe = html.escape(ticker)
        if df.empty or "ticker" not in df.columns:
            return _layout(f"Inspect {ticker}", "<p>No scored data — run refresh.</p>")
        match = df[df["ticker"].str.upper() == ticker.upper()]
        if match.empty:
            return _layout(f"Inspect {ticker}",
                           f"<p>Ticker <code>{tkr_safe}</code> not in current scored set.</p>")
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
            ("Drawdown 2020 crash %", _format_pct(r.get("dd_2020_pct"), 3)),
            ("Drawdown 2022 bear %", _format_pct(r.get("dd_2022_pct"), 3)),
            ("Trap tier", r.get("trap_tier", "—")),
            ("Trap reason", r.get("trap_reason", "—") or "—"),
            ("Buy label", r.get("buy_label", "—")),
        ]
        rows = "".join(
            f"<b>{html.escape(str(k))}</b><span>{html.escape(str(v))}</span>"
            for k, v in kvs
        )
        phase2 = (
            "<h3>Phase 2 (coming soon)</h3>"
            "<div class='placeholder'>📰 News feed — RSS &amp; SEC EDGAR 8-K headlines "
            "for this ticker.</div>"
            "<div class='placeholder'>📋 Past status — how this ticker's score has "
            "drifted over time.</div>"
            "<div class='placeholder'>📉 Live performance through future drawdowns.</div>"
        )
        return _layout(
            f"Inspect {ticker.upper()}",
            f"<div class='kv'>{rows}</div>{phase2}",
        )

    @app.route("/lab")
    def lab() -> str:
        """What-if scoring sandbox: re-rank the cached scored set with new weights."""
        result = _get_result()
        if result.scored.empty:
            return _layout("LAB", "<p>No scored data yet — run a refresh first.</p>")
        eff = config.effective_settings()
        defaults_w = config._DEFAULTS["COMPOSITE_FACTOR_WEIGHTS"]
        w = {}
        for k in ("s_disc", "s_res", "s_sust", "s_peer"):
            raw = request.args.get(f"w_{k}", "").strip()
            try:
                w[k] = max(0.0, float(raw)) if raw else float(eff["COMPOSITE_FACTOR_WEIGHTS"][k])
            except (TypeError, ValueError):
                w[k] = float(eff["COMPOSITE_FACTOR_WEIGHTS"][k])
        penalty_raw = request.args.get("penalty", "").strip()
        try:
            penalty = float(penalty_raw) if penalty_raw else float(eff["PENALTY_BASE"])
        except (TypeError, ValueError):
            penalty = float(eff["PENALTY_BASE"])
        if not (0 < penalty <= 1):
            penalty = float(eff["PENALTY_BASE"])

        original_order = list(result.scored["ticker"])
        rescored: list[dict] = []
        for _, row in result.scored.iterrows():
            cmp_ = scoring.composite(
                float(row["s_disc"]), float(row["s_res"]),
                float(row["s_sust"]), float(row["s_peer"]),
                z1=row.get("z1"),
                current_discount_pct=row.get("current_discount_pct"),
                peer_penalty_gate=bool(row.get("peer_penalty_gate", False)),
                weights=w, penalty_base=penalty,
            )
            rescored.append({
                "ticker": row["ticker"],
                "name": row.get("name", ""),
                "old_composite": float(row["composite"]),
                "new_composite": cmp_["composite"],
            })
        rescored.sort(key=lambda d: d["new_composite"], reverse=True)
        new_order = [d["ticker"] for d in rescored]
        rows_html = []
        for new_rank, d in enumerate(rescored):
            old_rank = original_order.index(d["ticker"])
            delta = old_rank - new_rank
            delta_str = (f"<span style='color:#3fb950'>↑{delta}</span>" if delta > 0
                         else f"<span style='color:#f85149'>↓{-delta}</span>" if delta < 0
                         else "<span class='muted'>—</span>")
            rows_html.append(
                f"<tr><td>{new_rank + 1}</td>"
                f"<td><a href='/inspect/{html.escape(str(d['ticker']))}' "
                f"style='color:#58a6ff'>{html.escape(str(d['ticker']))}</a></td>"
                f"<td>{html.escape(str(d['name'] or ''))}</td>"
                f"<td>{d['old_composite']:.1f}</td>"
                f"<td>{d['new_composite']:.1f}</td>"
                f"<td>{delta_str}</td></tr>"
            )
        ctrl_inputs = "".join(
            f"<label for='w_{k}'><b>{k}</b></label>"
            f"<input id='w_{k}' name='w_{k}' type='range' min='0' max='1' step='0.05' "
            f"value='{w[k]}' oninput=\"document.getElementById('out_{k}').textContent=this.value\"/>"
            f"<span id='out_{k}'>{w[k]:.2f}</span>"
            for k in ("s_disc", "s_res", "s_sust", "s_peer")
        )
        ctrl_inputs += (
            f"<label for='penalty'><b>penalty base</b></label>"
            f"<input id='penalty' name='penalty' type='range' min='0.5' max='1' step='0.01' "
            f"value='{penalty}' oninput=\"document.getElementById('out_penalty').textContent=this.value\"/>"
            f"<span id='out_penalty'>{penalty:.2f}</span>"
        )
        body = (
            "<p class='muted'>Experiment with weights and the penalty base. "
            "Results re-rank in-place using the cached snapshot — no re-fetch.</p>"
            "<form method='get' action='/lab'>"
            f"<div class='lab-controls'>{ctrl_inputs}</div>"
            f"<div class='actions' style='margin-top:1rem'>"
            f"<button type='submit'>Re-rank</button>"
            f"<a class='btn' href='/lab' style='text-decoration:none;display:inline-block'>Reset</a>"
            "</div></form>"
            "<table style='margin-top:1rem'>"
            "<tr><th>#</th><th>Ticker</th><th>Name</th>"
            "<th>Original</th><th>New</th><th>Δ rank</th></tr>"
            + "".join(rows_html) +
            "</table>"
        )
        return _layout("LAB", body)

    @app.route("/api/refresh", methods=["POST"])
    def api_refresh():
        try:
            summary = engine.refresh_universe()
            _CACHE.clear()
            return jsonify({"ok": True, "message": "Refresh complete",
                            "summary": summary})
        except Exception as e:
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

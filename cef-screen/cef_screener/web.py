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

from . import config, engine, cache, portfolio, scoring, news


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
tr.row-link { cursor: pointer; }
tr.row-link:hover { background: #21262d; }
td.why { color: #8b949e; font-size: 0.85rem; max-width: 22rem;
       overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
table.sortable th { cursor: pointer; user-select: none; position: relative; }
table.sortable th:hover { background: #21262d; }
table.sortable th.sort-asc::after  { content: " ▲"; color: #58a6ff; }
table.sortable th.sort-desc::after { content: " ▼"; color: #58a6ff; }
"""


_NAV = """
<nav>
  <a href="/">BUY</a>
  <a href="/sell">SELL</a>
  <a href="/lab">LAB</a>
  <a href="/config">CONFIG</a>
  <span class="spacer"></span>
  <button id="quick-refresh-btn" type="button" title="Re-fetch only the universe snapshot (~5s)"
    onclick="(function(b){b.disabled=true;b.innerHTML='Refreshing&hellip;<span class=spinner></span>';
      fetch('/api/refresh',{method:'POST'}).then(r=>r.json()).then(j=>{
        b.innerHTML = j.ok ? '✓ snapshot refreshed' : ('✗ ' + (j.message||'failed'));
        setTimeout(()=>location.reload(), 600);
      }).catch(e=>{b.innerHTML='✗ error';b.disabled=false;});
    })(this)">Quick refresh</button>
  <button id="full-refresh-btn" type="button"
    title="Re-fetch universe + per-ticker history + news for the gatekeeper top-N (~1-3 min)"
    onclick="(function(b){b.disabled=true;b.innerHTML='Full refresh&hellip;<span class=spinner></span>';
      fetch('/api/refresh?mode=full',{method:'POST'}).then(r=>r.json()).then(j=>{
        if(j.ok){
          var s=j.summary||{};
          b.innerHTML='✓ '+ (s.price_history||0)+' price · '+(s.news||0)+' news';
        } else { b.innerHTML='✗ ' + (j.message||'failed'); }
        setTimeout(()=>location.reload(), 900);
      }).catch(e=>{b.innerHTML='✗ error';b.disabled=false;});
    })(this)">Full refresh</button>
</nav>
"""


_SORT_JS = """
(function(){
  function parseCell(text){
    var t = (text || '').trim();
    if (t === '' || t === '\\u2014') return null;
    var n = parseFloat(t.replace(/[%+,\\s]/g, ''));
    return isNaN(n) ? t : n;
  }
  function isNumericColumn(rows, idx){
    var seen = 0;
    for (var i = 0; i < rows.length; i++){
      var v = parseCell(rows[i].cells[idx] && rows[i].cells[idx].textContent);
      if (v === null) continue;
      if (typeof v !== 'number') return false;
      seen++;
    }
    return seen > 0;
  }
  function sortTable(table, idx, th){
    var rows = Array.from(table.querySelectorAll('tr')).filter(function(r){
      return r.cells.length > 0 && r.parentNode.tagName !== 'THEAD'
             && r.querySelectorAll('th').length === 0;
    });
    if (!rows.length) return;
    var dir = th.classList.contains('sort-asc') ? 'desc' : 'asc';
    table.querySelectorAll('th').forEach(function(h){
      h.classList.remove('sort-asc', 'sort-desc');
    });
    th.classList.add(dir === 'asc' ? 'sort-asc' : 'sort-desc');
    var numeric = isNumericColumn(rows, idx);
    rows.sort(function(a, b){
      var av = parseCell(a.cells[idx] && a.cells[idx].textContent);
      var bv = parseCell(b.cells[idx] && b.cells[idx].textContent);
      if (av === null && bv === null) return 0;
      if (av === null) return 1;       // nulls always last
      if (bv === null) return -1;
      if (numeric){
        return dir === 'asc' ? av - bv : bv - av;
      }
      var sa = String(av).toLowerCase(), sb = String(bv).toLowerCase();
      return dir === 'asc' ? sa.localeCompare(sb) : sb.localeCompare(sa);
    });
    var parent = rows[0].parentNode;
    rows.forEach(function(r){ parent.appendChild(r); });
  }
  function activate(){
    document.querySelectorAll('table.sortable').forEach(function(table){
      var headers = table.querySelectorAll('th');
      headers.forEach(function(th, idx){
        th.addEventListener('click', function(e){
          e.stopPropagation();
          sortTable(table, idx, th);
        });
      });
    });
  }
  if (document.readyState === 'loading'){
    document.addEventListener('DOMContentLoaded', activate);
  } else {
    activate();
  }
})();
"""


def _layout(title: str, body: str) -> str:
    title_safe = html.escape(title)
    return (f"<!doctype html><html><head><meta charset='utf-8'>"
            f"<title>{title_safe} — CEF Screener</title>"
            f"<style>{_BASE_CSS}</style></head><body>"
            f"<header><h1>CEF Screener — {title_safe}</h1></header>"
            f"{_NAV}<main>{body}</main>"
            f"<script>{_SORT_JS}</script></body></html>")


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


_TRAP_TIP = {
    "CONFIRMED": "Distribution trap CONFIRMED — high ROC, weak coverage, "
                 "NAV erosion. Avoid.",
    "SUSPECT":   "Trap SUSPECTED — one or two warning signs. Investigate.",
    "WATCH":     "On the watchlist — mild concern, not blocking.",
    "OK":        "No trap signal.",
}


def _why_text(row: Any) -> str:
    """One-line 'why' for the BUY table — trap reason, sparse note, or composite."""
    reason = row.get("trap_reason")
    if reason:
        return str(reason)
    if bool(row.get("sparse", False)):
        return "limited history — score is provisional"
    label = str(row.get("buy_label") or "")
    if label.startswith("BUY-A"):
        return "passes all screens with margin"
    if label.startswith("BUY-B"):
        return "passes screens but smaller margin"
    if label.startswith("AVOID"):
        return "distribution coverage looks unsustainable"
    return "—"


def _trap_tooltip(tier: str) -> str:
    if not tier:
        return ""
    key = tier.strip().upper()
    return _TRAP_TIP.get(key, "")


def _data_completeness_banner(scored: pd.DataFrame) -> str:
    """Big yellow warning if most rows are missing per-ticker history."""
    if scored is None or scored.empty:
        return ""
    cols_to_check = ("nav_cagr_3y", "median_disc_5y", "dd_2020_pct")
    have_history = 0
    for _, row in scored.iterrows():
        if any(_present(row.get(c)) for c in cols_to_check):
            have_history += 1
    total = len(scored)
    if have_history >= total * 0.5:
        return ""    # at least half the rows have real data
    missing = total - have_history
    return (
        f"<div class='warn'>⚠️ <b>{missing} of {total}</b> rows are missing "
        f"per-ticker history (NAV CAGR, 5Y median discount, crisis drawdowns) "
        f"— composite scores are placeholders. Click <b>Full refresh</b> in "
        f"the top-right to fetch this data from CEFConnect (~1-3 minutes).</div>"
    )


def _present(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, float) and v != v:    # NaN
        return False
    return True


_NEWS_SIGNALS = (
    # Each entry is (keyword tuple, signal label, explanation)
    # Checked in order — first match wins, so put most-specific rules first.
    (("rights offering", "rights issuance"), "DILUTIVE",
     "Rights offerings dilute existing holders and usually pressure NAV "
     "near-term — often a tactical wait-for-reset signal."),
    (("secondary offering", "share issuance", "follow-on"), "DILUTIVE",
     "New share issuance can pressure the premium/discount short-term."),
    (("tender offer", "buyback", "repurchase program", "share repurchase"),
     "ACCRETIVE",
     "Buybacks/tenders narrow the discount and are NAV-accretive — "
     "usually a bullish catalyst."),
    (("open-end", "open ending", "liquidation", "termination", "wind down",
      "wind-down"), "CATALYST",
     "Liquidation / open-ending forces the discount toward zero — "
     "typically a one-shot positive catalyst for discounted CEFs."),
    (("merger", "acquisition", "reorganization"), "CATALYST",
     "Combinations often trigger a discount close — usually bullish if "
     "you bought at a wide discount."),
    (("distribution cut", "dividend cut", "reduced distribution",
      "lower distribution", "decreas"), "SELL SIGNAL",
     "Distribution cuts crush the s_sust score and often spark forced "
     "selling — reassess before adding."),
    (("distribution increase", "dividend increase", "raises distribution",
      "raised distribution", "distribution hike", "dividend hike",
      "distribution boost", "dividend boost"), "BUY SIGNAL",
     "A hike when coverage > 1.0× is a credibility signal — boosts the "
     "s_sust score."),
    (("special distribution", "year-end distribution", "capital gain"),
     "MIXED",
     "Special distributions juice the reported yield but typically come "
     "out of NAV — not a fundamental buy signal."),
    (("return of capital", "ROC"), "MIXED",
     "Heavy ROC inflates the distribution rate but erodes NAV — watch "
     "the s_sust coverage ratio."),
    (("manager change", "portfolio manager", "sub-adviser", "subadviser",
      "adviser change", "advisor change"), "STRATEGY RISK",
     "Manager transitions break the strategy-continuity assumption — "
     "re-evaluate the thesis."),
    (("SEC", "lawsuit", "investigation", "probe", "subpoena",
      "settlement"), "GOVERNANCE RISK",
     "Material governance / legal risk — consider trimming until "
     "resolved."),
    (("activist", "13D", "proxy", "board"), "CATALYST",
     "Activist involvement often drives discount-narrowing actions — "
     "watch for tender offers or open-ending."),
    (("downgrade", "rating cut"), "BEARISH",
     "Credit downgrade in the holdings increases NAV risk — peer-relative "
     "weakness likely."),
    (("upgrade", "rating raised"), "BULLISH",
     "Credit upgrade in the holdings supports NAV — usually a tailwind."),
    (("rate cut", "fed cut", "fed pause"), "BULLISH",
     "Lower rates compress leverage costs and support bond/CEF NAVs."),
    (("rate hike", "rate increase", "tightening"), "BEARISH",
     "Higher rates raise leverage costs and pressure CEF NAVs."),
    (("leverage", "credit facility", "borrowing"), "CONTEXT",
     "Leverage drives both yield and downside — re-check in the current "
     "rate environment."),
    (("monthly distribution", "declares distribution", "distribution declared",
      "declares monthly"), "ROUTINE",
     "Routine declaration — only matters if the amount changed vs the "
     "prior period."),
    (("quarterly report", "earnings", "fiscal", "annual report",
      "shareholder report"), "ROUTINE",
     "Routine financial filing — useful context, rarely a standalone "
     "catalyst."),
)


def _news_relevance(title: str) -> tuple[str, str]:
    """Return (signal_label, explanation) for a news headline.

    Pure keyword heuristic — first matching rule wins. Falls back to a
    generic note when nothing matches.
    """
    t = (title or "").lower()
    for keywords, label, explanation in _NEWS_SIGNALS:
        for kw in keywords:
            if kw.lower() in t:
                return label, explanation
    return ("GENERAL",
            "General market or fund news — read the headline for context. "
            "Rarely a decisive buy/sell signal on its own.")


_SIGNAL_COLOURS = {
    "BUY SIGNAL": "#3fb950",
    "BULLISH": "#3fb950",
    "ACCRETIVE": "#3fb950",
    "CATALYST": "#3fb950",
    "SELL SIGNAL": "#f85149",
    "BEARISH": "#f85149",
    "DILUTIVE": "#f85149",
    "GOVERNANCE RISK": "#f85149",
    "STRATEGY RISK": "#d29922",
    "MIXED": "#d29922",
    "CONTEXT": "#8b949e",
    "ROUTINE": "#8b949e",
    "GENERAL": "#8b949e",
}


def _news_html(ticker: str) -> str:
    """Render up to 5 cached/fetched headlines for a ticker as an HTML block."""
    try:
        items = news.fetch_headlines(ticker)
    except Exception as e:    # pragma: no cover - defensive
        return (f"<div class='placeholder'>News fetch failed: "
                f"{html.escape(str(e))}</div>")
    if not items:
        return ("<div class='placeholder'>📰 No recent news headlines for "
                f"<code>{html.escape(ticker)}</code>.</div>")
    rows = []
    for it in items:
        raw_title = it.get("title", "") or ""
        title = html.escape(raw_title)
        link = it.get("link", "") or "#"
        link_safe = html.escape(link, quote=True)
        pub = html.escape(it.get("published", "") or "")
        raw_summary = (it.get("summary") or "").strip()
        # Use signal-keyword logic on title + summary so the relevance
        # explanation reflects the article body too.
        signal, why = _news_relevance(f"{raw_title} {raw_summary}")
        signal_safe = html.escape(signal)
        why_safe = html.escape(why)
        colour = _SIGNAL_COLOURS.get(signal, "#8b949e")
        summary_html = ""
        if raw_summary:
            summary_html = (
                f"<div style='color:#c9d1d9;font-size:0.85rem;"
                f"margin-top:0.25rem'>{html.escape(raw_summary)}</div>"
            )
        rows.append(
            f"<li style='margin-bottom:0.9rem'>"
            f"<a href='{link_safe}' target='_blank' rel='noopener' "
            f"style='color:#58a6ff;font-weight:500'>{title}</a>"
            f"<div class='muted' style='font-size:0.8rem'>{pub}</div>"
            f"{summary_html}"
            f"<div style='font-size:0.85rem;margin-top:0.25rem'>"
            f"<b style='color:{colour}'>{signal_safe}</b> — "
            f"<span style='color:#c9d1d9'>{why_safe}</span></div>"
            f"</li>"
        )
    return ("<h3>📰 Recent news</h3><ul style='padding-left:1.2rem'>"
            + "".join(rows) + "</ul>")


# =====================================================================
# Phase 2 — past status (score drift)
# =====================================================================
_SPARK_BLOCKS = "▁▂▃▄▅▆▇█"


def _sparkline(values: list[float]) -> str:
    """Unicode block-character sparkline for a list of numbers."""
    nums = [float(v) for v in values
            if v is not None and not (isinstance(v, float) and v != v)]
    if not nums:
        return ""
    lo, hi = min(nums), max(nums)
    if hi - lo < 1e-9:
        return _SPARK_BLOCKS[3] * len(nums)
    bucket = len(_SPARK_BLOCKS) - 1
    out = []
    for v in nums:
        idx = int(round((v - lo) / (hi - lo) * bucket))
        out.append(_SPARK_BLOCKS[max(0, min(bucket, idx))])
    return "".join(out)


def _past_status_html(ticker: str) -> str:
    """Render the historical-scores trend section for /inspect."""
    try:
        hist = cache.load_historical_scores(ticker)
    except Exception as e:
        return (f"<h3>📋 Past status</h3>"
                f"<div class='placeholder'>Past status unavailable: "
                f"{html.escape(str(e))}</div>")
    if hist is None or hist.empty:
        return ("<h3>📋 Past status</h3>"
                "<div class='placeholder'>No score history cached yet — "
                "this section starts populating after the next pipeline run. "
                "Click <b>Quick refresh</b> or <b>Full refresh</b> above to "
                "force a run.</div>")
    head = ("<tr><th>Date</th><th>Composite</th><th>S Disc</th>"
            "<th>S Res</th><th>S Sust</th><th>S Peer</th>"
            "<th>Mult</th><th>Label</th></tr>")
    row_html = []
    for _, r in hist.tail(10).iloc[::-1].iterrows():
        row_html.append(
            "<tr>"
            f"<td>{html.escape(str(r.get('snapshot_date') or ''))}</td>"
            f"<td>{_format_pct(r.get('composite'), 1)}</td>"
            f"<td>{_format_pct(r.get('s_disc'), 1)}</td>"
            f"<td>{_format_pct(r.get('s_res'), 1)}</td>"
            f"<td>{_format_pct(r.get('s_sust'), 1)}</td>"
            f"<td>{_format_pct(r.get('s_peer'), 1)}</td>"
            f"<td>{_format_pct(r.get('multiplier'), 2)}</td>"
            f"<td>{html.escape(str(r.get('buy_label') or ''))}</td>"
            "</tr>"
        )
    table = (f"<table class='sortable' style='font-size:0.85rem'>"
             f"{head}{''.join(row_html)}</table>")

    # One snapshot → no drift to show; explain instead of rendering noise.
    n = len(hist)
    if n < 2:
        snap_date = html.escape(str(hist.iloc[0].get('snapshot_date') or ''))
        return (
            "<h3>📋 Past status — score drift</h3>"
            f"<div class='placeholder'>First snapshot recorded on "
            f"<b>{snap_date}</b>. Score drift will appear here after the "
            f"next pipeline run (sparkline + change-vs-first below the "
            f"snapshot table).</div>"
            f"{table}"
        )

    composites = [float(v) for v in hist["composite"].tolist()
                  if v is not None and not (isinstance(v, float) and v != v)]
    spark = _sparkline(composites)
    first = hist.iloc[0]
    last = hist.iloc[-1]
    delta = None
    if _present(last.get("composite")) and _present(first.get("composite")):
        delta = float(last["composite"]) - float(first["composite"])
    delta_str = ""
    if delta is not None:
        arrow = "▲" if delta > 0 else ("▼" if delta < 0 else "▶")
        colour = ("#3fb950" if delta > 0
                  else ("#f85149" if delta < 0 else "#8b949e"))
        delta_str = (f" <span style='color:{colour}'>{arrow} "
                     f"{abs(delta):.1f} vs first</span>")
    return (
        f"<h3>📋 Past status — score drift ({n} snapshots)</h3>"
        f"<div style='font-family:monospace;font-size:1.4rem;letter-spacing:2px'>"
        f"{html.escape(spark)}{delta_str}</div>"
        f"{table}"
    )


# =====================================================================
# Phase 2 — drawdowns / live performance
# =====================================================================
def _rolling_drawdowns(nav_series: list[float]) -> dict:
    """Compute max drawdown over trailing 30 / 90 / 252 / 756 day windows.

    Returns a dict of ``{window_label: pct}`` where pct is positive (e.g.
    25.0 means 25% drawdown). Missing windows are omitted.
    """
    if not nav_series:
        return {}
    out: dict[str, float] = {}
    n = len(nav_series)
    spec = [("30d", 30), ("90d", 90), ("1y", 252), ("3y", 756)]
    for label, win in spec:
        if n < 2:
            break
        slice_ = nav_series[-win:] if n >= win else nav_series
        running_max = slice_[0]
        max_dd = 0.0
        for v in slice_:
            if v > running_max:
                running_max = v
            if running_max > 0:
                dd = (running_max - v) / running_max * 100.0
                if dd > max_dd:
                    max_dd = dd
        out[label] = max_dd
    return out


def _drawdowns_html(ticker: str, row: pd.Series | dict) -> str:
    """Render the rolling-drawdown + crisis-window section for /inspect."""
    try:
        ph = cache.load_price_history(ticker)
    except Exception as e:
        return (f"<h3>📉 Drawdown profile</h3>"
                f"<div class='placeholder'>Drawdowns unavailable: "
                f"{html.escape(str(e))}</div>")
    if ph is None or ph.empty:
        return ("<h3>📉 Drawdown profile</h3>"
                "<div class='placeholder'>No price history cached for "
                f"<code>{html.escape(ticker)}</code> — run <b>Full refresh</b>.</div>")
    nav_col = "nav" if "nav" in ph.columns else "price"
    if nav_col not in ph.columns:
        return ("<h3>📉 Drawdown profile</h3>"
                "<div class='placeholder'>Cached history has no NAV column "
                "— refresh required.</div>")
    nav_clean = pd.to_numeric(ph[nav_col], errors="coerce").dropna().tolist()
    dd = _rolling_drawdowns(nav_clean)
    rows = "".join(
        f"<tr><td>{html.escape(label)}</td>"
        f"<td>−{val:.1f}%</td></tr>"
        for label, val in dd.items()
    )
    table = (f"<table style='font-size:0.9rem;max-width:300px'>"
             f"<tr><th>Window</th><th>Max drawdown</th></tr>"
             f"{rows}</table>") if dd else ""
    crisis_bits = []
    for label, key in (("2020 (COVID)", "dd_2020_pct"),
                       ("2022 (rate-hike)", "dd_2022_pct")):
        v = row.get(key) if hasattr(row, "get") else None
        if _present(v):
            crisis_bits.append(f"<li><b>{html.escape(label)}</b>: "
                               f"−{float(v):.1f}%</li>")
    crisis = ("<h4 style='margin:0.5rem 0'>Crisis-window drawdowns</h4>"
              f"<ul>{''.join(crisis_bits)}</ul>") if crisis_bits else ""
    # NAV sparkline over the trailing 120 trading days
    spark_vals = nav_clean[-120:] if len(nav_clean) >= 2 else []
    spark = _sparkline(spark_vals)
    spark_html = ""
    if spark:
        spark_html = (
            f"<h4 style='margin:0.5rem 0'>NAV trend (last {len(spark_vals)} "
            f"sessions)</h4>"
            f"<div style='font-family:monospace;font-size:1.4rem;"
            f"letter-spacing:2px'>{html.escape(spark)}</div>"
        )
    return ("<h3>📉 Drawdown profile</h3>"
            f"{table}{crisis}{spark_html}")


_LEGEND_HTML = """
<details class="legend" open>
  <summary>What do these labels mean? (click rows for full details)</summary>
  <p style="margin:0.5rem 0"><b>Buy label</b> — the bottom-line call combining the
    composite score and the trap detector:</p>
  <ul>
    <li><b class="label-buy-a">BUY-A (high conviction)</b> — composite ≥
      tier-A threshold and the trap detector is quiet.</li>
    <li><b class="label-buy-b">BUY-B (worth a look)</b> — composite is decent
      but lower margin; sanity-check before sizing up.</li>
    <li><b class="label-watch">watchlist · trap suspected</b> — something
      looks off; investigate before committing.</li>
    <li><b class="label-avoid">AVOID — distribution trap</b> — distribution
      coverage looks unsustainable (see below).</li>
    <li><i>sparse data</i> — limited price/distribution history; treat the
      score as provisional.</li>
  </ul>
  <p style="margin:0.75rem 0 0.25rem"><b>What is a "distribution trap"?</b></p>
  <p style="margin:0">A CEF paying out more in distributions than its NAV can
    sustain — typically funded by Return-of-Capital (ROC), eroding NAV. When the
    music stops the distribution gets cut and price collapses. We classify:</p>
  <ul style="margin-top:0.25rem">
    <li><b>CONFIRMED</b> — high ROC + falling NAV + weak coverage. Avoid.</li>
    <li><b>SUSPECT</b> — one or two warning signs.</li>
    <li><b>WATCH</b> — mild concern; not blocking.</li>
    <li><b>—</b> (or <b>OK</b>) — no trap signal.</li>
  </ul>
  <p style="margin:0.5rem 0 0" class="muted">Click a row to drill into the
    full per-ticker breakdown.</p>
</details>
"""


_COMPOSITE_EXPLAINER = """
<details class="legend" open style="margin-bottom:1rem">
  <summary>📊 How the composite score is computed</summary>
  <p style="margin:0.5rem 0"><b>Composite</b> = weighted average of four
    sub-scores (each 0–100), then multiplied by a quality multiplier:</p>
  <pre style="background:#0d1117;padding:0.75rem 1rem;border-radius:4px;
       margin:0.5rem 0;font-size:0.85rem;color:#c9d1d9;white-space:pre-wrap">
composite = (w_disc·s_disc + w_res·s_res + w_sust·s_sust + w_peer·s_peer)
            × multiplier</pre>
  <p style="margin:0.5rem 0 0.25rem"><b>The four sub-scores:</b></p>
  <ul style="margin-top:0.25rem">
    <li><b>s_disc — Discount opportunity (25%)</b>
      Where today's discount sits inside the fund's own 5-year
      discount range. Wide-vs-history → high score. Built from
      <code>current_discount_pct</code> and the median + extremes
      of <code>discount_history</code>.</li>
    <li><b>s_res — NAV resilience (25%)</b>
      How well the fund's NAV held up during the 2020 (COVID) and
      2022 (rate-hike) drawdowns, normalized against peers. Smaller
      drawdowns → high score.</li>
    <li><b>s_sust — Distribution sustainability (25%)</b>
      Does the fund's NAV growth cover its distribution rate?
      <code>coverage = nav_cagr_3y ÷ distribution_rate</code>. A
      coverage of 1.0 means earnings exactly cover payouts; above
      1.0 builds NAV, below 1.0 erodes it (the classic ROC trap).</li>
    <li><b>s_peer — Peer-relative 3y return (25%)</b>
      Percentile rank of this fund's <code>yr3_ret_on_nav</code>
      vs. all gatekeeper funds in the same Morningstar category.
      Top quartile → high score.</li>
  </ul>
  <p style="margin:0.5rem 0 0.25rem"><b>The multiplier (penalty/bonus):</b></p>
  <ul style="margin-top:0.25rem">
    <li>Starts at <b>1.00</b>.</li>
    <li>Drops to <code>PENALTY_BASE</code> (default 0.75) when a
      trap is <b>CONFIRMED</b> or <b>SUSPECT</b>, or when peer-relative
      drawdown exceeds the gate.</li>
    <li>Smaller penalties (~0.9) apply for sparse history, missing
      coverage data, or other data-quality flags.</li>
  </ul>
  <p style="margin:0.5rem 0 0.25rem"><b>Then the buy label:</b></p>
  <ul style="margin-top:0.25rem">
    <li><b class="label-buy-a">BUY-A</b> — composite ≥
      <code>BUY_TIER_A_MIN</code> (75) and no trap.</li>
    <li><b class="label-buy-b">BUY-B</b> — composite ≥
      <code>BUY_TIER_B_MIN</code> (60) and no <b>CONFIRMED</b> trap.</li>
    <li><b class="label-avoid">AVOID</b> — below B threshold, or trap
      confirmed at any score.</li>
  </ul>
  <p style="margin:0.5rem 0 0" class="muted">
    The weights below are normalized to sum to 1.0 at scoring time, so
    you can use any positive values. Click any fund row on the BUY
    page → "Inspect" to see all four sub-scores broken down.
  </p>
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
                    "<th>Trap</th><th>Buy</th><th>Why?</th></tr>")
            body_rows = []
            for _, r in result.scored.iterrows():
                label = str(r["buy_label"] or "")
                cls = _label_css_class(label)
                ticker = html.escape(str(r["ticker"]))
                trap_tier = str(r.get("trap_tier", "") or "")
                trap_tip = html.escape(_trap_tooltip(trap_tier))
                why = html.escape(_why_text(r))
                body_rows.append(
                    f"<tr class='row-link' onclick=\"window.location='/inspect/{ticker}'\">"
                    f"<td><a href='/inspect/{ticker}' style='color:#58a6ff'>"
                    f"{ticker}</a></td>"
                    f"<td>{html.escape(str(r.get('name', '') or ''))}</td>"
                    f"<td>{html.escape(str(r.get('category_name', '') or ''))}</td>"
                    f"<td>{_format_pct(r.get('current_discount_pct'))}</td>"
                    f"<td>{_format_pct(r.get('z1'))}</td>"
                    f"<td>{_format_pct(r.get('composite'), 1)}</td>"
                    f"<td title='{trap_tip}'>{html.escape(trap_tier)}</td>"
                    f"<td class='{cls}' title='{html.escape(label)}'>"
                    f"{html.escape(label)}</td>"
                    f"<td class='why' title='{why}'>{why}</td></tr>"
                )
            rows_html = ("<table class='sortable'>" + head + "".join(body_rows) + "</table>")
        snap = html.escape(str(result.snapshot_date or "—"))
        data_banner = _data_completeness_banner(result.scored)
        body = (f"<p>Snapshot: <b>{snap}</b> · "
                f"Universe: {result.universe_size} · "
                f"Liquid: {result.liquid_universe_size}</p>"
                f"{_warnings_html(result.warnings)}"
                f"{data_banner}"
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
                + "<table class='sortable'>" + head + "".join(rows) + "</table>")
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
            f"{_COMPOSITE_EXPLAINER}"
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
        # If we have no price history, surface a clear hint right at the top.
        history_banner = ""
        if not _present(r.get("nav_cagr_3y")) and not _present(r.get("dd_2020_pct")):
            history_banner = (
                f"<div class='warn'>📡 No per-ticker history cached for "
                f"<code>{html.escape(ticker.upper())}</code>. The composite "
                f"score is a placeholder — click <b>Full refresh</b> above "
                f"to fetch this fund's price/discount/distribution history.</div>"
            )
        news_block = _news_html(ticker.upper())
        phase2 = (_past_status_html(ticker.upper())
                  + _drawdowns_html(ticker.upper(), r))
        return _layout(
            f"Inspect {ticker.upper()}",
            f"{history_banner}<div class='kv'>{rows}</div>"
            f"{news_block}{phase2}",
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
            "<table class='sortable' style='margin-top:1rem'>"
            "<tr><th>#</th><th>Ticker</th><th>Name</th>"
            "<th>Original</th><th>New</th><th>Δ rank</th></tr>"
            + "".join(rows_html) +
            "</table>"
        )
        return _layout("LAB", body)

    @app.route("/api/refresh", methods=["POST"])
    def api_refresh():
        mode = request.args.get("mode", "quick").strip().lower()
        full = (mode == "full")
        try:
            summary = engine.refresh_universe(full=full)
            _CACHE.clear()
            msg = "Full refresh complete" if full else "Snapshot refresh complete"
            return jsonify({"ok": True, "message": msg, "summary": summary})
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

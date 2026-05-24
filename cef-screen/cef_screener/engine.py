"""Pipeline orchestrator — ``run_pipeline()`` returns a ``RunResult``.

The engine glues together cache reads, metric computation, scoring, rules,
and portfolio evaluation. It contains *no* HTTP or HTML logic. Called by
both ``cli.py`` and ``web.py``.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

import pandas as pd

from . import cache, config, ingest, metrics, portfolio, rules, scoring

log = logging.getLogger(__name__)


@dataclass
class RunResult:
    snapshot_date: str | None
    snapshot_age_hours: float | None
    universe_size: int
    liquid_universe_size: int
    gatekeeper: pd.DataFrame
    scored: pd.DataFrame
    holdings: list[dict] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def _safe_float(v) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if pd.isna(f):
        return None
    return f


def _flip_sign(cefconnect_value: float | None) -> float | None:
    """CEFConnect: positive = premium. Plan: positive = discount. Flip."""
    if cefconnect_value is None:
        return None
    return -cefconnect_value


def _build_per_ticker_inputs(
    row: dict,
    price_history: pd.DataFrame,
    discount_history: pd.DataFrame,
    distribution_history: pd.DataFrame,
) -> dict:
    discount_pct = _flip_sign(_safe_float(row.get("discount")))
    median_disc_5y = None
    if not discount_history.empty and "discount" in discount_history.columns:
        clean = pd.to_numeric(discount_history["discount"], errors="coerce").dropna()
        if not clean.empty:
            median_disc_5y = float(-clean.median())

    nav_cagr_3y = metrics.nav_cagr(price_history, years=3) if not price_history.empty else None
    nav_tr_3y = (metrics.nav_total_return_annualised(price_history, distribution_history, years=3)
                 if not price_history.empty else None)

    dd_2020 = dd_2022 = None
    for start, end in config.DRAWDOWN_WINDOWS:
        if not price_history.empty:
            d = metrics.peak_to_trough_drawdown_pct(price_history, start, end, price_col="nav")
            if "2020" in start:
                dd_2020 = d
            elif "2022" in start:
                dd_2022 = d

    cadence = metrics.detect_cadence(distribution_history) if not distribution_history.empty else "mixed"
    roc12 = metrics.roc_pct(distribution_history, months=12) if not distribution_history.empty else None
    roc24 = metrics.roc_pct(distribution_history, months=24) if not distribution_history.empty else None
    roc_trend = (roc12 - roc24) if (roc12 is not None and roc24 is not None) else None
    cuts = metrics.distribution_cuts_5y(distribution_history, cadence) if not distribution_history.empty else None
    dist_cagr = metrics.distribution_cagr_5y(distribution_history, cadence) if not distribution_history.empty else None

    eps = _safe_float(row.get("eps"))
    current_dist = _safe_float(row.get("current_distribution"))
    freq = row.get("distribution_frequency")
    coverage, _src = metrics.select_coverage(
        category=row.get("category_name"),
        distribution_history=distribution_history,
        eps=eps,
        current_distribution=current_dist,
        distribution_frequency=freq,
    )

    crisis_vals: list[float] = []
    for start, end in config.DRAWDOWN_WINDOWS:
        if not distribution_history.empty:
            cm = metrics.crisis_distribution_maintenance(distribution_history, start, end)
            if cm is not None:
                crisis_vals.append(cm)
    crisis = min(crisis_vals) if crisis_vals else None

    comp_quality = "incomplete"
    if not distribution_history.empty:
        comps = distribution_history.tail(12).apply(metrics.composition_valid, axis=1)
        if bool(comps.any()):
            comp_quality = "full"

    dist_hist_years = None
    if not distribution_history.empty and "ex_date" in distribution_history.columns:
        clean = pd.to_datetime(distribution_history["ex_date"], errors="coerce").dropna()
        if not clean.empty:
            dist_hist_years = (clean.max() - clean.min()).days / 365.25

    return {
        "z1": _safe_float(row.get("z_score_1yr")),
        "z3": _safe_float(row.get("z_score_3m")),
        "current_discount_pct": discount_pct,
        "median_disc_5y": median_disc_5y,
        "dd_2020_pct": dd_2020,
        "dd_2022_pct": dd_2022,
        "lev_pct": _safe_float(row.get("leverage_ratio")),
        "roc_pct_12m": roc12,
        "roc_trend": roc_trend,
        "dist_cuts_5y": cuts,
        "dist_cagr_5y": dist_cagr,
        "coverage": coverage,
        "nav_cagr_3y": nav_cagr_3y,
        "nav_total_return_3y": nav_tr_3y,
        "unii_ratio": _safe_float(row.get("unii_per_share")),
        "unii_per_share": _safe_float(row.get("unii_per_share")),
        "crisis_maintenance": crisis,
        "composition_quality": comp_quality,
        "distribution_history_years": dist_hist_years,
        "distribution_rate_on_nav": _safe_float(row.get("distribution_rate_nav")),
    }


def _score_one(inputs: dict, peer_pct: float | None,
               benchmark_cagr_3y: float | None) -> dict:
    sd = scoring.s_disc(inputs["z1"], inputs["z3"],
                        inputs["current_discount_pct"], inputs["median_disc_5y"])
    sr = scoring.s_res(inputs["dd_2020_pct"], inputs["dd_2022_pct"],
                       inputs["lev_pct"])
    ss = scoring.s_sust(
        roc_pct_12m=inputs["roc_pct_12m"], roc_trend=inputs["roc_trend"],
        dist_cuts_5y=inputs["dist_cuts_5y"], dist_cagr_5y=inputs["dist_cagr_5y"],
        coverage=inputs["coverage"], nav_cagr_3y=inputs["nav_cagr_3y"],
        unii_ratio=inputs["unii_ratio"], crisis_maintenance=inputs["crisis_maintenance"],
        lev_pct=inputs["lev_pct"], composition_quality=inputs["composition_quality"],
        distribution_history_years=inputs["distribution_history_years"],
    )
    sp = scoring.s_peer(peer_pct, inputs["nav_total_return_3y"], benchmark_cagr_3y)
    cmp_ = scoring.composite(
        sd["value"], sr["value"], ss["value"], sp["value"],
        z1=inputs["z1"], current_discount_pct=inputs["current_discount_pct"],
        peer_penalty_gate=sp["penalty"],
    )
    trap = scoring.trap_classification(
        roc_pct_12m=inputs["roc_pct_12m"], roc_trend=inputs["roc_trend"],
        nav_cagr_3y=inputs["nav_cagr_3y"], coverage=inputs["coverage"],
        unii_per_share=inputs["unii_per_share"],
        distribution_rate_on_nav=inputs["distribution_rate_on_nav"],
        nav_total_return_3y=inputs["nav_total_return_3y"],
        benchmark_cagr_3y=benchmark_cagr_3y,
        composition_quality=inputs["composition_quality"],
        distribution_history_years=inputs["distribution_history_years"],
    )
    trap_tier = rules.trap_tier_label(trap["suspect"], trap["confirmed"], trap["watch"])
    sparse = sd["sparse"] or sr["sparse"] or ss["sparse"] or sp["sparse"]
    return {
        "s_disc": sd["value"], "s_res": sr["value"],
        "s_sust": ss["value"], "s_peer": sp["value"],
        "composite": cmp_["composite"], "multiplier": cmp_["multiplier"],
        "total_severity": cmp_["total_severity"],
        "peer_penalty_gate": bool(sp["penalty"]),
        "trap_tier": trap_tier,
        "trap_reason": trap["reason"],
        "buy_label": rules.buy_label(cmp_["composite"], trap_tier, sparse=sparse),
        "sparse": sparse,
    }


def _snapshot_age_hours(snapshot_date: str | None) -> float | None:
    if not snapshot_date:
        return None
    try:
        snap = datetime.fromisoformat(snapshot_date[:10]).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return (datetime.now(timezone.utc) - snap).total_seconds() / 3600.0


def _missed_business_days(snapshot_date: str | None,
                          *, today: "datetime.date | None" = None) -> int | None:
    """Number of US business days (Mon-Fri) that have fully elapsed
    between ``snapshot_date`` and ``today`` (exclusive on both ends).

    0 means the snapshot is current relative to the calendar — no new
    business day has passed since the data was published. >= 1 means
    a trading day went by without an update (genuinely stale).

    Returns ``None`` if ``snapshot_date`` is missing or unparseable.
    Note: weekday-only, does not account for US market holidays.
    """
    if not snapshot_date:
        return None
    try:
        snap = datetime.fromisoformat(snapshot_date[:10]).date()
    except ValueError:
        return None
    if today is None:
        today = datetime.now(timezone.utc).date()
    if snap >= today:
        return 0
    n = 0
    cur = snap + timedelta(days=1)
    while cur < today:
        if cur.weekday() < 5:
            n += 1
        cur += timedelta(days=1)
    return n


def _peer_percentile_for(universe: pd.DataFrame, row: dict,
                         own_ret: float | None) -> float | None:
    """Compare ``own_ret`` (universe row's ``yr3_ret_on_nav``, in percent)
    against the same column across category-and-leverage-tier peers.

    Bug history: used to compare ``inputs["nav_total_return_3y"]`` (3y
    NAV total return, in decimal — e.g. 0.18) against peers' ``yr1_ret_on_nav``
    (1y return, in percent — e.g. 18.0). The unit mismatch alone made own
    always smaller than every peer → percentile = 0 across the board,
    which dragged composites down by ~10-15 points. The 3y-vs-1y horizon
    mismatch was a separate apples-to-oranges bug. Fixed by using the
    same column (yr3_ret_on_nav) on both sides.
    """
    if own_ret is None or universe.empty:
        return None
    cat = row.get("category_name")
    if not cat:
        return None
    lev_tier = config.leverage_tier(_safe_float(row.get("leverage_ratio")))
    peers = universe[universe["category_name"] == cat].copy()
    peers["_tier"] = peers["leverage_ratio"].apply(
        lambda v: config.leverage_tier(_safe_float(v))
    )
    bucket = peers[peers["_tier"] == lev_tier]
    if len(bucket) < 5:
        bucket = peers
    series = pd.to_numeric(bucket.get("yr3_ret_on_nav", pd.Series([], dtype=float)),
                           errors="coerce").dropna()
    return metrics.peer_percentile(series, own_ret) if not series.empty else None


def _benchmark_cagr(category: str | None) -> float | None:
    if not category:
        return None
    benchmark = config.benchmark_for(category)
    return config.BENCHMARK_CAGR_3Y.get(benchmark) if benchmark else None


def run_pipeline(*, positions_path=None) -> RunResult:
    """Read cached data, score gatekeeper top-N, evaluate portfolio."""
    universe = cache.load_latest_universe()
    snapshot_date = (universe["snapshot_date"].iloc[0]
                     if not universe.empty and "snapshot_date" in universe.columns
                     else None)
    snapshot_age = _snapshot_age_hours(snapshot_date)

    warnings: list[str] = []
    if universe.empty:
        warnings.append("Universe cache is empty — run `cef-screen refresh` first.")
        return RunResult(snapshot_date, snapshot_age, 0, 0,
                         universe, universe, [], warnings)

    liquid = universe[universe.apply(rules.passes_liquidity, axis=1)]

    if snapshot_age is not None and snapshot_age > 24:
        missed = _missed_business_days(snapshot_date)
        if missed and missed >= 1:
            # At least one trading day has come and gone without an update.
            warnings.append(
                f"Snapshot is {snapshot_age:.1f}h old and {missed} business "
                f"day(s) behind — CEFConnect should have newer data. Try "
                f"Full refresh."
            )
        # else: snapshot date is the most recent business day (e.g. weekend
        # or pre-market on Monday) — no warning, the API has nothing newer.

    gate = rules.gatekeeper_top_n(universe, n=config.GATEKEEPER_SIZE,
                                  sort_col="z_score_1yr")

    rows: list[dict] = []
    for tkr in gate["ticker"]:
        row = gate.loc[gate["ticker"] == tkr].iloc[0].to_dict()
        ph = cache.load_price_history(tkr)
        dh = cache.load_discount_history(tkr)
        dx = cache.load_distribution_history(tkr)
        inputs = _build_per_ticker_inputs(row, ph, dh, dx)
        # Use the universe row's yr3_ret_on_nav (in percent) so we compare
        # against peers on the same column/unit/horizon.
        own_3y_pct = _safe_float(row.get("yr3_ret_on_nav"))
        peer_pct = _peer_percentile_for(universe, row, own_3y_pct)
        benchmark_cagr = _benchmark_cagr(row.get("category_name"))
        score = _score_one(inputs, peer_pct, benchmark_cagr)
        rows.append({"ticker": tkr, **row, **inputs, **score, "peer_pct": peer_pct})

    scored = pd.DataFrame(rows).sort_values("composite", ascending=False) if rows else pd.DataFrame()

    # Persist scores for the score-drift / past-status feature on /inspect.
    try:
        cache.persist_historical_scores(scored, snapshot_date)
    except Exception as e:    # pragma: no cover - defensive
        log.warning("persist_historical_scores failed: %r", e)

    holdings: list[dict] = []
    try:
        poss = portfolio.load_positions(positions_path) if positions_path else portfolio.load_positions()
    except (ValueError, OSError):
        poss = []
        warnings.append("Could not read positions.json")

    if poss:
        snap_by_t: dict[str, dict] = {}
        for _, r in universe.iterrows():
            snap_by_t[str(r["ticker"]).upper()] = {
                "price": _safe_float(r.get("price")),
                "z1": _safe_float(r.get("z_score_1yr")),
                "z3": _safe_float(r.get("z_score_3m")),
            }
        dists_by_t: dict[str, list] = {}
        for pos in poss:
            dh = cache.load_distribution_history(pos.ticker)
            dists_by_t[pos.ticker.upper()] = dh.to_dict("records") if not dh.empty else []
        holdings = portfolio.evaluate_portfolio(poss, snap_by_t, dists_by_t)

    return RunResult(
        snapshot_date=snapshot_date,
        snapshot_age_hours=snapshot_age,
        universe_size=len(universe),
        liquid_universe_size=len(liquid),
        gatekeeper=gate,
        scored=scored,
        holdings=holdings,
        warnings=warnings,
    )


def refresh_universe(*, tickers: list[str] | None = None,
                     full: bool = False) -> dict:
    """Fetch universe + (optionally) per-ticker histories from CEFConnect.

    - ``tickers`` is ``None`` and ``full=False`` → fast path: only the
      universe snapshot is refreshed.
    - ``tickers`` is ``None`` and ``full=True`` → fetch the universe, then
      auto-select the gatekeeper top-N (lowest 1Y z-score among liquid
      tickers) and fetch per-ticker histories + news for those.
    - ``tickers`` is a list → fetch universe + per-ticker histories + news
      for exactly those tickers.

    Per-ticker history fetch delegates to :func:`cache.refresh_ticker_deep`
    (which uses the right backfill periods / date ranges for each series).
    Returns a summary dict with row counts for the web/CLI layers.
    """
    universe_rows = ingest.fetch_universe()
    cache.write_universe(universe_rows)
    summary: dict[str, int] = {"universe": len(universe_rows),
                               "price_history": 0,
                               "discount_history": 0,
                               "distribution_history": 0,
                               "news": 0,
                               "errors": 0}
    if full and not tickers:
        u = cache.load_latest_universe()
        if not u.empty:
            liquid = u[u.apply(rules.passes_liquidity, axis=1)]
            gate = rules.gatekeeper_top_n(liquid, n=config.GATEKEEPER_SIZE,
                                          sort_col="z_score_1yr")
            tickers = list(gate["ticker"])
    if tickers:
        from . import news as news_module
        for tkr in tickers:
            deep = cache.refresh_ticker_deep(tkr, force_full=full)
            if "error" in deep:
                summary["errors"] += 1
                log.warning("refresh_ticker_deep(%s) failed: %s",
                            tkr, deep["error"])
            else:
                summary["price_history"] += deep.get("price_history", 0)
                summary["discount_history"] += deep.get("discount_history", 0)
                summary["distribution_history"] += deep.get(
                    "distribution_history", 0)
            try:
                items = news_module.fetch_headlines(tkr, force_refresh=True)
                summary["news"] += len(items)
            except Exception as e:    # pragma: no cover - network failures
                log.warning("news.fetch_headlines(%s) failed: %r", tkr, e)
    return summary

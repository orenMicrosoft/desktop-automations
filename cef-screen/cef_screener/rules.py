"""Buy/sell gatekeeper, trap tiers, sell-trigger evaluator.

This module owns the rule-based filtering that runs **before** scoring
(gatekeeper) and **after** scoring (trap classification + sell triggers).
It is pure: it takes data + thresholds in and returns labels/booleans out.
"""
from __future__ import annotations

from typing import Iterable, Mapping

import pandas as pd

from . import config, metrics


# ---------------------------------------------------------------- liquidity
def passes_liquidity(row: Mapping) -> bool:
    """Wrapper around metrics.passes_liquidity for self-documenting call sites.

    Accepts either canonical universe column names (``market_cap_usd_m``,
    ``avg_daily_volume``) or the test alias ``market_cap_usdm``.
    """
    mc = row.get("market_cap_usd_m")
    if mc is None:
        mc = row.get("market_cap_usdm")
    return metrics.passes_liquidity(mc, row.get("avg_daily_volume"))


# ---------------------------------------------------------------- gatekeeper
def gatekeeper_top_n(
    universe_df: pd.DataFrame,
    n: int | None = None,
    sort_col: str = "z1",
) -> pd.DataFrame:
    """Liquidity-filter then rank by ``sort_col`` ascending → return top *n*.

    Z-Score conventions: low (more negative) is better; we sort ascending and
    take the head. Funds missing ``sort_col`` are dropped from candidacy.
    """
    if n is None:
        n = config.GATEKEEPER_SIZE
    if universe_df.empty:
        return universe_df.head(0)

    df = universe_df.copy()
    df["passes_liquidity"] = df.apply(passes_liquidity, axis=1)
    df = df[df["passes_liquidity"]]
    if df.empty:
        return df.head(0)

    df = df.dropna(subset=[sort_col])
    if df.empty:
        return df.head(0)

    return df.sort_values(sort_col, ascending=True).head(n)


# ---------------------------------------------------------------- sell triggers
def evaluate_sell_triggers(
    *,
    z1: float | None,
    z3: float | None,
    return_pct: float | None,
    sell_z1_hard: float | None = None,
    sell_z1_mean_revert: float | None = None,
    sell_z3_confirm: float | None = None,
    target_gain_pct: float | None = None,
    stop_loss_pct: float | None = None,
) -> dict:
    """Run §5.7 sell-trigger taxonomy against one position.

    ``return_pct`` is a FRACTION (e.g., 0.12 = +12%, not 12).
    Returns ``{ 'triggers': [list of str], 'urgency': 0..3 }`` where:
      0 = HOLD, 1 = WATCH, 2 = REVIEW, 3 = SELL-NOW.

    All threshold args default to ``None`` and resolve to the current
    ``config.*`` value at call time, so runtime overrides take effect.
    """
    if sell_z1_hard is None:
        sell_z1_hard = config.SELL_Z1_HARD
    if sell_z1_mean_revert is None:
        sell_z1_mean_revert = config.SELL_Z1_MEAN_REVERT
    if sell_z3_confirm is None:
        sell_z3_confirm = config.SELL_Z3_MEAN_REVERT_CONFIRM
    if target_gain_pct is None:
        target_gain_pct = config.SELL_TARGET_GAIN_PCT
    if stop_loss_pct is None:
        stop_loss_pct = config.SELL_STOP_LOSS_PCT
    triggers: list[str] = []
    urgency = 0

    if z1 is not None and z1 >= sell_z1_hard:
        triggers.append(f"SELL: Z1-HARD (z1={z1:+.2f} ≥ {sell_z1_hard:+.2f})")
        urgency = max(urgency, 3)

    if (z1 is not None and z3 is not None
            and z1 >= sell_z1_mean_revert and z3 >= sell_z3_confirm):
        triggers.append(
            f"SELL: MEAN-REVERT (z1={z1:+.2f} ≥ {sell_z1_mean_revert:+.2f} "
            f"AND z3={z3:+.2f} ≥ {sell_z3_confirm:+.2f})"
        )
        urgency = max(urgency, 3)

    if return_pct is not None and return_pct >= target_gain_pct:
        triggers.append(f"SELL: TARGET-GAIN ({return_pct:+.1%} ≥ {target_gain_pct:+.1%})")
        urgency = max(urgency, 2)

    if return_pct is not None and return_pct <= stop_loss_pct:
        triggers.append(f"SELL: STOP-LOSS ({return_pct:+.1%} ≤ {stop_loss_pct:+.1%})")
        urgency = max(urgency, 3)

    if (z1 is not None and z1 >= sell_z1_mean_revert and
            (z3 is None or z3 < sell_z3_confirm)):
        # Z1 hit mean-revert threshold but Z3 didn't confirm → REVIEW
        triggers.append(
            f"REVIEW: Z1 hit mean-revert ({z1:+.2f}) without Z3 confirmation"
        )
        urgency = max(urgency, 2)

    if (z1 is not None and 1.0 <= z1 < sell_z1_mean_revert):
        triggers.append(f"WATCH: Z1 elevated ({z1:+.2f})")
        urgency = max(urgency, 1)

    return {"triggers": triggers, "urgency": urgency}


# ---------------------------------------------------------------- trap labels
def trap_tier_label(suspect: bool, confirmed: bool, watch: bool) -> str:
    """Convert booleans from ``scoring.trap_classification`` to a tier string."""
    if confirmed:
        return "CONFIRMED"
    if suspect:
        return "SUSPECT"
    if watch:
        return "WATCH"
    return "OK"


# ---------------------------------------------------------------- BUY label
def buy_label(composite: float, trap_tier: str, sparse: bool = False) -> str:
    """Convert composite score + trap tier into the BUY column label.

    Plan §5.6: TIER A ≥ 75 (high-conviction), TIER B 60–75 (review),
    TIER C < 60 (avoid). Trap CONFIRMED tags as TRAP overlay.
    Sparse data appends "(sparse data)" so users know coverage is incomplete.
    """
    if trap_tier == "CONFIRMED":
        return "AVOID — distribution trap"
    if composite >= config.BUY_TIER_A_MIN:
        base = "BUY-A (high conviction)"
    elif composite >= config.BUY_TIER_B_MIN:
        base = "BUY-B (worth a look)"
    else:
        base = "AVOID"

    overlays: list[str] = []
    if trap_tier == "SUSPECT":
        overlays.append("trap suspected")
    elif trap_tier == "WATCH":
        overlays.append("watchlist")
    if sparse:
        overlays.append("sparse data")

    return base + ((" · " + " · ".join(overlays)) if overlays else "")

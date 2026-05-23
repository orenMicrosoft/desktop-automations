"""Sub-score + composite computations with the asymmetric geometric penalty.

Like ``metrics.py``, this module is pure (numpy / pandas / config only). It
consumes a *fund-level* dict produced by the engine and returns the four
sub-scores + composite + severity decomposition for one fund. Vectorised
peer-rank work belongs in ``metrics.blended_peer_percentile``; this module
does the per-fund math.

All sub-scores are 0–100 (higher = better). Severity per factor is bounded
to [0, 2]; ``total_severity`` is in [0, 8] which makes the asymmetric
multiplier ``0.75 ** total_severity`` live in [0.10, 1.00].
"""
from __future__ import annotations

from typing import Iterable

import math

from . import config


def _clip(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return float(v)


def _sigmoid(x: float) -> float:
    # Numerically stable
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


# =====================================================================
# 5.1 Statistical Discount sub-score
# =====================================================================
def s_disc(
    z1: float | None,
    z3: float | None,
    current_discount_pct: float | None,
    median_disc_5y: float | None,
) -> dict:
    """Plan §5.1 with v3.2 structural-discount blend.

    NOTE: ``current_discount_pct`` and ``median_disc_5y`` use the PLAN
    convention where **positive = discount, negative = premium** (i.e.,
    +8 means the fund trades 8 % below NAV). This is the OPPOSITE of
    CEFConnect's raw ``DiscountPremiumPercent`` field — callers must
    flip the sign at ingest/engine boundary.

    Returns ``{'value': s_disc, 'z_part': ..., 'abs_part': ..., 'struct_part': ...,
                'sparse': bool}``.
    """
    sparse = False

    if z1 is None:
        # Without any Z signal at all, default to 50 and flag sparse
        z_part = 50.0
        sparse = True
    elif z3 is None:
        # Z1 only — re-weight (RD-5)
        z_part = _clip(50 - 30 * z1)
        sparse = True
    else:
        z_part = _clip(50 - 20 * z1 - 10 * z3)

    abs_part = _clip(50 + 3 * (current_discount_pct or 0))
    struct_part = _clip(50 + 2.5 * (median_disc_5y or 0))

    if z1 is not None and z1 > 0:
        # Don't let stale deep discount mask an overbought fund
        abs_eff = min(abs_part, 70.0)
        struct_eff = min(struct_part, 70.0)
    else:
        abs_eff = abs_part
        struct_eff = struct_part

    val = (config.SDISC_Z_WEIGHT * z_part
           + config.SDISC_ABS_WEIGHT * abs_eff
           + config.SDISC_STRUCT_WEIGHT * struct_eff)
    return {
        "value": _clip(val),
        "z_part": z_part,
        "abs_part": abs_part,
        "struct_part": struct_part,
        "sparse": sparse,
    }


# =====================================================================
# 5.2 Capital Resilience sub-score
# =====================================================================
def s_res(
    dd_2020_pct: float | None,
    dd_2022_pct: float | None,
    lev_pct: float | None,
    fallback_to_peer_median: float = 60.0,
) -> dict:
    """Hybrid: dd_input = max(0.7*dd_2022 + 0.3*dd_2020, 0.8*max(dd_2020, dd_2022))."""
    if dd_2020_pct is None and dd_2022_pct is None:
        # Sparse — use conservative prior per §5.2 ("category median minus 5")
        return {"value": _clip(fallback_to_peer_median - 5),
                "dd_input": None, "lev_mult": 1.0, "sparse": True}
    dd20 = dd_2020_pct if dd_2020_pct is not None else 0.0
    dd22 = dd_2022_pct if dd_2022_pct is not None else 0.0
    dd_worst = max(dd20, dd22)
    dd_regime = 0.7 * dd22 + 0.3 * dd20
    dd_input = max(dd_regime, 0.8 * dd_worst)
    lev_mult = min(1.0 + config.LEV_MULT_COEF * _lev_frac(lev_pct), config.LEV_MULT_CAP)
    return {
        "value": _clip(100 - dd_input * lev_mult),
        "dd_input": dd_input, "lev_mult": lev_mult, "sparse": False,
    }


def _lev_frac(lev_pct: float | None) -> float:
    if lev_pct is None:
        return config.UNIVERSE_MEDIAN_LEV_PCT / 100.0
    return float(lev_pct) / 100.0


# =====================================================================
# 5.3 Distribution Sustainability sub-score
# =====================================================================
def s_sust(
    roc_pct_12m: float | None,
    roc_trend: float | None,
    dist_cuts_5y: int | None,
    dist_cagr_5y: float | None,
    coverage: float | None,
    nav_cagr_3y: float | None,
    unii_ratio: float | None,
    crisis_maintenance: float | None,
    lev_pct: float | None,
    composition_quality: str,    # 'full' or 'incomplete'
    distribution_history_years: float | None,
) -> dict:
    """Weighted blend with leverage-drag and crisis-maintenance per v3.2.

    Returns ``{'value', 'components', 'sparse', 'lev_drag', 'roc_s', 'coverage_s', ...}``.
    """
    # Component sub-scores
    if roc_pct_12m is not None:
        roc_trend_eff = max(0.0, roc_trend or 0.0)
        roc_s = _clip(100 - 150 * roc_pct_12m - 50 * roc_trend_eff)
    else:
        roc_s = None

    cuts_s = _clip(100 - 25 * dist_cuts_5y) if dist_cuts_5y is not None else None
    growth_s = _clip(50 + 500 * dist_cagr_5y) if dist_cagr_5y is not None else None
    coverage_s = (100 * _sigmoid((coverage - 1.0) / config.COVERAGE_SCALE)
                  if coverage is not None else None)
    nav_s = (100 * _sigmoid(nav_cagr_3y / config.NAV_CAGR_SCALE)
             if nav_cagr_3y is not None else None)
    unii_s = (100 * _sigmoid(unii_ratio / config.UNII_SCALE)
              if unii_ratio is not None else None)
    crisis_s = (_clip(100 * crisis_maintenance) if crisis_maintenance is not None else None)

    full_q = (composition_quality == "full"
              and distribution_history_years is not None
              and distribution_history_years >= 3)

    if full_q:
        weights = dict(config.SUST_WEIGHTS_FULL)
        components = {
            "roc": roc_s, "cuts": cuts_s, "growth": growth_s,
            "coverage": coverage_s, "nav": nav_s, "unii": unii_s,
            "crisis_maint": crisis_s,
        }
        sparse = False
    else:
        weights = dict(config.SUST_WEIGHTS_FALLBACK)
        components = {"coverage": coverage_s, "nav": nav_s, "unii": unii_s}
        sparse = True

    # Drop None components and re-normalise
    active = {k: v for k, v in components.items() if v is not None and k in weights}
    w_total = sum(weights[k] for k in active)
    if w_total > 0:
        s_raw = sum(weights[k] * active[k] for k in active) / w_total
    else:
        s_raw = 50.0

    # Leverage financing-cost drag (additive style, capped)
    lf = _lev_frac(lev_pct)
    lev_drag = min(max(0.0, (lf - 0.25) * 0.8), 0.20)
    value = _clip(s_raw * (1 - lev_drag))

    return {
        "value": value, "components": components, "active_components": active,
        "sparse": sparse, "lev_drag": lev_drag, "roc_s": roc_s, "coverage_s": coverage_s,
        "nav_s": nav_s, "crisis_s": crisis_s,
    }


# =====================================================================
# 5.4 Peer sub-score + penalty gate
# =====================================================================
def s_peer(blended_percentile: float | None,
           self_ret_3y: float | None,
           benchmark_cagr_3y: float | None) -> dict:
    """Percentile → score plus the penalty-gate (relative AND absolute weakness)."""
    if blended_percentile is None:
        return {"value": 50.0, "penalty": False, "sparse": True}
    val = 100 * float(blended_percentile)
    abs_under = (
        None if self_ret_3y is None or benchmark_cagr_3y is None
        else (self_ret_3y - benchmark_cagr_3y)
    )
    penalty = bool(blended_percentile < 0.25 and abs_under is not None and abs_under < 0)
    return {"value": _clip(val), "penalty": penalty, "abs_under": abs_under,
            "sparse": False}


# =====================================================================
# 5.5 Composite with asymmetric penalty
# =====================================================================
def composite(
    s_disc_v: float, s_res_v: float, s_sust_v: float, s_peer_v: float,
    z1: float | None,
    current_discount_pct: float | None,
    peer_penalty_gate: bool,
    weights: dict | None = None,
    penalty_base: float | None = None,
) -> dict:
    """Weighted-mean × ``penalty_base ** total_severity``.

    ``weights`` defaults to ``config.COMPOSITE_FACTOR_WEIGHTS`` at call time;
    pass an explicit dict to override (used by the Lab what-if view).
    Weights are normalised internally so any positive scale works.
    """

    # Discount severity: max of Z-overbought and premium-territory breaches
    sev_z = 0.0 if z1 is None else max(0.0, (z1 - 1.0) / 1.0)
    sev_prem = 0.0 if current_discount_pct is None else max(
        0.0, ((-2.0) - current_discount_pct) / 5.0)
    sev_disc = min(2.0, max(sev_z, sev_prem))

    sev_res = min(2.0, max(0.0, (50 - s_res_v) / 15.0))
    sev_sust = min(2.0, max(0.0, (40 - s_sust_v) / 15.0))
    sev_peer = (min(2.0, max(0.0, (25 - s_peer_v) / 15.0))
                if peer_penalty_gate else 0.0)

    total_severity = sev_disc + sev_res + sev_sust + sev_peer
    base = penalty_base if penalty_base is not None else config.PENALTY_BASE
    multiplier = base ** total_severity

    w = weights if weights is not None else config.COMPOSITE_FACTOR_WEIGHTS
    w_disc = max(0.0, float(w.get("s_disc", 0.25)))
    w_res = max(0.0, float(w.get("s_res", 0.25)))
    w_sust = max(0.0, float(w.get("s_sust", 0.25)))
    w_peer = max(0.0, float(w.get("s_peer", 0.25)))
    w_total = w_disc + w_res + w_sust + w_peer
    if w_total <= 0:
        w_disc = w_res = w_sust = w_peer = 0.25
        w_total = 1.0
    linear = ((s_disc_v * w_disc) + (s_res_v * w_res)
              + (s_sust_v * w_sust) + (s_peer_v * w_peer)) / w_total
    return {
        "composite": round(linear * multiplier, 1),
        "linear_mean": round(linear, 2),
        "multiplier": round(multiplier, 4),
        "total_severity": round(total_severity, 3),
        "severity_disc": round(sev_disc, 3),
        "severity_res": round(sev_res, 3),
        "severity_sust": round(sev_sust, 3),
        "severity_peer": round(sev_peer, 3),
    }


# =====================================================================
# §5.6 Destructive-ROC trap classifier
# =====================================================================
def trap_classification(
    roc_pct_12m: float | None,
    roc_trend: float | None,
    nav_cagr_3y: float | None,
    coverage: float | None,
    unii_per_share: float | None,
    distribution_rate_on_nav: float | None,
    nav_total_return_3y: float | None,
    benchmark_cagr_3y: float | None,
    composition_quality: str,
    distribution_history_years: float | None,
) -> dict:
    """Tier-1 SUSPECT + Tier-2 CONFIRMED trap detection per §5.6."""
    if distribution_history_years is not None and distribution_history_years < 3:
        return {"suspect": False, "confirmed": False, "watch": True,
                "reason": "TrapWatch: <3y distribution history"}

    if composition_quality != "full":
        return {"suspect": False, "confirmed": False, "watch": False,
                "reason": None}

    suspect_reasons: list[str] = []
    if roc_pct_12m is not None and roc_pct_12m > 0.40 and (nav_cagr_3y or 0) < 0:
        suspect_reasons.append(f"ROC {roc_pct_12m:.0%} + neg NAV CAGR")
    if roc_trend is not None and roc_trend > 0.20:
        suspect_reasons.append(f"ROC trend +{roc_trend:.0%}")
    if (coverage is not None and coverage < 0.7
            and unii_per_share is not None and unii_per_share < 0):
        suspect_reasons.append("coverage<0.7 + UNII<0")
    if (nav_cagr_3y is not None and benchmark_cagr_3y is not None
            and nav_cagr_3y < 0 and benchmark_cagr_3y > 0
            and distribution_rate_on_nav is not None):
        excess = (distribution_rate_on_nav - nav_total_return_3y
                  if nav_total_return_3y is not None else 0)
        thresh = max(0.02, 0.25 * distribution_rate_on_nav)
        if excess > thresh:
            suspect_reasons.append(f"excess payout {excess:.1%}")
    suspect = len(suspect_reasons) > 0

    confirmed = False
    if (roc_pct_12m is not None and roc_pct_12m > 0.50
            and nav_total_return_3y is not None and benchmark_cagr_3y is not None):
        rel_under = nav_total_return_3y - benchmark_cagr_3y
        if rel_under < -0.02 and (
                (coverage is not None and coverage < 0.9)
                or (unii_per_share is not None and unii_per_share < 0)):
            confirmed = True

    return {
        "suspect": suspect, "confirmed": confirmed, "watch": False,
        "reason": "; ".join(suspect_reasons) if suspect_reasons else "OK",
    }

"""Pure metric computations — Z-scores, drawdowns, NAV TR, coverage, peer ranks.

This module imports ONLY numpy + pandas + stdlib + config. It never touches
the cache or the network. Every function takes a DataFrame (or scalar) and
returns a scalar / DataFrame / dict. That makes scoring deterministic and
testable in isolation.

All percentages are expressed as plain numbers (-15.0 = -15%, NOT -0.15) to
match the CEFConnect convention, *except* leverage_fraction which is the
decimal form (0.30 = 30%) consumed by leverage-multiplier calculations.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Iterable

import numpy as np
import pandas as pd

from . import config


# =====================================================================
# Discount Z-scores
# =====================================================================
def zscore(current: float | None, series: pd.Series) -> float | None:
    """Standard Z of ``current`` against the distribution of ``series``.

    Returns None if the series is too short or std is zero. Series values
    must be in the same units as ``current`` (typically discount %).
    """
    if current is None or series is None or len(series) < 12:
        return None
    s = pd.to_numeric(series, errors="coerce").dropna()
    if len(s) < 12:
        return None
    mu = float(s.mean())
    sd = float(s.std(ddof=1))
    if sd == 0 or np.isnan(sd):
        return None
    return float((current - mu) / sd)


def zscore_from_discount_history(
    current_discount: float | None,
    weekly_discount: pd.DataFrame,
    window_weeks: int = 156,  # 3Y default
) -> float | None:
    """Z of current vs the last ``window_weeks`` weekly observations."""
    if weekly_discount is None or weekly_discount.empty:
        return None
    s = weekly_discount["discount"].tail(window_weeks)
    return zscore(current_discount, s)


def median_discount(weekly_discount: pd.DataFrame, window_weeks: int = 260) -> float | None:
    """5Y (default) median of the weekly discount series."""
    if weekly_discount is None or weekly_discount.empty:
        return None
    s = pd.to_numeric(weekly_discount["discount"].tail(window_weeks),
                      errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.median())


# =====================================================================
# Drawdowns
# =====================================================================
def peak_to_trough_drawdown_pct(
    price_history: pd.DataFrame,
    start: str | date,
    end: str | date,
    price_col: str = "nav",
) -> float | None:
    """Worst peak-to-trough drawdown in [start, end] window, as a positive %.

    Returns None if the window has <5 observations.
    """
    if price_history is None or price_history.empty:
        return None
    if isinstance(start, str):
        start = date.fromisoformat(start)
    if isinstance(end, str):
        end = date.fromisoformat(end)
    df = price_history.copy()
    if "data_date" not in df.columns:
        return None
    df["data_date"] = pd.to_datetime(df["data_date"])
    mask = (df["data_date"].dt.date >= start) & (df["data_date"].dt.date <= end)
    win = df.loc[mask, price_col].dropna()
    if len(win) < 5:
        return None
    running_max = win.cummax()
    dd = (win - running_max) / running_max
    worst = float(dd.min())
    if worst >= 0:
        return 0.0
    return abs(worst) * 100.0  # positive percent


# =====================================================================
# Leverage helpers
# =====================================================================
def leverage_fraction(lev_pct: float | None) -> float:
    """Convert literal-percent leverage to decimal. Substitutes universe median when null."""
    if lev_pct is None or (isinstance(lev_pct, float) and np.isnan(lev_pct)):
        return config.UNIVERSE_MEDIAN_LEV_PCT / 100.0
    return float(lev_pct) / 100.0


def leverage_multiplier(lev_pct: float | None) -> float:
    """Resilience leverage multiplier: 1 + 0.5·lev_frac, capped at 1.40."""
    return float(min(1.0 + config.LEV_MULT_COEF * leverage_fraction(lev_pct),
                     config.LEV_MULT_CAP))


# =====================================================================
# NAV CAGR (simple price-only CAGR for trap detection)
# =====================================================================
def nav_cagr(price_history: pd.DataFrame, years: float = 3.0) -> float | None:
    """Simple NAV-only annualised growth rate over the trailing ``years`` window.

    Uses first/last NAV in the window; no reinvestment of distributions.
    Returns None if <30 NAV observations in the window.
    """
    if price_history is None or price_history.empty:
        return None
    df = price_history.copy()
    if "data_date" not in df.columns or "nav" not in df.columns:
        return None
    df["data_date"] = pd.to_datetime(df["data_date"], errors="coerce")
    df = df.dropna(subset=["data_date", "nav"]).sort_values("data_date")
    if df.empty:
        return None
    end = df["data_date"].max()
    start = end - pd.DateOffset(years=int(years))
    df = df.loc[df["data_date"] >= start]
    if len(df) < 30:
        return None
    first = float(df["nav"].iloc[0])
    last = float(df["nav"].iloc[-1])
    actual_years = (df["data_date"].iloc[-1] - df["data_date"].iloc[0]).days / 365.25
    if first <= 0 or actual_years < 0.5:
        return None
    return float((last / first) ** (1.0 / actual_years) - 1.0)


# =====================================================================
# NAV total return with period-by-period reinvestment (RD-7 BLOCKER 2)
# =====================================================================
def nav_total_return_annualised(
    price_history: pd.DataFrame,
    distribution_history: pd.DataFrame,
    years: float = 3.0,
) -> float | None:
    """Total return on NAV, period-by-period reinvestment, annualised.

    cum = product over months: (nav[t] / nav[t-1]) + dist_in_month[t] / nav[t-1]
    annualised = cum ** (1/years) - 1
    """
    if price_history is None or price_history.empty:
        return None
    df = price_history.copy()
    df["data_date"] = pd.to_datetime(df["data_date"])
    df = df.sort_values("data_date").set_index("data_date")
    if "nav" not in df.columns or df["nav"].dropna().empty:
        return None
    end_date = df.index.max()
    start_date = end_date - pd.DateOffset(years=int(years))
    df = df.loc[df.index >= start_date].copy()
    if len(df) < 12:
        return None
    monthly = df["nav"].resample("ME").last().dropna()
    if len(monthly) < 6:
        return None

    # Aggregate distributions by month (use ex_date if available else declared)
    dist_monthly = pd.Series(0.0, index=monthly.index)
    if distribution_history is not None and not distribution_history.empty:
        dh = distribution_history.copy()
        date_col = "ex_date" if "ex_date" in dh.columns else "declared_date"
        dh[date_col] = pd.to_datetime(dh[date_col], errors="coerce")
        dh = dh.dropna(subset=[date_col]).sort_values(date_col).set_index(date_col)
        dh = dh.loc[dh.index >= start_date]
        if not dh.empty:
            grouped = dh["tot_div"].resample("ME").sum()
            dist_monthly = dist_monthly.add(grouped, fill_value=0.0).reindex(
                monthly.index, fill_value=0.0)

    nav_prev = monthly.shift(1)
    monthly_return = (monthly / nav_prev) + (dist_monthly / nav_prev) - 1.0
    monthly_return = monthly_return.dropna()
    if monthly_return.empty:  # pragma: no cover - defensive; monthly already validated ≥6
        return None
    cum = float((1.0 + monthly_return).prod())
    actual_years = max((monthly.index[-1] - monthly.index[0]).days / 365.25, 0.25)
    return float(cum ** (1.0 / actual_years) - 1.0)


# =====================================================================
# Distribution cadence + composition validity (§5.3 RD-5 BLOCKERS 1 & 2)
# =====================================================================
def detect_cadence(distribution_history: pd.DataFrame) -> str:
    """Return one of 'M', 'Q', 'S', 'A', 'mixed' from regular payments."""
    if distribution_history is None or distribution_history.empty:
        return "mixed"
    dh = distribution_history.copy()
    if "special" in dh.columns:
        dh = dh[dh["special"].isna() | (dh["special"] == 0)]
    if dh.empty:
        return "mixed"
    date_col = "ex_date" if "ex_date" in dh.columns else "declared_date"
    dates = pd.to_datetime(dh[date_col], errors="coerce").dropna().sort_values()
    if len(dates) < 3:
        return "mixed"
    gaps = dates.diff().dropna().dt.days
    median_gap = float(gaps.median())
    if 20 <= median_gap <= 45:
        return "M"
    if 60 <= median_gap <= 110:
        return "Q"
    if 160 <= median_gap <= 210:
        return "S"
    if 330 <= median_gap <= 400:
        return "A"
    return "mixed"


def composition_valid(row: pd.Series) -> bool:
    """A distribution row is composition-valid iff at least one component is
    non-null AND the components sum to TotDiv within $0.001."""
    comps = [row.get("income"), row.get("capital_return"),
             row.get("capital_lt"), row.get("capital_st")]
    non_null = [c for c in comps if c is not None and not pd.isna(c)]
    if not non_null:
        return False
    tot = row.get("tot_div")
    if tot is None or pd.isna(tot):
        return False
    return abs(sum(non_null) - float(tot)) < 0.001


def roc_pct(distribution_history: pd.DataFrame, months: int = 12) -> float | None:
    """ROC% over the trailing ``months`` months (None if composition sparse)."""
    if distribution_history is None or distribution_history.empty:
        return None
    dh = distribution_history.copy()
    date_col = "ex_date" if "ex_date" in dh.columns else "declared_date"
    dh[date_col] = pd.to_datetime(dh[date_col], errors="coerce")
    dh = dh.dropna(subset=[date_col]).sort_values(date_col)
    cutoff = dh[date_col].max() - pd.DateOffset(months=months)
    dh = dh.loc[dh[date_col] >= cutoff]
    if dh.empty:
        return None
    valid = dh[dh.apply(composition_valid, axis=1)]
    if len(valid) < max(3, int(0.5 * len(dh))):
        return None
    cr = pd.to_numeric(valid["capital_return"], errors="coerce").fillna(0).sum()
    td = pd.to_numeric(valid["tot_div"], errors="coerce").sum()
    if td == 0:
        return None
    return float(cr / td)


def distribution_cuts_5y(distribution_history: pd.DataFrame, cadence: str) -> int | None:
    """Count of consecutive regular distributions where amount dropped ≥2% vs prior."""
    if cadence == "mixed" or distribution_history is None or distribution_history.empty:
        return None
    dh = distribution_history.copy()
    if "special" in dh.columns:
        dh = dh[dh["special"].isna() | (dh["special"] == 0)]
    date_col = "ex_date" if "ex_date" in dh.columns else "declared_date"
    dh[date_col] = pd.to_datetime(dh[date_col], errors="coerce")
    dh = dh.dropna(subset=[date_col]).sort_values(date_col)
    if len(dh) < 2:
        return None
    amounts = pd.to_numeric(dh["tot_div"], errors="coerce").dropna().values
    if len(amounts) < 2:
        return None
    cuts = 0
    for i in range(1, len(amounts)):
        if amounts[i] < 0.98 * amounts[i - 1]:
            cuts += 1
    return cuts


def distribution_cagr_5y(distribution_history: pd.DataFrame, cadence: str) -> float | None:
    """5Y annualised growth rate of distributions, using cadence-aware annualisation."""
    if cadence == "mixed" or distribution_history is None or distribution_history.empty:
        return None
    per_year = {"M": 12, "Q": 4, "S": 2, "A": 1}.get(cadence)
    if per_year is None:
        return None
    dh = distribution_history.copy()
    if "special" in dh.columns:
        dh = dh[dh["special"].isna() | (dh["special"] == 0)]
    date_col = "ex_date" if "ex_date" in dh.columns else "declared_date"
    dh[date_col] = pd.to_datetime(dh[date_col], errors="coerce")
    dh = dh.dropna(subset=[date_col]).sort_values(date_col)
    if len(dh) < per_year * 2:
        return None
    cutoff_end = dh[date_col].max()
    cutoff_start = cutoff_end - pd.DateOffset(years=5)
    dh = dh.loc[dh[date_col] >= cutoff_start]
    if len(dh) < per_year * 2:
        return None
    # Annualise: first per_year payments vs last per_year payments
    first = pd.to_numeric(dh["tot_div"].head(per_year), errors="coerce").sum()
    last = pd.to_numeric(dh["tot_div"].tail(per_year), errors="coerce").sum()
    if first <= 0:
        return None
    years = (dh[date_col].iloc[-1] - dh[date_col].iloc[0]).days / 365.25
    if years < 1:
        return None
    return float((last / first) ** (1.0 / years) - 1.0)


# =====================================================================
# Coverage (RD-7 BLOCKER 4): NII for fixed-income, EPS proxy otherwise
# =====================================================================
def nii_coverage(distribution_history: pd.DataFrame, months: int = 12) -> float | None:
    """Sum(Income) / Sum(TotDiv) over the trailing N months — needs valid composition."""
    if distribution_history is None or distribution_history.empty:
        return None
    dh = distribution_history.copy()
    date_col = "ex_date" if "ex_date" in dh.columns else "declared_date"
    dh[date_col] = pd.to_datetime(dh[date_col], errors="coerce")
    dh = dh.dropna(subset=[date_col]).sort_values(date_col)
    cutoff = dh[date_col].max() - pd.DateOffset(months=months)
    dh = dh.loc[dh[date_col] >= cutoff]
    valid = dh[dh.apply(composition_valid, axis=1)]
    if valid.empty:
        return None
    income = pd.to_numeric(valid["income"], errors="coerce").fillna(0).sum()
    tot = pd.to_numeric(valid["tot_div"], errors="coerce").sum()
    if tot == 0:
        return None
    return float(income / tot)


def eps_coverage(eps: float | None, current_distribution: float | None,
                 distribution_frequency: str | None) -> float | None:
    """EPS / annualised distribution.

    annualised_dist = current_distribution × periods/year, derived from frequency.
    """
    if eps is None or current_distribution is None or current_distribution == 0:
        return None
    per_year = {
        "monthly": 12, "quarterly": 4, "semi-annual": 2, "semi annual": 2,
        "annual": 1, "annually": 1,
    }.get((distribution_frequency or "").strip().lower(), 12)
    annual_dist = current_distribution * per_year
    if annual_dist == 0:  # pragma: no cover - guarded by current_distribution check above
        return None
    return float(eps / annual_dist)


def select_coverage(
    category: str | None,
    distribution_history: pd.DataFrame,
    eps: float | None,
    current_distribution: float | None,
    distribution_frequency: str | None,
) -> tuple[float | None, str]:
    """Choose NII for fixed-income, EPS otherwise; fall back to EPS if NII null."""
    if config.is_fixed_income(category):
        nii = nii_coverage(distribution_history)
        if nii is not None:
            return nii, "NII"
    eps_cov = eps_coverage(eps, current_distribution, distribution_frequency)
    return eps_cov, "EPS-proxy"


# =====================================================================
# Crisis distribution maintenance (Dad's signal — §5.3 v3.2)
# =====================================================================
def crisis_distribution_maintenance(
    distribution_history: pd.DataFrame,
    crisis_start: str | date,
    crisis_end: str | date,
    baseline_months: int = 12,
) -> float | None:
    """min_during_crisis / mean_baseline. 1.0 = fully maintained."""
    if distribution_history is None or distribution_history.empty:
        return None
    if isinstance(crisis_start, str):
        crisis_start = date.fromisoformat(crisis_start)
    if isinstance(crisis_end, str):
        crisis_end = date.fromisoformat(crisis_end)
    dh = distribution_history.copy()
    if "special" in dh.columns:
        dh = dh[dh["special"].isna() | (dh["special"] == 0)]
    date_col = "ex_date" if "ex_date" in dh.columns else "declared_date"
    dh[date_col] = pd.to_datetime(dh[date_col], errors="coerce")
    dh = dh.dropna(subset=[date_col])

    baseline_start = pd.Timestamp(crisis_start) - pd.DateOffset(months=baseline_months)
    baseline_end = pd.Timestamp(crisis_start)
    baseline = dh[(dh[date_col] >= baseline_start) & (dh[date_col] < baseline_end)]["tot_div"]
    crisis = dh[(dh[date_col] >= pd.Timestamp(crisis_start)) &
                (dh[date_col] <= pd.Timestamp(crisis_end))]["tot_div"]
    if baseline.empty or crisis.empty:
        return None
    baseline_avg = float(pd.to_numeric(baseline, errors="coerce").mean())
    crisis_min = float(pd.to_numeric(crisis, errors="coerce").min())
    if baseline_avg <= 0:
        return None
    return crisis_min / baseline_avg


# =====================================================================
# Peer percentile within category
# =====================================================================
def peer_percentile(values: pd.Series, target_value: float | None) -> float | None:
    """Return percentile (0..1) of ``target_value`` within the ``values`` series."""
    if target_value is None or values is None or values.empty:
        return None
    s = pd.to_numeric(values, errors="coerce").dropna()
    if s.empty:
        return None
    rank = float((s < target_value).sum() + 0.5 * (s == target_value).sum())
    return rank / len(s)


def blended_peer_percentile(
    df: pd.DataFrame,
    ticker: str,
    category_col: str = "category_name",
    ret_1y_col: str = "yr1_ret_on_nav",
    ret_3y_col: str = "yr3_ret_on_nav",
    weight_1y: float = 0.4,
) -> float | None:
    """0.4·1Y + 0.6·3Y percentile rank within the fund's CategoryName peer set.

    Falls back to 1Y-only if the fund or the peers lack 3Y history.
    """
    if df is None or df.empty or ticker not in set(df.get("ticker", [])):
        return None
    self_row = df[df["ticker"] == ticker].iloc[0]
    cat = self_row.get(category_col)
    if cat is None:
        return None
    peers = df[df[category_col] == cat]
    if len(peers) < 2:
        return None
    self_1y = self_row.get(ret_1y_col)
    self_3y = self_row.get(ret_3y_col)
    p1 = peer_percentile(peers[ret_1y_col], self_1y)
    p3 = peer_percentile(peers[ret_3y_col], self_3y) if self_3y is not None and pd.notna(self_3y) else None
    if p1 is None and p3 is None:
        return None
    if p3 is None:
        return float(p1)
    if p1 is None:
        return float(p3)
    return float(weight_1y * p1 + (1.0 - weight_1y) * p3)


# =====================================================================
# Liquidity gate
# =====================================================================
def passes_liquidity(market_cap_usd_m: float | None, avg_daily_volume: float | None) -> bool:
    """Plan §5.0: market cap ≥ $10M AND adv ≥ 10k shares/day."""
    if market_cap_usd_m is None or avg_daily_volume is None:
        return False
    if (isinstance(market_cap_usd_m, float) and np.isnan(market_cap_usd_m)) or \
       (isinstance(avg_daily_volume, float) and np.isnan(avg_daily_volume)):
        return False
    return (market_cap_usd_m >= config.LIQUIDITY_MIN_MARKET_CAP_USDM and
            avg_daily_volume >= config.LIQUIDITY_MIN_AVG_DAILY_VOL)

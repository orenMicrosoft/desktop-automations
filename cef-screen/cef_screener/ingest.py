"""CEFConnect HTTP client + raw fetchers.

Single source of truth for talking to https://www.cefconnect.com/api/v3/. Wraps
``requests.Session`` with retries, a realistic User-Agent / Referer / XHR header
trio (per plan §1a quirk #5), and one fetcher per documented endpoint.

All fetchers return parsed JSON (dict or list). They do NOT touch the cache;
that's ``cache.py``'s job. They do NOT validate semantics; that's the caller's
job — but they DO normalise the few documented quirks (5D reverse-order, etc.).

Public surface:
    fetch_universe()                    -> list[dict]   (361 funds, one snapshot)
    fetch_price_history(tkr, period)    -> list[dict]   (per-day price/NAV/discount)
    fetch_discount_history(tkr, period) -> list[dict]   (weekly discount series)
    fetch_distribution_history(tkr, start_date, end_date) -> list[dict]
    diagnose()                          -> dict          (smoke-test all 4 endpoints)
"""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from . import config

log = logging.getLogger(__name__)


class EndpointGone(RuntimeError):
    """Raised when an endpoint returns an unrecoverable shape change."""


_PERIODS_PRICING = ("5D", "1M", "YTD", "1Y", "3Y", "5Y", "All")
_PERIODS_DISCOUNT = ("6M", "YTD", "1Y", "3Y", "5Y", "All")


def _build_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=config.HTTP_RETRY_TOTAL,
        backoff_factor=config.HTTP_RETRY_BACKOFF,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=8)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update({
        "User-Agent": config.HTTP_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "X-Requested-With": "XMLHttpRequest",
    })
    return s


_SESSION: requests.Session | None = None


def session() -> requests.Session:
    """Module-level lazy session — keep connection pool warm across fetches."""
    global _SESSION
    if _SESSION is None:
        _SESSION = _build_session()
    return _SESSION


def _get_json(path: str, referer: str | None = None) -> Any:
    url = f"{config.CEFCONNECT_BASE}{path}"
    headers = {"Referer": referer or "https://www.cefconnect.com/"}
    log.debug("GET %s", url)
    r = session().get(url, headers=headers, timeout=config.HTTP_TIMEOUT_SEC)
    if r.status_code == 404:
        raise EndpointGone(f"404 from {url} — endpoint may have been renamed")
    r.raise_for_status()
    return r.json()


# =====================================================================
# 1. /DailyPricing — universe snapshot
# =====================================================================
def fetch_universe() -> list[dict]:
    """Return the full DailyPricing universe (~361 funds, ~620KB JSON)."""
    data = _get_json("/DailyPricing")
    if not isinstance(data, list) or len(data) < 100:
        raise EndpointGone(
            f"DailyPricing returned unexpected shape: type={type(data).__name__}, "
            f"len={len(data) if hasattr(data, '__len__') else 'n/a'}"
        )
    return data


# =====================================================================
# 2. /PricingHistory/{tkr}/{period} — daily price/NAV/discount
# =====================================================================
def fetch_price_history(ticker: str, period: str = "1M") -> list[dict]:
    """Fetch price/NAV/discount history for one ticker.

    Per plan §1a:
    - Period codes are STRINGS ("1Y", "3Y", "All"); numeric values silently
      return empty.
    - /5D is reverse-chronological; we normalise to ascending here.
    - Latest data may lag /All by up to 5 trading days (weekly refresh).
    """
    if period not in _PERIODS_PRICING:
        raise ValueError(f"Invalid pricing period {period!r}; want one of {_PERIODS_PRICING}")
    referer = f"https://www.cefconnect.com/fund/{ticker}"
    payload = _get_json(f"/PricingHistory/{ticker}/{period}", referer=referer)
    rows = (payload or {}).get("Data", {}).get("PriceHistory", []) or []
    # Normalise to ascending DataDate
    rows.sort(key=lambda r: r.get("DataDate") or "")
    return rows


# =====================================================================
# 3. /DiscountCharter/fund/{tkr}/{period} — weekly discount baseline
# =====================================================================
def fetch_discount_history(ticker: str, period: str = "5Y") -> list[dict]:
    if period not in _PERIODS_DISCOUNT:
        raise ValueError(f"Invalid discount period {period!r}; want one of {_PERIODS_DISCOUNT}")
    referer = f"https://www.cefconnect.com/fund/{ticker}"
    payload = _get_json(f"/DiscountCharter/fund/{ticker}/{period}", referer=referer)
    rows = (payload or {}).get("Data", []) or []
    rows.sort(key=lambda r: r.get("DataDate") or "")
    return rows


# =====================================================================
# 4. /distributionhistory/fund/{tkr}/{startDate}/{endDate}
# =====================================================================
def fetch_distribution_history(
    ticker: str,
    start: date | str,
    end: date | str,
) -> list[dict]:
    """Per-distribution NII/ROC/CapGain breakdown.

    Path must use ``YYYY-MM-DD`` (dashes) — slashes 404 per plan §1a quirk #7.
    Empirically backfills 5+ years even though the page disclaimer says "past
    year only" (verified).
    """
    s = _fmt_date(start)
    e = _fmt_date(end)
    referer = f"https://www.cefconnect.com/fund/{ticker}"
    payload = _get_json(f"/distributionhistory/fund/{ticker}/{s}/{e}", referer=referer)
    rows = (payload or {}).get("Data", []) or [] if isinstance(payload, dict) else []
    return rows


def _fmt_date(d: date | str) -> str:
    if isinstance(d, str):
        return d  # already formatted
    return d.strftime("%Y-%m-%d")


# =====================================================================
# Diagnose — does what `cef-screen --diagnose` does in the plan
# =====================================================================
def diagnose(sample_ticker: str = "PFL") -> dict:
    """One-shot smoke test of every endpoint. Returns {endpoint: status_str}."""
    out: dict[str, str] = {}
    try:
        u = fetch_universe()
        out["DailyPricing"] = f"ok ({len(u)} funds)"
    except Exception as e:  # noqa: BLE001
        out["DailyPricing"] = f"FAIL: {e!r}"
    try:
        ph = fetch_price_history(sample_ticker, "1M")
        out["PricingHistory"] = f"ok ({len(ph)} rows)"
    except Exception as e:  # noqa: BLE001
        out["PricingHistory"] = f"FAIL: {e!r}"
    try:
        dh = fetch_discount_history(sample_ticker, "1Y")
        out["DiscountCharter"] = f"ok ({len(dh)} rows)"
    except Exception as e:  # noqa: BLE001
        out["DiscountCharter"] = f"FAIL: {e!r}"
    try:
        today = date.today()
        dist = fetch_distribution_history(
            sample_ticker, today - timedelta(days=365), today
        )
        out["distributionhistory"] = f"ok ({len(dist)} rows)"
    except Exception as e:  # noqa: BLE001
        out["distributionhistory"] = f"FAIL: {e!r}"
    return out

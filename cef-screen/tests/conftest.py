"""Shared pytest fixtures.

Every test gets:
- ``cache_path`` — a temp SQLite cache file (CEF_SCREENER_CACHE_DIR env override)
- ``initialised_cache`` — same, with schema applied
- ``mock_universe`` — synthetic DailyPricing rows that match the real schema
- ``mock_price_history`` / ``mock_discount_history`` / ``mock_distribution_history``
"""
from __future__ import annotations

import importlib
import os
from datetime import date, timedelta
from pathlib import Path

import pytest


@pytest.fixture(autouse=True)
def _isolate_cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point every module's cache at a per-test temp dir. Autouse → no test
    can ever touch the real %LOCALAPPDATA%\\cef_screener cache by accident."""
    cdir = tmp_path / "cef_cache"
    cdir.mkdir()
    monkeypatch.setenv("CEF_SCREENER_CACHE_DIR", str(cdir))
    # Force a fresh module import so config.cache_dir() re-reads the env var
    import cef_screener.config as _cfg
    importlib.reload(_cfg)
    import cef_screener.cache as _cache
    importlib.reload(_cache)
    import cef_screener.ingest as _ingest
    importlib.reload(_ingest)
    return cdir


@pytest.fixture
def cache_path(_isolate_cache_dir: Path) -> Path:
    return _isolate_cache_dir / "cache.sqlite"


@pytest.fixture
def initialised_cache(cache_path: Path) -> Path:
    from cef_screener import cache
    cache.init_db(cache_path)
    return cache_path


def _mk_universe_row(
    ticker: str,
    category: str = "Taxable Bond",
    sponsor: str = "PIMCO",
    nav: float = 10.0,
    price: float = 9.0,
    discount: float = -10.0,
    z1: float = -2.0,
    leverage_pct: float | None = 25.0,
    avg_daily_volume: float = 100_000,
    market_cap_m: float = 500.0,
    return_on_nav: float = 6.0,
    yr3_ret: float = 4.0,
    unii: float | None = 0.05,
    eps: float = 0.5,
    current_dist: float = 0.8,
    dist_freq: str = "Monthly",
    last_updated: str = "2026-05-22T00:00:00",
) -> dict:
    """Build one DailyPricing-shaped row with the fields the cache needs."""
    return {
        "Ticker": ticker,
        "Name": f"{ticker} Fund",
        "SponsorName": sponsor,
        "CategoryName": category,
        "Price": price,
        "NAV": nav,
        "Discount": discount,
        "DistributionRatePrice": (current_dist * 12 / price) * 100 if price else 0,
        "DistributionRateNAV": (current_dist * 12 / nav) * 100 if nav else 0,
        "ReturnOnNAV": return_on_nav,
        "Yr3RetOnNav": yr3_ret,
        "Yr5RetOnNav": yr3_ret * 0.9,
        "ZScore1Yr": z1,
        "ZScore3M": z1 * 0.6,
        "ZScore6M": z1 * 0.8,
        "Discount52WkAvg": discount + 2,
        "UNIIPerShare": unii,
        "EarningsPerShare": eps,
        "CurrentDistribution": current_dist,
        "DistributionFrequency": dist_freq,
        "LeverageRatioPercentage": leverage_pct,
        "IsLeveraged": leverage_pct is not None and leverage_pct > 0,
        "MarketCapUSDm": market_cap_m,
        "AvgDailyVolume": avg_daily_volume,
        "ExpenseRatio": 1.2,
        "NavTicker": f"X{ticker}X",
        "IsManagedDistribution": False,
        "LastUpdated": last_updated,
        "NAVPublished": last_updated,
    }


@pytest.fixture
def mock_universe() -> list[dict]:
    """50-fund synthetic universe spanning all leverage tiers + categories."""
    rows = []
    categories = [
        "Taxable Bond", "High Yield", "Senior Loan",
        "Municipal", "Preferred", "US Equity", "Real Estate",
    ]
    sponsors = ["PIMCO", "BlackRock", "Nuveen", "Eaton Vance"]
    for i in range(50):
        cat = categories[i % len(categories)]
        sponsor = sponsors[i % len(sponsors)]
        # Spread Z1 across [-3.5, +1.0] so gatekeeper has work to do
        z1 = -3.5 + (i * 0.1)
        # Spread leverage across all tiers
        lev = [0, 5, 20, 35, 50, None][i % 6]
        rows.append(_mk_universe_row(
            ticker=f"T{i:02d}",
            category=cat,
            sponsor=sponsor,
            nav=10.0 + (i % 5),
            price=(10.0 + (i % 5)) * (1 + (i % 7 - 3) * 0.02),
            discount=-15 + (i % 10),
            z1=round(z1, 2),
            leverage_pct=lev,
            avg_daily_volume=50_000 if i % 8 else 5_000,  # some illiquid
            market_cap_m=200 if i % 9 else 5.0,            # some tiny
            return_on_nav=8 - (i % 6),
            yr3_ret=5 - (i % 4),
            unii=0.05 if i % 3 else -0.02,
        ))
    return rows


@pytest.fixture
def mock_price_history_rows() -> list[dict]:
    """200 trading days of price/NAV/discount around a synthetic shock."""
    rows = []
    base = date(2025, 1, 2)
    for i in range(260):  # ~1 year of trading days
        d = base + timedelta(days=i)
        # Synthesise a 15% drawdown in days 50-80
        drawdown = -0.15 if 50 <= i <= 80 else 0.0
        nav = 10.0 * (1 + 0.0002 * i + drawdown)
        price = nav * 0.92
        rows.append({
            "DataDate": d.isoformat(),
            "Data": round(price, 4),
            "NAVData": round(nav, 4),
            "DiscountData": round((price - nav) / nav * 100, 2),
        })
    return rows


@pytest.fixture
def mock_discount_history_rows() -> list[dict]:
    """52 weekly discount observations."""
    base = date(2025, 1, 5)
    return [
        {"DataDate": (base + timedelta(weeks=i)).isoformat(),
         "Data": round(-8 + (i % 7 - 3), 2)}
        for i in range(52)
    ]


@pytest.fixture
def mock_distribution_history_rows() -> list[dict]:
    """12 monthly distributions with NII/ROC composition."""
    base = date(2025, 5, 15)
    rows = []
    for i in range(12):
        d = (base.replace(month=((base.month - 1 + i) % 12) + 1,
                          year=base.year + ((base.month - 1 + i) // 12)))
        rows.append({
            "DeclaredDateDisplay": d.isoformat(),
            "ExDivDateDisplay": d.isoformat(),
            "PayDateDisplay": d.isoformat(),
            "TotDiv": 0.08,
            "Income": 0.06,
            "CapitalReturn": 0.02,
            "CapitalLT": 0.0,
            "CapitalST": 0.0,
            "Special": None,
        })
    return rows

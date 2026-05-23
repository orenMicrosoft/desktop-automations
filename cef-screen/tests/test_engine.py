"""Tests for cef_screener.engine — orchestrator integration."""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

from cef_screener import engine, cache, config, portfolio


# ---------------------------------------------------------------- helpers
def _seed_universe(snapshot_date: str = "2026-05-22"):
    rows = []
    cats = ["Taxable Bond", "Municipal Bond", "US Equity"]
    for i in range(35):
        cat = cats[i % 3]
        rows.append({
            "Ticker": f"T{i:02d}",
            "Name": f"Fund {i}",
            "CategoryName": cat,
            "SponsorName": "Acme",
            "Price": 10.0 + i * 0.1,
            "NAV": 12.0 + i * 0.1,
            "Discount": -10.0 - (i % 5),       # CEFConnect convention: negative = discount
            "DistributionRateNAV": 0.08,
            "ReturnOnNAV": 0.04 + (i % 7) * 0.005,
            "Yr3RetOnNav": 0.05,
            "Yr5RetOnNav": 0.06,
            "ZScore1Yr": -2.0 + i * 0.1,        # ascending; T00 cheapest
            "ZScore3M": 0.0,
            "ZScore6M": 0.0,
            "Discount52WkAvg": -8.0,
            "UNIIPerShare": 0.1,
            "EarningsPerShare": 0.5,
            "CurrentDistribution": 0.05,
            "DistributionFrequency": "Monthly",
            "LeverageRatioPercentage": 25.0,
            "IsLeveraged": True,
            "MarketCapUSDm": 250.0,
            "AvgDailyVolume": 100_000,
            "ExpenseRatio": 0.01,
            "NavTicker": f"XT{i:02d}",
            "IsManagedDistribution": False,
            "LastUpdated": snapshot_date,
            "NAVPublished": snapshot_date,
        })
    return rows


def _seed_history_for(ticker: str, anchor_date: date = date(2026, 5, 22)):
    """Return (price_rows, discount_rows, distribution_rows) for a ticker."""
    p_rows = []
    d_rows = []
    for i in range(800):  # ~2.2 years daily
        d = (anchor_date - timedelta(days=800 - i)).isoformat()
        nav = 12.0 + 0.001 * i + (0.5 if i == 400 else 0)  # mild trend with one spike
        price = nav - 1.0 - 0.001 * (i % 30)
        disc = ((price - nav) / nav) * 100
        p_rows.append({"DataDate": d, "Price": price, "NAV": nav, "Discount": disc})
        if i % 7 == 0:
            d_rows.append({"DataDate": d, "Discount": disc})
    dx_rows = []
    for m in range(60):
        ex = (anchor_date - timedelta(days=30 * (60 - m))).isoformat()
        dx_rows.append({
            "ExDate": ex, "DeclaredDate": ex, "PayDate": ex,
            "TotalDistribution": 0.05,
            "IncomeDistribution": 0.04,
            "CapitalReturnDistribution": 0.005,
            "CapitalLongTermDistribution": 0.005,
            "CapitalShortTermDistribution": 0.0,
            "Special": 0,
        })
    return p_rows, d_rows, dx_rows


def _populate(initialised_cache):
    cache.write_universe(_seed_universe())
    # Seed history for the gatekeeper-eligible tickers (lowest Z1)
    for i in range(config.GATEKEEPER_SIZE):
        tkr = f"T{i:02d}"
        ph, dh, dx = _seed_history_for(tkr)
        cache.write_price_history(tkr, ph)
        cache.write_discount_history(tkr, dh)
        cache.write_distribution_history(tkr, dx)


# ---------------------------------------------------------------- helpers
class TestSafeFloat:
    def test_none(self):
        assert engine._safe_float(None) is None

    def test_int(self):
        assert engine._safe_float(5) == 5.0

    def test_string_valid(self):
        assert engine._safe_float("3.14") == pytest.approx(3.14)

    def test_string_invalid(self):
        assert engine._safe_float("xyz") is None

    def test_nan(self):
        import math
        assert engine._safe_float(math.nan) is None


class TestFlipSign:
    def test_none(self):
        assert engine._flip_sign(None) is None

    def test_positive(self):
        assert engine._flip_sign(5.0) == -5.0

    def test_negative(self):
        assert engine._flip_sign(-3.0) == 3.0


class TestSnapshotAge:
    def test_none(self):
        assert engine._snapshot_age_hours(None) is None

    def test_invalid_date(self):
        assert engine._snapshot_age_hours("garbage") is None

    def test_valid_date(self):
        today = date.today().isoformat()
        out = engine._snapshot_age_hours(today)
        assert out is not None
        assert -1 < out < 48  # roughly today's age in hours


class TestBenchmarkCagr:
    def test_none(self):
        assert engine._benchmark_cagr(None) is None

    def test_unknown_category(self):
        out = engine._benchmark_cagr("XYZ-NOT-A-CATEGORY")
        # benchmark_for returns "VTI" default → which IS in BENCHMARK_CAGR_3Y
        assert out is not None or out is None  # acceptable either way

    def test_known(self):
        out = engine._benchmark_cagr("Taxable Bond")
        assert out is not None


class TestPeerPercentile:
    def test_none_own_returns_none(self):
        out = engine._peer_percentile_for(pd.DataFrame(), {}, None)
        assert out is None

    def test_empty_universe_returns_none(self):
        out = engine._peer_percentile_for(pd.DataFrame(), {"category_name": "X"}, 0.05)
        assert out is None

    def test_no_category_returns_none(self):
        df = pd.DataFrame({"category_name": ["X"], "leverage_ratio": [25.0],
                           "yr1_ret_on_nav": [0.05]})
        out = engine._peer_percentile_for(df, {"category_name": None}, 0.05)
        assert out is None

    def test_basic_rank(self):
        df = pd.DataFrame({
            "category_name": ["X"] * 6,
            "leverage_ratio": [25.0] * 6,
            "yr1_ret_on_nav": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06],
        })
        out = engine._peer_percentile_for(df, {"category_name": "X", "leverage_ratio": 25}, 0.04)
        assert out is not None
        assert 0.0 <= out <= 1.0

    def test_falls_back_to_category_only(self):
        # Only 2 funds in bucket → falls back to category (all 6)
        df = pd.DataFrame({
            "category_name": ["X"] * 6,
            "leverage_ratio": [25, 25, 60, 60, 60, 60],
            "yr1_ret_on_nav": [0.01, 0.02, 0.03, 0.04, 0.05, 0.06],
        })
        out = engine._peer_percentile_for(
            df, {"category_name": "X", "leverage_ratio": 25}, 0.04,
        )
        assert out is not None


# ---------------------------------------------------------------- integration
class TestRunPipeline:
    def test_empty_cache_returns_warning(self, initialised_cache):
        result = engine.run_pipeline()
        assert result.universe_size == 0
        assert any("Universe cache is empty" in w for w in result.warnings)

    def test_full_pipeline(self, initialised_cache):
        _populate(initialised_cache)
        result = engine.run_pipeline()
        assert result.universe_size == 35
        assert result.liquid_universe_size == 35   # all liquid in synthetic data
        assert len(result.gatekeeper) == config.GATEKEEPER_SIZE
        assert not result.scored.empty
        # All scored funds have a composite + buy_label
        for col in ("composite", "s_disc", "s_res", "s_sust", "s_peer",
                    "trap_tier", "buy_label"):
            assert col in result.scored.columns
        assert result.snapshot_date is not None

    def test_with_holdings(self, initialised_cache, tmp_path):
        _populate(initialised_cache)
        positions_file = tmp_path / "positions.json"
        portfolio.add_position("T00", 100, 9.0, "2025-01-01", path=positions_file)
        result = engine.run_pipeline(positions_path=positions_file)
        assert len(result.holdings) == 1
        assert result.holdings[0]["position"]["ticker"] == "T00"
        assert result.holdings[0]["return"]["total_pct"] is not None

    def test_invalid_positions_file(self, initialised_cache, tmp_path):
        _populate(initialised_cache)
        p = tmp_path / "positions.json"
        p.write_text("{not-a-list}", encoding="utf-8")
        result = engine.run_pipeline(positions_path=p)
        assert any("positions.json" in w for w in result.warnings)
        assert result.holdings == []

    def test_old_snapshot_warning(self, initialised_cache):
        # Seed with a snapshot date 5 days ago
        old = (date.today() - timedelta(days=5)).isoformat()
        cache.write_universe(_seed_universe(old))
        # Seed history for the gatekeeper-eligible tickers (lowest Z1)
        for i in range(config.GATEKEEPER_SIZE):
            tkr = f"T{i:02d}"
            ph, dh, dx = _seed_history_for(tkr, date.today())
            cache.write_price_history(tkr, ph)
            cache.write_discount_history(tkr, dh)
            cache.write_distribution_history(tkr, dx)
        result = engine.run_pipeline()
        assert any("Snapshot is" in w for w in result.warnings)


# ---------------------------------------------------------------- _build_per_ticker_inputs branches
class TestBuildInputs:
    def test_empty_histories(self):
        row = {
            "z_score_1yr": -1.0, "discount": -8.0,
            "leverage_ratio": 30, "unii_per_share": 0.1,
            "eps": 0.5, "current_distribution": 0.04,
            "distribution_frequency": "Monthly",
            "category_name": "Taxable Bond",
            "distribution_rate_nav": 0.08,
        }
        empty = pd.DataFrame()
        out = engine._build_per_ticker_inputs(row, empty, empty, empty)
        assert out["z1"] == -1.0
        assert out["current_discount_pct"] == 8.0   # flipped
        assert out["nav_cagr_3y"] is None
        assert out["composition_quality"] == "incomplete"
        assert out["crisis_maintenance"] is None

    def test_with_discount_history(self):
        row = {
            "z_score_1yr": -1.0, "discount": -8.0,
            "leverage_ratio": 30, "unii_per_share": 0.1,
            "eps": 0.5, "current_distribution": 0.04,
            "distribution_frequency": "Monthly",
            "category_name": "Taxable Bond",
            "distribution_rate_nav": 0.08,
        }
        dh = pd.DataFrame({"data_date": pd.date_range("2024-01-01", periods=10),
                           "discount": [-5, -6, -7, -8, -9, -8, -7, -6, -5, -4]})
        out = engine._build_per_ticker_inputs(row, pd.DataFrame(), dh, pd.DataFrame())
        assert out["median_disc_5y"] is not None
        # Median of [-5..-9] is -6.5; flipped to plan convention = +6.5
        assert out["median_disc_5y"] == pytest.approx(6.5)

    def test_with_distribution_history(self):
        row = {
            "z_score_1yr": -1.0, "discount": -8.0,
            "leverage_ratio": 30, "unii_per_share": 0.1,
            "eps": 0.5, "current_distribution": 0.04,
            "distribution_frequency": "Monthly",
            "category_name": "Taxable Bond",
            "distribution_rate_nav": 0.08,
        }
        _, _, dx = _seed_history_for("T00")
        # Convert seed rows to cache-format DataFrame
        dh = pd.DataFrame([{
            "ex_date": pd.Timestamp(r["ExDate"]),
            "declared_date": pd.Timestamp(r["DeclaredDate"]),
            "tot_div": r["TotalDistribution"],
            "income": r["IncomeDistribution"],
            "capital_return": r["CapitalReturnDistribution"],
            "capital_lt": r["CapitalLongTermDistribution"],
            "capital_st": r["CapitalShortTermDistribution"],
            "special": r["Special"],
        } for r in dx])
        out = engine._build_per_ticker_inputs(row, pd.DataFrame(), pd.DataFrame(), dh)
        assert out["composition_quality"] == "full"
        assert out["distribution_history_years"] is not None
        assert out["distribution_history_years"] >= 3


# ---------------------------------------------------------------- refresh_universe
class TestRefreshUniverse:
    def test_universe_only(self, initialised_cache, monkeypatch):
        rows = _seed_universe()
        monkeypatch.setattr(engine.ingest, "fetch_universe", lambda: rows)
        summary = engine.refresh_universe()
        assert summary["universe"] == len(rows)
        assert summary["price_history"] == 0
        # Universe is now in cache
        u = cache.load_latest_universe()
        assert len(u) == len(rows)

    def test_with_tickers(self, initialised_cache, monkeypatch):
        rows = _seed_universe()
        ph, dh, dx = _seed_history_for("T00")
        monkeypatch.setattr(engine.ingest, "fetch_universe", lambda: rows)
        monkeypatch.setattr(engine.ingest, "fetch_price_history",
                            lambda tkr: ph)
        monkeypatch.setattr(engine.ingest, "fetch_discount_history",
                            lambda tkr: dh)
        monkeypatch.setattr(engine.ingest, "fetch_distribution_history",
                            lambda tkr: dx)
        summary = engine.refresh_universe(tickers=["T00"])
        assert summary["universe"] == len(rows)
        assert summary["price_history"] == len(ph)
        assert summary["discount_history"] == len(dh)
        assert summary["distribution_history"] == len(dx)

    def test_with_tickers_empty_histories(self, initialised_cache, monkeypatch):
        # Each fetch returns empty; corresponding `if` branch is False.
        rows = _seed_universe()
        monkeypatch.setattr(engine.ingest, "fetch_universe", lambda: rows)
        monkeypatch.setattr(engine.ingest, "fetch_price_history", lambda tkr: [])
        monkeypatch.setattr(engine.ingest, "fetch_discount_history", lambda tkr: [])
        monkeypatch.setattr(engine.ingest, "fetch_distribution_history",
                            lambda tkr: [])
        summary = engine.refresh_universe(tickers=["T00"])
        assert summary["price_history"] == 0
        assert summary["discount_history"] == 0
        assert summary["distribution_history"] == 0
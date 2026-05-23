"""Tests for cef_screener.portfolio."""
from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path

import pytest

from cef_screener import portfolio


# ---------------------------------------------------------------- Position
class TestPosition:
    def test_roundtrip_dict(self):
        p = portfolio.Position("pfl", 100, 10.0, "2024-01-15")
        d = p.to_dict()
        assert d == {"ticker": "pfl", "shares": 100.0, "cost_basis": 10.0,
                     "purchase_date": "2024-01-15"}
        p2 = portfolio.Position.from_dict(d)
        # from_dict uppercases the ticker
        assert p2.ticker == "PFL"
        assert p2.shares == 100.0


# ---------------------------------------------------------------- IO
class TestIO:
    def test_load_missing_file_returns_empty(self, tmp_path):
        out = portfolio.load_positions(tmp_path / "nope.json")
        assert out == []

    def test_load_empty_file_returns_empty(self, tmp_path):
        p = tmp_path / "positions.json"
        p.write_text("", encoding="utf-8")
        assert portfolio.load_positions(p) == []

    def test_load_whitespace_only_returns_empty(self, tmp_path):
        p = tmp_path / "positions.json"
        p.write_text("   \n  \t  ", encoding="utf-8")
        assert portfolio.load_positions(p) == []

    def test_load_invalid_root_raises(self, tmp_path):
        p = tmp_path / "positions.json"
        p.write_text('{"not": "a list"}', encoding="utf-8")
        with pytest.raises(ValueError):
            portfolio.load_positions(p)

    def test_save_and_load_roundtrip(self, tmp_path):
        p = tmp_path / "positions.json"
        positions = [portfolio.Position("PFL", 100, 10.5, "2024-01-15")]
        portfolio.save_positions(positions, p)
        loaded = portfolio.load_positions(p)
        assert len(loaded) == 1
        assert loaded[0].ticker == "PFL"
        assert loaded[0].cost_basis == 10.5

    def test_save_creates_parent_dirs(self, tmp_path):
        target = tmp_path / "sub" / "dir" / "positions.json"
        portfolio.save_positions([portfolio.Position("X", 1, 1, "2024-01-01")], target)
        assert target.exists()


# ---------------------------------------------------------------- add/remove
class TestAddRemove:
    def test_add_new_position(self, tmp_path):
        p = tmp_path / "positions.json"
        result = portfolio.add_position("PFL", 100, 10.0, "2024-01-15", path=p)
        assert len(result) == 1
        assert result[0].ticker == "PFL"

    def test_add_replaces_existing(self, tmp_path):
        p = tmp_path / "positions.json"
        portfolio.add_position("PFL", 100, 10.0, "2024-01-15", path=p)
        result = portfolio.add_position("pfl", 200, 11.0, "2024-06-01", path=p)
        assert len(result) == 1
        assert result[0].shares == 200
        assert result[0].cost_basis == 11.0

    def test_add_default_purchase_date_today(self, tmp_path):
        p = tmp_path / "positions.json"
        result = portfolio.add_position("PFL", 100, 10.0, path=p)
        assert result[0].purchase_date == date.today().isoformat()

    def test_remove_existing(self, tmp_path):
        p = tmp_path / "positions.json"
        portfolio.add_position("PFL", 100, 10.0, "2024-01-15", path=p)
        portfolio.add_position("PTY", 50, 16.0, "2024-02-01", path=p)
        result = portfolio.remove_position("PFL", path=p)
        assert len(result) == 1
        assert result[0].ticker == "PTY"

    def test_remove_nonexistent_no_error(self, tmp_path):
        p = tmp_path / "positions.json"
        portfolio.add_position("PFL", 100, 10.0, "2024-01-15", path=p)
        result = portfolio.remove_position("XYZ", path=p)
        assert len(result) == 1


# ---------------------------------------------------------------- _parse_iso
class TestParseIso:
    def test_none(self):
        assert portfolio._parse_iso(None) is None

    def test_string_iso(self):
        assert portfolio._parse_iso("2024-01-15") == date(2024, 1, 15)

    def test_datetime_obj(self):
        assert portfolio._parse_iso(datetime(2024, 1, 15, 10, 30)) == date(2024, 1, 15)

    def test_date_obj(self):
        d = date(2024, 1, 15)
        assert portfolio._parse_iso(d) == d

    def test_invalid_string(self):
        assert portfolio._parse_iso("not-a-date") is None

    def test_string_with_extra_chars(self):
        # Truncates to first 10 chars then parses
        assert portfolio._parse_iso("2024-01-15T10:00:00") == date(2024, 1, 15)

    def test_nat_value(self):
        import pandas as pd
        assert portfolio._parse_iso(pd.NaT) is None

    def test_pd_timestamp(self):
        import pandas as pd
        assert portfolio._parse_iso(pd.Timestamp("2024-01-15")) == date(2024, 1, 15)

    def test_isna_typeerror_path(self):
        # An object whose __bool__ raises TypeError when passed through pd.isna
        # forces the except branch — e.g., a custom sentinel.
        class Weird:
            def __repr__(self):
                return "Weird()"
        # pd.isna on Weird() returns False (no exception), then it falls through to
        # fromisoformat which raises ValueError → returns None.
        # To hit the except branch we need pd.isna to raise. Pass a list of dates
        # which causes pd.isna to return an array; bool(array) raises ValueError.
        assert portfolio._parse_iso([1, 2, 3]) is None


# ---------------------------------------------------------------- distributions_since
class TestDistributionsSince:
    def test_basic_sum(self):
        rows = [
            {"ex_date": "2024-01-31", "tot_div": 0.10},
            {"ex_date": "2024-02-28", "tot_div": 0.10},
            {"ex_date": "2024-03-31", "tot_div": 0.10},
        ]
        # Purchased 2024-01-15, distributions on 1/31, 2/28, 3/31 all count
        assert abs(portfolio.distributions_since(rows, "2024-01-15") - 0.30) < 1e-9

    def test_strictly_after(self):
        rows = [{"ex_date": "2024-01-15", "tot_div": 0.10}]
        # Same day → excluded
        assert portfolio.distributions_since(rows, "2024-01-15") == 0.0

    def test_falls_back_to_declared_date(self):
        rows = [{"declared_date": "2024-02-01", "tot_div": 0.20}]
        assert abs(portfolio.distributions_since(rows, "2024-01-15") - 0.20) < 1e-9

    def test_invalid_purchase_returns_zero(self):
        rows = [{"ex_date": "2024-02-01", "tot_div": 0.10}]
        assert portfolio.distributions_since(rows, "garbage") == 0.0

    def test_skips_unparseable_dates(self):
        rows = [
            {"ex_date": None, "declared_date": None, "tot_div": 0.10},
            {"ex_date": "2024-02-01", "tot_div": 0.20},
        ]
        assert abs(portfolio.distributions_since(rows, "2024-01-15") - 0.20) < 1e-9

    def test_skips_none_tot_div(self):
        rows = [
            {"ex_date": "2024-02-01", "tot_div": None},
            {"ex_date": "2024-03-01", "tot_div": 0.20},
        ]
        assert abs(portfolio.distributions_since(rows, "2024-01-15") - 0.20) < 1e-9

    def test_skips_garbage_tot_div(self):
        rows = [
            {"ex_date": "2024-02-01", "tot_div": "not-a-number"},
            {"ex_date": "2024-03-01", "tot_div": 0.20},
        ]
        assert abs(portfolio.distributions_since(rows, "2024-01-15") - 0.20) < 1e-9


# ---------------------------------------------------------------- position_return
class TestPositionReturn:
    def test_no_price_returns_none(self):
        pos = portfolio.Position("PFL", 100, 10.0, "2024-01-15")
        out = portfolio.position_return(pos, None, [])
        assert out["price_pct"] is None
        assert out["total_pct"] is None

    def test_zero_cost_basis_returns_none(self):
        pos = portfolio.Position("PFL", 100, 0.0, "2024-01-15")
        out = portfolio.position_return(pos, 12.0, [])
        assert out["price_pct"] is None

    def test_pure_price_gain(self):
        pos = portfolio.Position("PFL", 100, 10.0, "2024-01-15")
        out = portfolio.position_return(pos, 12.0, [])
        assert abs(out["price_pct"] - 0.20) < 1e-9
        assert out["dist_pct"] == 0.0
        assert abs(out["total_pct"] - 0.20) < 1e-9

    def test_price_plus_distributions(self):
        pos = portfolio.Position("PFL", 100, 10.0, "2024-01-15")
        rows = [
            {"ex_date": "2024-02-01", "tot_div": 0.10},
            {"ex_date": "2024-03-01", "tot_div": 0.10},
        ]
        out = portfolio.position_return(pos, 11.0, rows)
        # price gain 10% + dist 0.20/10 = 2% → total 12%
        assert abs(out["total_pct"] - 0.12) < 1e-9
        assert out["dist_dollars_per_share"] == pytest.approx(0.20)

    def test_handles_none_distribution_list(self):
        pos = portfolio.Position("PFL", 100, 10.0, "2024-01-15")
        out = portfolio.position_return(pos, 11.0, None)
        assert abs(out["total_pct"] - 0.10) < 1e-9


# ---------------------------------------------------------------- evaluate_position
class TestEvaluatePosition:
    def test_clean_hold(self):
        pos = portfolio.Position("PFL", 100, 10.0, "2024-01-15")
        out = portfolio.evaluate_position(
            pos, current_price=10.5, z1=-0.5, z3=-0.3, distribution_rows=[],
        )
        assert out["urgency"] == 0
        assert out["return"]["total_pct"] == pytest.approx(0.05)

    def test_sell_now_z1_hard(self):
        pos = portfolio.Position("PFL", 100, 10.0, "2024-01-15")
        out = portfolio.evaluate_position(
            pos, current_price=11.0, z1=2.5, z3=1.5, distribution_rows=[],
        )
        assert out["urgency"] == 3

    def test_stop_loss_triggers(self):
        pos = portfolio.Position("PFL", 100, 10.0, "2024-01-15")
        out = portfolio.evaluate_position(
            pos, current_price=7.5, z1=-1.0, z3=-0.5, distribution_rows=[],
        )
        assert out["urgency"] == 3
        assert any("STOP-LOSS" in t for t in out["triggers"])

    def test_no_current_price(self):
        pos = portfolio.Position("PFL", 100, 10.0, "2024-01-15")
        out = portfolio.evaluate_position(
            pos, current_price=None, z1=-1.0, z3=-0.5,
        )
        assert out["return"]["total_pct"] is None
        assert out["urgency"] == 0  # no return_pct → no return-based triggers


# ---------------------------------------------------------------- evaluate_portfolio
class TestEvaluatePortfolio:
    def test_orders_by_urgency_desc(self):
        positions = [
            portfolio.Position("HOLD", 100, 10.0, "2024-01-15"),
            portfolio.Position("SELL", 100, 10.0, "2024-01-15"),
        ]
        snapshots = {
            "HOLD": {"price": 10.2, "z1": -0.5, "z3": -0.3},
            "SELL": {"price": 7.0, "z1": 2.5, "z3": 0.0},
        }
        out = portfolio.evaluate_portfolio(positions, snapshots)
        assert out[0]["position"]["ticker"] == "SELL"
        assert out[0]["urgency"] >= out[1]["urgency"]

    def test_handles_missing_snapshot(self):
        positions = [portfolio.Position("XYZ", 100, 10.0, "2024-01-15")]
        out = portfolio.evaluate_portfolio(positions, snapshot_by_ticker={})
        assert out[0]["current_price"] is None
        assert out[0]["urgency"] == 0

    def test_distributions_applied(self):
        positions = [portfolio.Position("PFL", 100, 10.0, "2024-01-15")]
        snapshots = {"PFL": {"price": 10.0, "z1": -0.5, "z3": -0.3}}
        dists = {"PFL": [{"ex_date": "2024-06-01", "tot_div": 0.5}]}
        out = portfolio.evaluate_portfolio(positions, snapshots, dists)
        # 0% price + 5% dist → 5%
        assert out[0]["return"]["total_pct"] == pytest.approx(0.05)

    def test_empty_portfolio(self):
        out = portfolio.evaluate_portfolio([], {})
        assert out == []

    def test_distributions_none_defaults_to_empty(self):
        positions = [portfolio.Position("PFL", 100, 10.0, "2024-01-15")]
        snapshots = {"PFL": {"price": 10.5, "z1": -0.5, "z3": -0.3}}
        out = portfolio.evaluate_portfolio(positions, snapshots, distributions_by_ticker=None)
        # 5% price + 0 dist
        assert out[0]["return"]["total_pct"] == pytest.approx(0.05)


# ---------------------------------------------------------------- default-path branch
class TestDefaultPaths:
    def test_load_positions_uses_config_default(self, tmp_path, monkeypatch):
        from cef_screener import config as cfg
        monkeypatch.setattr(cfg, "positions_path", lambda: tmp_path / "positions.json")
        # No file → empty
        assert portfolio.load_positions() == []

    def test_save_positions_uses_config_default(self, tmp_path, monkeypatch):
        from cef_screener import config as cfg
        monkeypatch.setattr(cfg, "positions_path", lambda: tmp_path / "positions.json")
        portfolio.save_positions([portfolio.Position("X", 1, 1, "2024-01-01")])
        assert (tmp_path / "positions.json").exists()

    def test_add_position_uses_config_default(self, tmp_path, monkeypatch):
        from cef_screener import config as cfg
        monkeypatch.setattr(cfg, "positions_path", lambda: tmp_path / "positions.json")
        out = portfolio.add_position("Y", 1, 1, "2024-01-01")
        assert len(out) == 1

    def test_remove_position_uses_config_default(self, tmp_path, monkeypatch):
        from cef_screener import config as cfg
        monkeypatch.setattr(cfg, "positions_path", lambda: tmp_path / "positions.json")
        portfolio.add_position("Z", 1, 1, "2024-01-01")
        out = portfolio.remove_position("Z")
        assert out == []

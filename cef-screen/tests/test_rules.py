"""Tests for cef_screener.rules — gatekeeper, sell triggers, labels."""
from __future__ import annotations

import pandas as pd
import pytest

from cef_screener import rules, config


# ---------------------------------------------------------------- gatekeeper
class TestGatekeeper:
    def _universe(self, n=40):
        rows = []
        for i in range(n):
            rows.append({
                "ticker": f"T{i:02d}",
                "z1": -2.0 + i * 0.05,  # ascending; lowest = T00
                "market_cap_usdm": 100.0,
                "avg_daily_volume": 50_000,
            })
        return pd.DataFrame(rows)

    def test_empty_input(self):
        out = rules.gatekeeper_top_n(pd.DataFrame())
        assert out.empty

    def test_top_n_sorted_ascending(self):
        df = self._universe(40)
        out = rules.gatekeeper_top_n(df, n=5)
        assert list(out["ticker"]) == ["T00", "T01", "T02", "T03", "T04"]

    def test_filters_illiquid(self):
        df = self._universe(5)
        df.loc[0, "avg_daily_volume"] = 100  # too low
        out = rules.gatekeeper_top_n(df, n=5)
        assert "T00" not in out["ticker"].values

    def test_drops_missing_z1(self):
        df = self._universe(5)
        df.loc[1, "z1"] = None
        out = rules.gatekeeper_top_n(df, n=5)
        assert "T01" not in out["ticker"].values

    def test_passes_liquidity_wrapper(self):
        row = {"market_cap_usdm": 100, "avg_daily_volume": 50_000}
        assert rules.passes_liquidity(row) is True
        bad = {"market_cap_usdm": 1, "avg_daily_volume": 1}
        assert rules.passes_liquidity(bad) is False

    def test_all_illiquid_returns_empty(self):
        df = self._universe(5)
        df["avg_daily_volume"] = 1
        out = rules.gatekeeper_top_n(df)
        assert out.empty

    def test_all_missing_z1_returns_empty(self):
        df = self._universe(5)
        df["z1"] = None
        out = rules.gatekeeper_top_n(df)
        assert out.empty


# ---------------------------------------------------------------- sell triggers
class TestSellTriggers:
    def test_no_triggers_clean_hold(self):
        out = rules.evaluate_sell_triggers(z1=-1.0, z3=-0.5, return_pct=0.05)
        assert out["urgency"] == 0
        assert out["triggers"] == []

    def test_z1_hard_sell_now(self):
        out = rules.evaluate_sell_triggers(z1=2.5, z3=0.0, return_pct=0.0)
        assert out["urgency"] == 3
        assert any("Z1-HARD" in t for t in out["triggers"])

    def test_mean_revert_combo(self):
        out = rules.evaluate_sell_triggers(z1=1.6, z3=1.1, return_pct=0.0)
        assert out["urgency"] == 3
        assert any("MEAN-REVERT" in t for t in out["triggers"])

    def test_mean_revert_z1_alone_no_trigger(self):
        # z1 meets but z3 doesn't confirm → REVIEW (urgency 2)
        out = rules.evaluate_sell_triggers(z1=1.6, z3=0.5, return_pct=0.0)
        assert all("MEAN-REVERT (" not in t for t in out["triggers"])
        assert any("REVIEW" in t for t in out["triggers"])
        assert out["urgency"] == 2

    def test_target_gain_review(self):
        out = rules.evaluate_sell_triggers(z1=-1.0, z3=-0.5, return_pct=0.15)
        assert out["urgency"] == 2
        assert any("TARGET-GAIN" in t for t in out["triggers"])

    def test_stop_loss_sell_now(self):
        out = rules.evaluate_sell_triggers(z1=-1.0, z3=-0.5, return_pct=-0.25)
        assert out["urgency"] == 3
        assert any("STOP-LOSS" in t for t in out["triggers"])

    def test_z1_watch_range(self):
        out = rules.evaluate_sell_triggers(z1=1.2, z3=0.0, return_pct=0.0)
        assert out["urgency"] == 1
        assert any("WATCH" in t for t in out["triggers"])

    def test_none_z_inputs(self):
        out = rules.evaluate_sell_triggers(z1=None, z3=None, return_pct=None)
        assert out["urgency"] == 0

    def test_none_z3_doesnt_break_mean_revert(self):
        # z1 high but z3 missing → MEAN-REVERT full not triggered, REVIEW fires
        out = rules.evaluate_sell_triggers(z1=1.6, z3=None, return_pct=0.0)
        assert all("MEAN-REVERT (" not in t for t in out["triggers"])
        assert any("REVIEW" in t for t in out["triggers"])

    def test_simultaneous_multiple_triggers(self):
        # Z1 hard + stop loss → 2 sell-now triggers, urgency stays 3
        out = rules.evaluate_sell_triggers(z1=2.5, z3=0.0, return_pct=-0.30)
        assert out["urgency"] == 3
        assert len(out["triggers"]) >= 2

    def test_target_and_z1_hard_both_fire(self):
        out = rules.evaluate_sell_triggers(z1=2.5, z3=0.0, return_pct=0.20)
        types = [t.split(":")[0] for t in out["triggers"]]
        assert "SELL" in types and out["urgency"] == 3


# ---------------------------------------------------------------- trap_tier_label
class TestTrapTier:
    def test_confirmed_takes_priority(self):
        assert rules.trap_tier_label(suspect=True, confirmed=True, watch=True) == "CONFIRMED"

    def test_suspect_when_no_confirm(self):
        assert rules.trap_tier_label(suspect=True, confirmed=False, watch=False) == "SUSPECT"

    def test_watch(self):
        assert rules.trap_tier_label(suspect=False, confirmed=False, watch=True) == "WATCH"

    def test_ok(self):
        assert rules.trap_tier_label(suspect=False, confirmed=False, watch=False) == "OK"


# ---------------------------------------------------------------- buy_label
class TestBuyLabel:
    def test_trap_confirmed_overrides(self):
        assert rules.buy_label(95, "CONFIRMED") == "TRAP-CONFIRMED"

    def test_tier_a(self):
        assert rules.buy_label(80, "OK") == "BUY-A"

    def test_tier_a_threshold_boundary(self):
        assert rules.buy_label(config.BUY_TIER_A_MIN, "OK") == "BUY-A"

    def test_tier_b(self):
        assert rules.buy_label(65, "OK") == "BUY-B"

    def test_avoid(self):
        assert rules.buy_label(40, "OK") == "AVOID"

    def test_suspect_overlay(self):
        out = rules.buy_label(80, "SUSPECT")
        assert "BUY-A" in out and "TRAP-SUSPECT" in out

    def test_watch_overlay(self):
        out = rules.buy_label(65, "WATCH")
        assert "BUY-B" in out and "WATCH" in out

    def test_sparse_overlay(self):
        out = rules.buy_label(80, "OK", sparse=True)
        assert "PROVISIONAL" in out

    def test_multiple_overlays(self):
        out = rules.buy_label(65, "SUSPECT", sparse=True)
        assert "TRAP-SUSPECT" in out and "PROVISIONAL" in out

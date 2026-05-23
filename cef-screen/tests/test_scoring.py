"""Tests for cef_screener.scoring — sub-scores, composite, trap classifier."""
from __future__ import annotations

import math

import pytest

from cef_screener import scoring, config


# ---------------------------------------------------------------- helpers
class TestHelpers:
    def test_clip_within(self):
        assert scoring._clip(50) == 50.0

    def test_clip_below(self):
        assert scoring._clip(-10) == 0.0

    def test_clip_above(self):
        assert scoring._clip(120) == 100.0

    def test_clip_custom_bounds(self):
        assert scoring._clip(0.5, lo=0, hi=1) == 0.5
        assert scoring._clip(-1, lo=0, hi=1) == 0
        assert scoring._clip(2, lo=0, hi=1) == 1

    def test_sigmoid_positive(self):
        assert abs(scoring._sigmoid(0) - 0.5) < 1e-9
        assert abs(scoring._sigmoid(2.0) - 1 / (1 + math.exp(-2.0))) < 1e-9

    def test_sigmoid_negative(self):
        assert abs(scoring._sigmoid(-2.0) - math.exp(-2.0) / (1 + math.exp(-2.0))) < 1e-9


# ---------------------------------------------------------------- s_disc
class TestSDisc:
    def test_full_inputs_cheap(self):
        # Plan convention: positive = discount. z1=-2 = very cheap on Z basis,
        # current=+20 = 20 % discount, median=+25 = 25 % structural discount.
        # z_part = 50 - 20·(-2) - 10·(-1) = 100
        # abs_part = 50 + 3·20 = 110 → clipped to 100
        # struct_part = 50 + 2.5·25 = 112.5 → clipped to 100
        out = scoring.s_disc(z1=-2.0, z3=-1.0, current_discount_pct=20,
                             median_disc_5y=25)
        assert out["value"] > 90
        assert not out["sparse"]
        assert out["z_part"] == 100.0
        assert out["abs_part"] == 100.0
        assert out["struct_part"] == 100.0

    def test_z1_only_uses_fallback_weights(self):
        # z3 None → z_part = clip(50 - 30·z1) = 80 for z1=-1
        out = scoring.s_disc(z1=-1.0, z3=None, current_discount_pct=10,
                             median_disc_5y=10)
        assert out["z_part"] == 80.0
        assert out["sparse"]

    def test_no_z_signal_defaults(self):
        out = scoring.s_disc(z1=None, z3=None, current_discount_pct=10,
                             median_disc_5y=10)
        assert out["z_part"] == 50.0
        assert out["sparse"]

    def test_z_positive_caps_abs_and_struct(self):
        # z1>0 and discount large → abs/struct hit cap of 70 instead of full value
        out = scoring.s_disc(z1=0.5, z3=0.5, current_discount_pct=15,
                             median_disc_5y=20)
        # abs_part = 50 + 3·15 = 95, struct = 50 + 2.5·20 = 100; both capped at 70
        without_cap = (
            config.SDISC_Z_WEIGHT * out["z_part"]
            + config.SDISC_ABS_WEIGHT * out["abs_part"]
            + config.SDISC_STRUCT_WEIGHT * out["struct_part"]
        )
        assert out["value"] < without_cap
        assert out["abs_part"] == 95.0
        assert out["struct_part"] == 100.0

    def test_z_positive_no_cap_needed(self):
        # z1>0 but abs/struct already <= 70 → cap has no effect
        out = scoring.s_disc(z1=0.5, z3=0.5, current_discount_pct=5,
                             median_disc_5y=5)
        # abs_part = 50 + 15 = 65, struct = 50 + 12.5 = 62.5 (both <=70)
        expected = (
            config.SDISC_Z_WEIGHT * out["z_part"]
            + config.SDISC_ABS_WEIGHT * out["abs_part"]
            + config.SDISC_STRUCT_WEIGHT * out["struct_part"]
        )
        assert abs(out["value"] - expected) < 0.01

    def test_handles_none_for_optional_inputs(self):
        out = scoring.s_disc(z1=-1.5, z3=-0.5, current_discount_pct=None,
                             median_disc_5y=None)
        assert out["abs_part"] == 50.0
        assert out["struct_part"] == 50.0


# ---------------------------------------------------------------- s_res
class TestSRes:
    def test_no_drawdowns_uses_fallback(self):
        out = scoring.s_res(None, None, 0)
        assert out["sparse"]
        assert out["value"] == 55.0  # 60 - 5 fallback default

    def test_only_one_window(self):
        out = scoring.s_res(15.0, None, 0)
        assert not out["sparse"]
        # dd22=0; dd20=15; worst=15; regime=0.3·15=4.5; input=max(4.5, 12)=12
        assert out["dd_input"] == 12.0

    def test_full_with_leverage(self):
        out = scoring.s_res(dd_2020_pct=20, dd_2022_pct=10, lev_pct=40)
        # dd_input = max(0.7·10 + 0.3·20, 0.8·20) = max(13, 16) = 16
        # lev_mult = min(1 + 0.5·0.4, 1.40) = 1.20
        # s_res = clip(100 - 16·1.20) = 100 - 19.2 = 80.8
        assert abs(out["value"] - 80.8) < 0.1

    def test_extreme_drawdown_clipped_to_zero(self):
        out = scoring.s_res(80, 80, 100)  # massive lev + dd
        assert out["value"] == 0.0


# ---------------------------------------------------------------- s_sust
class TestSSust:
    def test_full_path_all_components(self):
        out = scoring.s_sust(
            roc_pct_12m=0.10, roc_trend=0.05, dist_cuts_5y=0,
            dist_cagr_5y=0.03, coverage=1.1, nav_cagr_3y=0.04,
            unii_ratio=0.2, crisis_maintenance=1.0, lev_pct=20,
            composition_quality="full", distribution_history_years=5.0,
        )
        assert out["value"] > 50
        assert not out["sparse"]
        assert "crisis_maint" in out["active_components"]

    def test_sparse_path_v2_weights(self):
        out = scoring.s_sust(
            roc_pct_12m=None, roc_trend=None, dist_cuts_5y=None,
            dist_cagr_5y=None, coverage=1.2, nav_cagr_3y=0.05,
            unii_ratio=0.3, crisis_maintenance=None, lev_pct=0,
            composition_quality="incomplete", distribution_history_years=None,
        )
        assert out["sparse"]
        assert set(out["active_components"].keys()) <= {"coverage", "nav", "unii"}

    def test_leverage_drag_capped(self):
        out_low = scoring.s_sust(
            roc_pct_12m=0.0, roc_trend=0.0, dist_cuts_5y=0,
            dist_cagr_5y=0.0, coverage=1.0, nav_cagr_3y=0.0,
            unii_ratio=0.0, crisis_maintenance=1.0, lev_pct=0,
            composition_quality="full", distribution_history_years=5,
        )
        out_high = scoring.s_sust(
            roc_pct_12m=0.0, roc_trend=0.0, dist_cuts_5y=0,
            dist_cagr_5y=0.0, coverage=1.0, nav_cagr_3y=0.0,
            unii_ratio=0.0, crisis_maintenance=1.0, lev_pct=80,
            composition_quality="full", distribution_history_years=5,
        )
        assert out_high["lev_drag"] == 0.20  # capped
        assert out_low["lev_drag"] == 0.0

    def test_negative_roc_trend_treated_as_zero(self):
        # roc_trend negative → effective trend = 0 → no extra penalty
        out = scoring.s_sust(
            roc_pct_12m=0.10, roc_trend=-0.05, dist_cuts_5y=0,
            dist_cagr_5y=0.0, coverage=1.0, nav_cagr_3y=0.0,
            unii_ratio=0.0, crisis_maintenance=1.0, lev_pct=0,
            composition_quality="full", distribution_history_years=5,
        )
        # roc_s = clip(100 - 150·0.10 - 50·0) = 85
        assert out["roc_s"] == 85.0

    def test_all_components_none_uses_50(self):
        out = scoring.s_sust(
            roc_pct_12m=None, roc_trend=None, dist_cuts_5y=None,
            dist_cagr_5y=None, coverage=None, nav_cagr_3y=None,
            unii_ratio=None, crisis_maintenance=None, lev_pct=None,
            composition_quality="incomplete", distribution_history_years=None,
        )
        # No active components → w_total = 0 → s_raw = 50; lev_drag from null=29% lev
        assert out["value"] > 0
        assert out["sparse"]

    def test_full_quality_but_no_3y_history_uses_sparse(self):
        out = scoring.s_sust(
            roc_pct_12m=0.1, roc_trend=0.0, dist_cuts_5y=0,
            dist_cagr_5y=0.0, coverage=1.0, nav_cagr_3y=0.0,
            unii_ratio=0.0, crisis_maintenance=None, lev_pct=0,
            composition_quality="full", distribution_history_years=1.5,
        )
        assert out["sparse"]


# ---------------------------------------------------------------- s_peer
class TestSPeer:
    def test_null_percentile_defaults_50(self):
        out = scoring.s_peer(None, None, None)
        assert out["value"] == 50.0
        assert out["sparse"]
        assert not out["penalty"]

    def test_high_percentile_no_penalty(self):
        out = scoring.s_peer(0.80, self_ret_3y=8.0, benchmark_cagr_3y=5.0)
        assert out["value"] == 80.0
        assert not out["penalty"]

    def test_bottom_quartile_underperf_triggers_penalty(self):
        out = scoring.s_peer(0.10, self_ret_3y=-2.0, benchmark_cagr_3y=4.0)
        assert out["penalty"]

    def test_bottom_quartile_but_outperforms_no_penalty(self):
        # Relative-only: bottom in category BUT category outperforms benchmark
        out = scoring.s_peer(0.10, self_ret_3y=6.0, benchmark_cagr_3y=4.0)
        assert not out["penalty"]

    def test_bottom_quartile_with_no_abs_data(self):
        out = scoring.s_peer(0.10, None, None)
        assert not out["penalty"]


# ---------------------------------------------------------------- composite
class TestComposite:
    def test_no_breaches_pure_linear(self):
        # current_discount_pct=+10 (plan: 10% discount) → sev_prem = (-2 - 10)/5 = -2.4 → 0
        out = scoring.composite(80, 80, 80, 80, z1=-1.0, current_discount_pct=10,
                                peer_penalty_gate=False)
        assert out["multiplier"] == 1.0
        assert out["composite"] == 80.0
        assert out["total_severity"] == 0.0

    def test_one_severe_breach(self):
        out = scoring.composite(80, 80, 80, 80, z1=3.0, current_discount_pct=10,
                                peer_penalty_gate=False)
        assert abs(out["multiplier"] - 0.5625) < 0.001
        assert out["severity_disc"] == 2.0
        assert out["composite"] == round(80 * 0.5625, 1)

    def test_multiple_breaches_compounded(self):
        out = scoring.composite(50, 20, 10, 0, z1=2.0, current_discount_pct=10,
                                peer_penalty_gate=True)
        assert out["total_severity"] > 5.0
        assert out["multiplier"] < 0.3

    def test_premium_territory_severity(self):
        # current_discount_pct=-8 (8% premium in plan convention) → sev_prem = (-2-(-8))/5 = 1.2
        out = scoring.composite(50, 50, 50, 50, z1=-0.5, current_discount_pct=-8,
                                peer_penalty_gate=False)
        assert abs(out["severity_disc"] - 1.2) < 0.001

    def test_premium_at_boundary(self):
        # current_discount_pct=-2 (2% premium, at boundary) → sev_prem = 0
        out = scoring.composite(50, 50, 50, 50, z1=-1.0, current_discount_pct=-2,
                                peer_penalty_gate=False)
        assert out["severity_disc"] == 0.0

    def test_extreme_premium_capped(self):
        # current=-15 → sev_prem = (-2-(-15))/5 = 2.6 → clipped to 2.0
        out = scoring.composite(50, 50, 50, 50, z1=-1.0, current_discount_pct=-15,
                                peer_penalty_gate=False)
        assert out["severity_disc"] == 2.0

    def test_handles_none_inputs(self):
        out = scoring.composite(50, 50, 50, 50, z1=None, current_discount_pct=None,
                                peer_penalty_gate=False)
        assert out["severity_disc"] == 0.0
        assert out["composite"] == 50.0

    def test_peer_penalty_gate_false_zero_severity(self):
        # peer is low but gate=False → severity_peer = 0
        out = scoring.composite(80, 80, 80, 0, z1=-1.0, current_discount_pct=10,
                                peer_penalty_gate=False)
        assert out["severity_peer"] == 0.0


# ---------------------------------------------------------------- trap_classification
class TestTrap:
    def test_short_history_returns_watch(self):
        out = scoring.trap_classification(
            roc_pct_12m=0.6, roc_trend=0.2, nav_cagr_3y=-0.05, coverage=0.5,
            unii_per_share=-0.1, distribution_rate_on_nav=0.10,
            nav_total_return_3y=-0.04, benchmark_cagr_3y=0.05,
            composition_quality="full", distribution_history_years=1.5,
        )
        assert out["watch"]
        assert not out["confirmed"]
        assert not out["suspect"]

    def test_incomplete_composition_returns_clean(self):
        out = scoring.trap_classification(
            roc_pct_12m=None, roc_trend=None, nav_cagr_3y=None, coverage=None,
            unii_per_share=None, distribution_rate_on_nav=None,
            nav_total_return_3y=None, benchmark_cagr_3y=None,
            composition_quality="incomplete", distribution_history_years=5,
        )
        assert not out["suspect"] and not out["confirmed"]

    def test_destructive_roc_outright(self):
        out = scoring.trap_classification(
            roc_pct_12m=0.50, roc_trend=0.0, nav_cagr_3y=-0.03, coverage=1.0,
            unii_per_share=0.0, distribution_rate_on_nav=0.10,
            nav_total_return_3y=-0.02, benchmark_cagr_3y=0.05,
            composition_quality="full", distribution_history_years=5,
        )
        assert out["suspect"]

    def test_roc_trend_rising_fast(self):
        out = scoring.trap_classification(
            roc_pct_12m=0.30, roc_trend=0.25, nav_cagr_3y=0.02, coverage=1.0,
            unii_per_share=0.0, distribution_rate_on_nav=0.08,
            nav_total_return_3y=0.05, benchmark_cagr_3y=0.04,
            composition_quality="full", distribution_history_years=5,
        )
        assert out["suspect"]

    def test_coverage_and_unii_combo(self):
        out = scoring.trap_classification(
            roc_pct_12m=0.10, roc_trend=0.0, nav_cagr_3y=0.02, coverage=0.6,
            unii_per_share=-0.05, distribution_rate_on_nav=0.08,
            nav_total_return_3y=0.05, benchmark_cagr_3y=0.04,
            composition_quality="full", distribution_history_years=5,
        )
        assert out["suspect"]

    def test_excess_payout_vs_benchmark(self):
        out = scoring.trap_classification(
            roc_pct_12m=0.20, roc_trend=0.05, nav_cagr_3y=-0.01, coverage=1.0,
            unii_per_share=0.0, distribution_rate_on_nav=0.12,
            nav_total_return_3y=0.0, benchmark_cagr_3y=0.05,
            composition_quality="full", distribution_history_years=5,
        )
        # nav_cagr<0 AND benchmark>0 AND excess_payout = 0.12 - 0.0 = 0.12 > max(0.02, 0.03)
        assert out["suspect"]

    def test_confirmed_trap(self):
        out = scoring.trap_classification(
            roc_pct_12m=0.65, roc_trend=0.10, nav_cagr_3y=-0.04, coverage=0.7,
            unii_per_share=-0.05, distribution_rate_on_nav=0.10,
            nav_total_return_3y=-0.03, benchmark_cagr_3y=0.05,
            composition_quality="full", distribution_history_years=5,
        )
        assert out["confirmed"]

    def test_clean_fund(self):
        out = scoring.trap_classification(
            roc_pct_12m=0.10, roc_trend=0.0, nav_cagr_3y=0.06, coverage=1.2,
            unii_per_share=0.05, distribution_rate_on_nav=0.06,
            nav_total_return_3y=0.06, benchmark_cagr_3y=0.05,
            composition_quality="full", distribution_history_years=5,
        )
        assert not out["suspect"] and not out["confirmed"]
        assert out["reason"] == "OK"


# ---------------------------------------------------------------- _lev_frac
def test_lev_frac_none_uses_median():
    assert abs(scoring._lev_frac(None) - 0.29) < 1e-6


def test_lev_frac_value():
    assert scoring._lev_frac(35) == 0.35

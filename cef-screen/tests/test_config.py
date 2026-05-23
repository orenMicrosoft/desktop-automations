"""Tests for cef_screener.config — pure functions and path helpers."""
from __future__ import annotations

import importlib
import os
from pathlib import Path

import pytest

from cef_screener import config


# ---------------------------------------------------------------- paths
def test_cache_dir_uses_env_override(tmp_path, monkeypatch):
    target = tmp_path / "my_cache"
    monkeypatch.setenv("CEF_SCREENER_CACHE_DIR", str(target))
    p = config.cache_dir()
    assert p == target
    assert p.is_dir()


def test_cache_dir_windows_default(tmp_path, monkeypatch):
    monkeypatch.delenv("CEF_SCREENER_CACHE_DIR", raising=False)
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path))
    monkeypatch.setattr(config, "_OS_NAME", "nt")
    p = config.cache_dir()
    assert p == tmp_path / "cef_screener"


def test_cache_dir_windows_fallback_when_localappdata_missing(tmp_path, monkeypatch):
    monkeypatch.delenv("CEF_SCREENER_CACHE_DIR", raising=False)
    monkeypatch.delenv("LOCALAPPDATA", raising=False)
    monkeypatch.setattr(config, "_OS_NAME", "nt")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    p = config.cache_dir()
    assert p == tmp_path / "AppData" / "Local" / "cef_screener"


def test_cache_dir_posix_xdg(tmp_path, monkeypatch):
    monkeypatch.delenv("CEF_SCREENER_CACHE_DIR", raising=False)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path))
    monkeypatch.setattr(config, "_OS_NAME", "posix")
    p = config.cache_dir()
    assert p == tmp_path / "cef_screener"


def test_cache_dir_posix_fallback_when_xdg_missing(tmp_path, monkeypatch):
    monkeypatch.delenv("CEF_SCREENER_CACHE_DIR", raising=False)
    monkeypatch.delenv("XDG_DATA_HOME", raising=False)
    monkeypatch.setattr(config, "_OS_NAME", "posix")
    monkeypatch.setattr(Path, "home", staticmethod(lambda: tmp_path))
    p = config.cache_dir()
    assert p == tmp_path / ".local" / "share" / "cef_screener"


def test_all_path_helpers_live_inside_cache_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("CEF_SCREENER_CACHE_DIR", str(tmp_path))
    base = config.cache_dir()
    assert config.cache_db_path() == base / "cache.sqlite"
    assert config.positions_path() == base / "positions.json"
    assert config.sell_log_path() == base / "sell_log.csv"
    assert config.exclusions_path() == base / "exclusions.yaml"
    assert config.lock_path() == base / "run.lock"


# ---------------------------------------------------------------- leverage
@pytest.mark.parametrize("pct,expected", [
    (None, "Unknown"),
    (0, "None"),
    (0.5, "None"),
    (0.51, "Low"),
    (15, "Low"),
    (15.01, "Moderate"),
    (30, "Moderate"),
    (30.01, "High"),
    (45, "High"),
    (45.01, "Extreme"),
    (80, "Extreme"),
])
def test_leverage_tier(pct, expected):
    assert config.leverage_tier(pct) == expected


# ---------------------------------------------------------------- benchmarks
@pytest.mark.parametrize("category,expected", [
    ("Covered Call", "XYLD"),
    ("MLP - Energy", "MLPX"),
    ("Energy Infrastructure", "MLPX"),
    ("Real Estate", "VNQ"),
    ("REIT - US", "VNQ"),
    ("Utility Sector", "XLU"),
    ("Infrastructure", "XLU"),
    ("Convertible", "CWB"),
    ("BDC", "BIZD"),
    ("Business Development Company", "BIZD"),
    ("Small Cap Equity", "IJR"),
    ("Mid Cap Growth", "IJH"),
    ("US Equity", "SPY"),
    ("Large Cap Value", "SPY"),
    ("Core Equity", "SPY"),
    ("Global Equity", "ACWI"),
    ("International Equity", "ACWI"),
    ("World Allocation", "ACWI"),
    ("High Yield Bond", "HYG"),
    ("Senior Loan", "BKLN"),
    ("Floating Rate", "BKLN"),
    ("Bank Loan", "BKLN"),
    ("National Muni", "MUB"),
    ("Single-State Muni", "MUB"),
    ("Muni Bond", "MUB"),
    ("Municipal", "MUB"),
    ("Preferred Securities", "PFF"),
    ("Emerging Markets", "EMB"),
    ("EM Debt", "EMB"),
    ("Emerging Bond", "EMB"),
    ("Taxable Bond", "AGG"),
    ("Multi-Sector Income", "AGG"),
    ("Investment Grade", "AGG"),
])
def test_benchmark_for_known_categories(category, expected):
    assert config.benchmark_for(category) == expected


def test_benchmark_for_none_or_empty():
    assert config.benchmark_for(None) == "SPY"
    assert config.benchmark_for("") == "SPY"


def test_benchmark_for_bond_word_fallback():
    # Doesn't match any explicit needle but contains 'credit' → AGG fallback
    assert config.benchmark_for("Misc Credit Strategies") == "AGG"
    assert config.benchmark_for("Specialty income fund") == "AGG"


def test_benchmark_for_unknown_equity_fallback():
    assert config.benchmark_for("Quantum Sector Rotation") == "SPY"


# ---------------------------------------------------------------- fixed income
@pytest.mark.parametrize("category,expected", [
    ("Taxable Bond", True),
    ("High Yield", True),
    ("Senior Loan", True),
    ("National Muni", True),
    ("Preferred Securities", True),
    ("EM Debt", True),
    ("Multi-Sector Income", True),
    ("Convertible", True),
    ("US Equity", False),
    ("Real Estate", False),
    ("Covered Call", False),
    ("", False),
    (None, False),
])
def test_is_fixed_income(category, expected):
    assert config.is_fixed_income(category) is expected


# ---------------------------------------------------------------- overrides
class TestOverrides:
    def setup_method(self):
        config.reset_overrides()

    def teardown_method(self):
        config.reset_overrides()

    def test_overrides_path(self):
        p = config.overrides_path()
        assert p.name == "config_overrides.json"
        assert p.parent == config.cache_dir()

    def test_load_missing_returns_empty(self):
        # Ensure file doesn't exist
        p = config.overrides_path()
        if p.exists():
            p.unlink()
        assert config.load_overrides() == {}

    def test_save_and_load_roundtrip(self):
        out = config.save_overrides({"PENALTY_BASE": 0.5})
        assert out["PENALTY_BASE"] == 0.5
        loaded = config.load_overrides()
        assert loaded["PENALTY_BASE"] == 0.5
        assert config.PENALTY_BASE == 0.5

    def test_save_rejects_invalid_value(self):
        # PENALTY_BASE must be 0 < x <= 1; -1 is invalid
        out = config.save_overrides({"PENALTY_BASE": -1.0})
        assert "PENALTY_BASE" not in out
        assert config.PENALTY_BASE == config._DEFAULTS["PENALTY_BASE"]

    def test_save_rejects_unknown_key(self):
        out = config.save_overrides({"UNKNOWN_THING": 42})
        assert "UNKNOWN_THING" not in out

    def test_save_rejects_uncastable(self):
        out = config.save_overrides({"PENALTY_BASE": "not-a-number"})
        assert "PENALTY_BASE" not in out

    def test_weights_validation(self):
        out = config.save_overrides({"COMPOSITE_FACTOR_WEIGHTS": {
            "s_disc": 0.4, "s_res": 0.2, "s_sust": 0.2, "s_peer": 0.2,
        }})
        assert "COMPOSITE_FACTOR_WEIGHTS" in out
        assert config.COMPOSITE_FACTOR_WEIGHTS["s_disc"] == 0.4

    def test_weights_rejects_missing_key(self):
        out = config.save_overrides({"COMPOSITE_FACTOR_WEIGHTS": {
            "s_disc": 0.5, "s_res": 0.5,
        }})
        assert "COMPOSITE_FACTOR_WEIGHTS" not in out

    def test_weights_rejects_zero_sum(self):
        out = config.save_overrides({"COMPOSITE_FACTOR_WEIGHTS": {
            "s_disc": 0, "s_res": 0, "s_sust": 0, "s_peer": 0,
        }})
        assert "COMPOSITE_FACTOR_WEIGHTS" not in out

    def test_weights_rejects_non_dict(self):
        out = config.save_overrides({"COMPOSITE_FACTOR_WEIGHTS": "foo"})
        assert "COMPOSITE_FACTOR_WEIGHTS" not in out

    def test_weights_rejects_non_numeric(self):
        out = config.save_overrides({"COMPOSITE_FACTOR_WEIGHTS": {
            "s_disc": "x", "s_res": 0.3, "s_sust": 0.3, "s_peer": 0.3,
        }})
        assert "COMPOSITE_FACTOR_WEIGHTS" not in out

    def test_int_gatekeeper_size(self):
        out = config.save_overrides({"GATEKEEPER_SIZE": 50})
        assert out["GATEKEEPER_SIZE"] == 50
        assert config.GATEKEEPER_SIZE == 50

    def test_negative_stop_loss(self):
        out = config.save_overrides({"SELL_STOP_LOSS_PCT": -0.30})
        assert out["SELL_STOP_LOSS_PCT"] == -0.30
        # positive value rejected
        out2 = config.save_overrides({"SELL_STOP_LOSS_PCT": 0.10})
        assert "SELL_STOP_LOSS_PCT" not in {
            k: v for k, v in out2.items() if v == 0.10
        }
        assert config.SELL_STOP_LOSS_PCT == -0.30

    def test_reset_restores_defaults(self):
        config.save_overrides({"PENALTY_BASE": 0.5})
        assert config.PENALTY_BASE == 0.5
        config.reset_overrides()
        assert config.PENALTY_BASE == config._DEFAULTS["PENALTY_BASE"]
        assert not config.overrides_path().exists()

    def test_reset_when_no_file(self):
        # Reset when no file exists must not raise
        p = config.overrides_path()
        if p.exists():
            p.unlink()
        config.reset_overrides()    # should be no-op

    def test_load_corrupt_json(self):
        p = config.overrides_path()
        p.write_text("{not valid json", encoding="utf-8")
        assert config.load_overrides() == {}

    def test_load_non_dict_json(self):
        p = config.overrides_path()
        p.write_text("[1, 2, 3]", encoding="utf-8")
        assert config.load_overrides() == {}

    def test_load_silently_drops_bad_entries(self):
        p = config.overrides_path()
        p.write_text(_dumps({
            "PENALTY_BASE": "not-a-number",
            "BUY_TIER_A_MIN": 80.0,
            "UNKNOWN_KEY": 99,
        }), encoding="utf-8")
        out = config.load_overrides()
        assert out == {"BUY_TIER_A_MIN": 80.0}

    def test_effective_settings_has_all_keys(self):
        eff = config.effective_settings()
        assert set(eff.keys()) == set(config.OVERRIDABLE.keys())

    def test_save_merges_with_existing(self):
        config.save_overrides({"PENALTY_BASE": 0.6})
        config.save_overrides({"BUY_TIER_A_MIN": 70.0})
        loaded = config.load_overrides()
        assert loaded["PENALTY_BASE"] == 0.6
        assert loaded["BUY_TIER_A_MIN"] == 70.0

    def test_load_drops_values_failing_validator(self):
        # PENALTY_BASE casts to float fine but validator rejects 2.0 (> 1)
        p = config.overrides_path()
        p.write_text(_dumps({"PENALTY_BASE": 2.0}), encoding="utf-8")
        assert config.load_overrides() == {}

    def test_default_value_helper(self):
        # Internal helper that's part of the public API for tests
        assert config._default_value("PENALTY_BASE") == config._DEFAULTS["PENALTY_BASE"]

    def test_validators_happy_path(self):
        # Hit the positive branches of each validator
        assert config._v_positive(1.0)
        assert not config._v_positive(0.0)
        assert not config._v_positive(float("nan"))
        assert not config._v_positive(float("inf"))
        assert config._v_nonneg(0.0)
        assert not config._v_nonneg(-1.0)
        assert not config._v_nonneg(float("nan"))
        assert config._v_negative(-1.0)
        assert not config._v_negative(0.0)
        assert config._v_0_to_1(0.5)
        assert config._v_0_to_1(1.0)
        assert not config._v_0_to_1(0.0)
        assert not config._v_0_to_1(1.5)
        assert config._v_pos_int(5)
        assert not config._v_pos_int(0)
        assert not config._v_pos_int(1.5)
        assert not config._v_pos_int("x")


def _dumps(d):
    import json
    return json.dumps(d)

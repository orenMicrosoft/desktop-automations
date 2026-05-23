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

"""Tests for cef_screener.cli — argparse subcommands."""
from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from cef_screener import cli, engine, portfolio


# ---------------------------------------------------------------- helpers
def _make_run_result(*, scored=True, holdings=False, warnings=None):
    if scored:
        sc = pd.DataFrame([{
            "ticker": "T00", "name": "Fund 0",
            "category_name": "Taxable Bond",
            "current_discount_pct": 8.5,
            "median_disc_5y": 7.0,
            "z1": -1.8, "nav_cagr_3y": 0.05,
            "nav_total_return_3y": 0.06,
            "distribution_rate_on_nav": 0.08,
            "coverage": 1.1, "composite": 78.5,
            "multiplier": 0.95,
            "s_disc": 80, "s_res": 75, "s_sust": 70, "s_peer": 65,
            "trap_tier": "—", "trap_reason": None,
            "buy_label": "A",
        }])
    else:
        sc = pd.DataFrame()
    hs = []
    if holdings:
        hs = [{
            "position": {"ticker": "T00", "shares": 100,
                         "cost_basis": 9.0, "purchase_date": "2025-01-01"},
            "return": {"price_pct": 0.111, "dist_pct": 0.05,
                       "total_pct": 0.161},
            "sell": {"action": "HOLD", "reason": "Within bands", "urgency": 0},
        }]
    return engine.RunResult(
        snapshot_date="2026-05-22", snapshot_age_hours=2.0,
        universe_size=35, liquid_universe_size=35,
        gatekeeper=pd.DataFrame(), scored=sc,
        holdings=hs, warnings=warnings or [],
    )


# ---------------------------------------------------------------- _fmt
class TestFmt:
    def test_none(self):
        assert cli._fmt(None) == "—"

    def test_nan(self):
        assert cli._fmt(float("nan")) == "—"

    def test_number(self):
        assert cli._fmt(3.14159) == "3.14"

    def test_number_digits(self):
        assert cli._fmt(3.14159, digits=4) == "3.1416"

    def test_garbage(self):
        # Non-castable falls back to str()
        assert cli._fmt("xyz") == "xyz"


# ---------------------------------------------------------------- _print_table
class TestPrintTable:
    def test_empty(self, capsys):
        cli._print_table([], ["a", "b"])
        out = capsys.readouterr().out
        assert "(no rows)" in out

    def test_basic(self, capsys):
        cli._print_table(
            [{"a": "foo", "b": "bar"}, {"a": "xx", "b": "yyy"}],
            ["a", "b"],
        )
        out = capsys.readouterr().out
        assert "a" in out and "b" in out
        assert "foo" in out and "yyy" in out


# ---------------------------------------------------------------- subcommands
class TestBuy:
    def test_empty(self, capsys):
        r = _make_run_result(scored=False)
        with patch.object(cli.engine, "run_pipeline", return_value=r):
            rc = cli.main(["buy"])
        assert rc == 1
        assert "Universe empty" in capsys.readouterr().out

    def test_with_results(self, capsys):
        r = _make_run_result(warnings=["Snapshot is 30h old"])
        with patch.object(cli.engine, "run_pipeline", return_value=r):
            rc = cli.main(["buy"])
        out = capsys.readouterr().out
        assert rc == 0
        assert "T00" in out
        assert "Snapshot" in out
        assert "30h old" in out


class TestSell:
    def test_no_holdings(self, capsys):
        r = _make_run_result()
        with patch.object(cli.engine, "run_pipeline", return_value=r):
            rc = cli.main(["sell"])
        assert rc == 0
        assert "No holdings" in capsys.readouterr().out

    def test_with_holdings(self, capsys):
        r = _make_run_result(holdings=True)
        with patch.object(cli.engine, "run_pipeline", return_value=r):
            rc = cli.main(["sell"])
        out = capsys.readouterr().out
        assert rc == 0
        assert "T00" in out
        assert "HOLD" in out


class TestInspect:
    def test_empty(self, capsys):
        r = _make_run_result(scored=False)
        with patch.object(cli.engine, "run_pipeline", return_value=r):
            rc = cli.main(["inspect", "T00"])
        assert rc == 1
        assert "No scored data" in capsys.readouterr().out

    def test_not_found(self, capsys):
        r = _make_run_result()
        with patch.object(cli.engine, "run_pipeline", return_value=r):
            rc = cli.main(["inspect", "XXX"])
        assert rc == 1
        assert "not found" in capsys.readouterr().out

    def test_found(self, capsys):
        r = _make_run_result()
        with patch.object(cli.engine, "run_pipeline", return_value=r):
            rc = cli.main(["inspect", "t00"])
        out = capsys.readouterr().out
        assert rc == 0
        assert "Fund 0" in out
        assert "Composite" in out


class TestRefresh:
    def test_no_tickers(self, capsys):
        with patch.object(cli.engine, "refresh_universe",
                          return_value={"universe": 100}) as mock:
            rc = cli.main(["refresh"])
        assert rc == 0
        mock.assert_called_once_with(tickers=None)
        assert "100" in capsys.readouterr().out

    def test_with_tickers(self, capsys):
        with patch.object(cli.engine, "refresh_universe",
                          return_value={"universe": 1}) as mock:
            rc = cli.main(["refresh", "--tickers", "PFL", "T00"])
        assert rc == 0
        mock.assert_called_once_with(tickers=["PFL", "T00"])


class TestPosition:
    def test_add(self, capsys, tmp_path, monkeypatch):
        p = tmp_path / "positions.json"
        monkeypatch.setattr(cli.portfolio.config, "positions_path", lambda: p)
        rc = cli.main(["position", "add", "T00", "100", "9.0", "2025-01-01"])
        assert rc == 0
        positions = portfolio.load_positions()
        assert positions[0].ticker == "T00"
        assert "Added position" in capsys.readouterr().out

    def test_list_empty(self, capsys, tmp_path, monkeypatch):
        p = tmp_path / "positions.json"
        monkeypatch.setattr(cli.portfolio.config, "positions_path", lambda: p)
        rc = cli.main(["position", "list"])
        assert rc == 0
        assert "(no positions)" in capsys.readouterr().out

    def test_list_with_positions(self, capsys, tmp_path, monkeypatch):
        p = tmp_path / "positions.json"
        monkeypatch.setattr(cli.portfolio.config, "positions_path", lambda: p)
        portfolio.add_position("T00", 100, 9.0, "2025-01-01")
        rc = cli.main(["position", "list"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "T00" in out
        assert "9.00" in out

    def test_remove(self, capsys, tmp_path, monkeypatch):
        p = tmp_path / "positions.json"
        monkeypatch.setattr(cli.portfolio.config, "positions_path", lambda: p)
        portfolio.add_position("T00", 100, 9.0, "2025-01-01")
        rc = cli.main(["position", "remove", "T00"])
        assert rc == 0
        assert "Removed T00" in capsys.readouterr().out
        assert portfolio.load_positions() == []


class TestDiagnose:
    def test_all_ok(self, capsys):
        with patch.object(cli.ingest, "diagnose",
                          return_value={"universe": True, "price": True,
                                        "discount": True, "distribution": True}):
            rc = cli.main(["diagnose"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "universe" in out

    def test_failure(self, capsys):
        with patch.object(cli.ingest, "diagnose",
                          return_value={"universe": False}):
            rc = cli.main(["diagnose", "PFL"])
        assert rc == 1


class TestServe:
    def test_delegates_to_web(self):
        with patch("cef_screener.web.main", return_value=0) as mock:
            rc = cli.main(["serve", "--no-browser", "--port", "9000",
                           "--host", "0.0.0.0"])
        assert rc == 0
        mock.assert_called_once_with(
            ["--host", "0.0.0.0", "--port", "9000", "--no-browser"]
        )

    def test_default_args(self):
        with patch("cef_screener.web.main", return_value=0) as mock:
            cli.main(["serve"])
        # Default port 8100, host 127.0.0.1, no --no-browser flag
        mock.assert_called_once_with(["--host", "127.0.0.1", "--port", "8100"])


class TestMainArgvFallback:
    def test_uses_sys_argv_when_none(self, monkeypatch, capsys):
        monkeypatch.setattr("sys.argv", ["cef-screen", "buy"])
        r = _make_run_result(scored=False)
        with patch.object(cli.engine, "run_pipeline", return_value=r):
            rc = cli.main()    # argv=None → reads from sys.argv
        assert rc == 1
        assert "Universe empty" in capsys.readouterr().out

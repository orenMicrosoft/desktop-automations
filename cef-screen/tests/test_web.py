"""Tests for cef_screener.web — Flask dashboard."""
from __future__ import annotations

import time
from unittest.mock import patch

import pandas as pd
import pytest

from cef_screener import web, engine, config


# ---------------------------------------------------------------- fixtures
@pytest.fixture(autouse=True)
def _reset_cache():
    """Ensure each test starts with a fresh result cache."""
    web._CACHE.clear()
    yield
    web._CACHE.clear()


def _make_run_result(*, with_scored=True, with_holdings=False, warnings=None):
    if with_scored:
        scored = pd.DataFrame([{
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
        }, {
            "ticker": "T01", "name": "Fund 1",
            "category_name": "Municipal Bond",
            "current_discount_pct": 4.0,
            "median_disc_5y": 5.0,
            "z1": -0.5, "nav_cagr_3y": 0.03,
            "nav_total_return_3y": 0.04,
            "distribution_rate_on_nav": 0.06,
            "coverage": 0.9, "composite": 55.0,
            "multiplier": 0.7,
            "s_disc": 50, "s_res": 60, "s_sust": 55, "s_peer": 50,
            "trap_tier": "Suspect", "trap_reason": "ROC > 50%",
            "buy_label": "PASS",
        }])
    else:
        scored = pd.DataFrame()
    holdings = []
    if with_holdings:
        holdings = [{
            "position": {"ticker": "T00", "shares": 100,
                         "cost_basis": 9.0, "purchase_date": "2025-01-01"},
            "return": {"price_pct": 0.111, "dist_pct": 0.05,
                       "total_pct": 0.161},
            "sell": {"action": "HOLD", "reason": "Within bands", "urgency": 0},
        }, {
            "position": {"ticker": "T01", "shares": 50,
                         "cost_basis": 12.0, "purchase_date": "2024-01-01"},
            "return": {"price_pct": 0.20, "dist_pct": 0.04,
                       "total_pct": 0.24},
            "sell": {"action": "SELL", "reason": "Target gain hit", "urgency": 3},
        }]
    return engine.RunResult(
        snapshot_date="2026-05-22",
        snapshot_age_hours=2.0,
        universe_size=35,
        liquid_universe_size=35,
        gatekeeper=pd.DataFrame(),
        scored=scored,
        holdings=holdings,
        warnings=warnings or [],
    )


@pytest.fixture
def client():
    app = web.create_app()
    app.config["TESTING"] = True
    return app.test_client()


# ---------------------------------------------------------------- _ResultCache
class TestResultCache:
    def test_get_when_empty(self):
        c = web._ResultCache()
        assert c.get() is None

    def test_set_and_get(self):
        c = web._ResultCache()
        r = _make_run_result()
        c.set(r)
        assert c.get() is r

    def test_get_expired(self):
        c = web._ResultCache()
        c.set(_make_run_result())
        c._ts = time.time() - 99999    # force stale
        assert c.get() is None

    def test_clear(self):
        c = web._ResultCache()
        c.set(_make_run_result())
        c.clear()
        assert c.get() is None


# ---------------------------------------------------------------- _get_result
class TestGetResult:
    def test_first_call_runs_pipeline(self):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r) as mock:
            out = web._get_result()
            assert out is r
            mock.assert_called_once()

    def test_second_call_uses_cache(self):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r) as mock:
            web._get_result()
            web._get_result()
            assert mock.call_count == 1


# ---------------------------------------------------------------- _format_pct
class TestFormatPct:
    def test_none(self):
        assert web._format_pct(None) == "—"

    def test_nan(self):
        assert web._format_pct(float("nan")) == "—"

    def test_garbage_string(self):
        assert web._format_pct("abc") == "—"

    def test_valid_default(self):
        assert web._format_pct(3.14159) == "3.14"

    def test_valid_custom_digits(self):
        assert web._format_pct(3.14159, digits=4) == "3.1416"

    def test_int_input(self):
        assert web._format_pct(5) == "5.00"


# ---------------------------------------------------------------- BUY route
class TestBuyRoute:
    def test_empty_scored(self, client):
        r = _make_run_result(with_scored=False)
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/")
            assert resp.status_code == 200
            assert b"No results" in resp.data
            assert b"CEF Screener" in resp.data

    def test_with_scored(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/")
            assert resp.status_code == 200
            assert b"T00" in resp.data
            assert b"T01" in resp.data
            assert b"Suspect" in resp.data
            assert b"Fund 0" in resp.data

    def test_with_warnings(self, client):
        r = _make_run_result(warnings=["Snapshot is 48h old"])
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/")
            assert b"Snapshot is 48h old" in resp.data


# ---------------------------------------------------------------- SELL route
class TestSellRoute:
    def test_no_holdings(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/sell")
            assert resp.status_code == 200
            assert b"No holdings" in resp.data

    def test_with_holdings(self, client):
        r = _make_run_result(with_holdings=True, warnings=["w1"])
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/sell")
            assert resp.status_code == 200
            assert b"T00" in resp.data
            assert b"SELL" in resp.data
            assert b"Target gain hit" in resp.data
            assert b"HOLD" in resp.data
            assert b"w1" in resp.data


# ---------------------------------------------------------------- CONFIG route
class TestConfigRoute:
    def test_config_page(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/config")
            assert resp.status_code == 200
            assert b"Snapshot date" in resp.data
            assert b"Gatekeeper size" in resp.data
            assert b"Penalty base" in resp.data
            assert str(config.SELL_Z1_HARD).encode() in resp.data


# ---------------------------------------------------------------- INSPECT route
class TestInspectRoute:
    def test_not_in_scored(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/inspect/ZZZZ")
            assert resp.status_code == 200
            assert b"not in current scored set" in resp.data

    def test_found(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/inspect/T00")
            assert resp.status_code == 200
            assert b"T00" in resp.data
            assert b"Taxable Bond" in resp.data
            assert b"Composite" in resp.data

    def test_case_insensitive(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/inspect/t00")
            assert resp.status_code == 200
            assert b"T00" in resp.data

    def test_empty_scored(self, client):
        r = _make_run_result(with_scored=False)
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/inspect/T00")
            assert resp.status_code == 200
            assert b"No scored data" in resp.data


# ---------------------------------------------------------------- /api/health
class TestHealth:
    def test_health(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert resp.get_json() == {"ok": True}


# ---------------------------------------------------------------- /api/refresh
class TestRefresh:
    def test_refresh_success(self, client):
        with patch.object(web.engine, "refresh_universe",
                          return_value={"universe": 35}) as mock:
            resp = client.post("/api/refresh")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["ok"] is True
            assert data["summary"]["universe"] == 35
            mock.assert_called_once()


# ---------------------------------------------------------------- create_app
class TestCreateApp:
    def test_returns_flask_app(self):
        app = web.create_app()
        assert app is not None
        assert "buy" in [r.endpoint for r in app.url_map.iter_rules()]


# ---------------------------------------------------------------- main()
class TestMain:
    def test_main_invokes_flask_run(self):
        called = {}

        def fake_run(host, port, debug):
            called["host"] = host
            called["port"] = port

        def fake_open(url):    # pragma: no cover
            called["opened"] = url

        with patch("cef_screener.web.webbrowser.open", side_effect=fake_open), \
             patch("cef_screener.web.Flask.run", side_effect=fake_run):
            rc = web.main(["--no-browser", "--port", "9999", "--host", "0.0.0.0"])
            assert rc == 0
            assert called["host"] == "0.0.0.0"
            assert called["port"] == 9999
            assert "opened" not in called   # --no-browser

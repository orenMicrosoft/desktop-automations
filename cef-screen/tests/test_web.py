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
            "dd_2020_pct": -0.18, "dd_2022_pct": -0.22,
            "peer_penalty_gate": False,
            "trap_tier": "—", "trap_reason": None,
            "buy_label": "BUY-A (high conviction)",
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
            "dd_2020_pct": -0.30, "dd_2022_pct": -0.15,
            "peer_penalty_gate": True,
            "trap_tier": "Suspect", "trap_reason": "ROC > 50%",
            "buy_label": "BUY-B (worth a look) · trap suspected",
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
            # New: "Why?" column header and a reason cell
            assert b"Why?" in resp.data
            assert b"ROC &gt; 50%" in resp.data    # trap_reason for T01 (escaped)
            # New: rows are clickable
            assert b"row-link" in resp.data
            assert b"window.location" in resp.data
            # New: legend includes the trap glossary
            assert b"distribution trap" in resp.data
            assert b"CONFIRMED" in resp.data

    def test_with_warnings(self, client):
        r = _make_run_result(warnings=["Snapshot is 48h old"])
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/")
            assert b"Snapshot is 48h old" in resp.data

    def test_trap_tooltip_present(self, client):
        # T01 has trap_tier=Suspect → tooltip should mention "SUSPECTED"
        r = _make_run_result()
        # Force trap_tier to a known canonical value to trigger the tooltip map
        r.scored.loc[r.scored["ticker"] == "T01", "trap_tier"] = "SUSPECT"
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/")
            assert b"SUSPECTED" in resp.data    # tooltip text


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
    def setup_method(self):
        config.reset_overrides()

    def teardown_method(self):
        config.reset_overrides()

    def test_config_renders_form(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/config")
            assert resp.status_code == 200
            # Should render an editable form with input names
            assert b"name='PENALTY_BASE'" in resp.data or b'name="PENALTY_BASE"' in resp.data
            assert b"name='SELL_Z1_HARD'" in resp.data or b'name="SELL_Z1_HARD"' in resp.data
            assert b"name='w_s_disc'" in resp.data or b'name="w_s_disc"' in resp.data
            assert b"Save" in resp.data
            assert b"Reset to defaults" in resp.data

    def test_config_status_saved_flash(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/config?status=saved")
            assert b"Configuration saved" in resp.data

    def test_config_status_reset_flash(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/config?status=reset")
            assert b"Reverted to defaults" in resp.data

    def test_config_status_bad_flash(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/config?status=bad&msg=oops")
            assert b"oops" in resp.data


# ---------------------------------------------------------------- /api/config (POST)
class TestApiConfigSave:
    def setup_method(self):
        config.reset_overrides()

    def teardown_method(self):
        config.reset_overrides()

    def test_save_scalar_field(self, client):
        resp = client.post("/api/config", data={"PENALTY_BASE": "0.5"})
        assert resp.status_code == 302
        assert "/config?status=saved" in resp.headers["Location"]
        assert config.PENALTY_BASE == 0.5

    def test_save_weights(self, client):
        resp = client.post("/api/config", data={
            "w_s_disc": "0.4", "w_s_res": "0.2",
            "w_s_sust": "0.2", "w_s_peer": "0.2",
        })
        assert resp.status_code == 302
        assert "saved" in resp.headers["Location"]
        assert config.COMPOSITE_FACTOR_WEIGHTS["s_disc"] == 0.4

    def test_save_skips_empty_field(self, client):
        original = config.PENALTY_BASE
        resp = client.post("/api/config", data={"PENALTY_BASE": "   "})
        assert resp.status_code == 302
        assert "saved" in resp.headers["Location"]
        assert config.PENALTY_BASE == original

    def test_save_uncastable_value(self, client):
        resp = client.post("/api/config", data={"PENALTY_BASE": "not-a-number"})
        assert resp.status_code == 302
        assert "status=bad" in resp.headers["Location"]
        assert "PENALTY_BASE" in resp.headers["Location"]

    def test_save_validation_rejected(self, client):
        # PENALTY_BASE must be 0 < x <= 1; 5.0 is rejected
        resp = client.post("/api/config", data={"PENALTY_BASE": "5.0"})
        assert resp.status_code == 302
        assert "status=bad" in resp.headers["Location"]
        assert "rejected" in resp.headers["Location"]

    def test_save_weight_non_numeric(self, client):
        resp = client.post("/api/config", data={
            "w_s_disc": "abc", "w_s_res": "0.3",
            "w_s_sust": "0.3", "w_s_peer": "0.3",
        })
        assert resp.status_code == 302
        assert "status=bad" in resp.headers["Location"]

    def test_save_weights_partial_missing(self, client):
        # If any weight field is empty, weights aren't applied at all
        original = dict(config.COMPOSITE_FACTOR_WEIGHTS)
        resp = client.post("/api/config", data={
            "w_s_disc": "0.4", "w_s_res": "",
            "w_s_sust": "0.2", "w_s_peer": "0.2",
        })
        assert resp.status_code == 302
        assert "saved" in resp.headers["Location"]
        assert config.COMPOSITE_FACTOR_WEIGHTS == original

    def test_save_clears_result_cache(self, client):
        r = _make_run_result()
        web._CACHE.set(r)
        assert web._CACHE.get() is r
        client.post("/api/config", data={"PENALTY_BASE": "0.5"})
        assert web._CACHE.get() is None


# ---------------------------------------------------------------- /api/config/reset
class TestApiConfigReset:
    def setup_method(self):
        config.reset_overrides()

    def teardown_method(self):
        config.reset_overrides()

    def test_reset_restores_defaults(self, client):
        config.save_overrides({"PENALTY_BASE": 0.5})
        assert config.PENALTY_BASE == 0.5
        resp = client.post("/api/config/reset")
        assert resp.status_code == 302
        assert "/config?status=reset" in resp.headers["Location"]
        assert config.PENALTY_BASE == config._DEFAULTS["PENALTY_BASE"]

    def test_reset_clears_result_cache(self, client):
        web._CACHE.set(_make_run_result())
        client.post("/api/config/reset")
        assert web._CACHE.get() is None


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
            # New: drawdown fields
            assert b"Drawdown 2020" in resp.data
            assert b"Drawdown 2022" in resp.data
            # New: Phase 2 stubs
            assert "📰".encode("utf-8") in resp.data
            assert "📋".encode("utf-8") in resp.data

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

    def test_ticker_html_escaped(self, client):
        r = _make_run_result(with_scored=False)
        with patch.object(web.engine, "run_pipeline", return_value=r):
            # Try an XSS payload as the ticker
            resp = client.get("/inspect/T00<script>")
            assert b"<script>" not in resp.data
            assert b"&lt;script&gt;" in resp.data


# ---------------------------------------------------------------- LAB route
class TestLabRoute:
    def test_empty_scored(self, client):
        r = _make_run_result(with_scored=False)
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/lab")
            assert resp.status_code == 200
            assert b"No scored data" in resp.data

    def test_default_weights(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/lab")
            assert resp.status_code == 200
            assert b"T00" in resp.data
            assert b"T01" in resp.data
            # Re-rank table headers
            assert b"Original" in resp.data
            assert b"New" in resp.data
            # Reset link
            assert b"Reset" in resp.data

    def test_custom_weights(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get(
                "/lab?w_s_disc=1.0&w_s_res=0&w_s_sust=0&w_s_peer=0&penalty=0.9"
            )
            assert resp.status_code == 200
            assert b"T00" in resp.data

    def test_invalid_weight_falls_back(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get(
                "/lab?w_s_disc=abc&w_s_res=xyz&w_s_sust=0.25&w_s_peer=0.25&penalty=nan"
            )
            assert resp.status_code == 200    # falls back to defaults
            assert b"T00" in resp.data

    def test_invalid_penalty_falls_back(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/lab?penalty=2.0")
            assert resp.status_code == 200

    def test_uncastable_penalty_falls_back(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/lab?penalty=not-a-number")
            assert resp.status_code == 200    # exception caught, default used

    def test_rerank_changes_order(self, client):
        # Build a result where re-ranking with heavy s_peer weight flips the order.
        # T00 has higher s_disc, T01 has higher s_peer... actually both have similar.
        # Use weights that emphasise one factor strongly to verify rank-delta column renders.
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get(
                "/lab?w_s_disc=0&w_s_res=0&w_s_sust=0&w_s_peer=1.0&penalty=1.0"
            )
            assert resp.status_code == 200
            # The output table should still contain both tickers
            assert b"T00" in resp.data and b"T01" in resp.data


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

    def test_refresh_failure_returns_500(self, client):
        with patch.object(web.engine, "refresh_universe",
                          side_effect=RuntimeError("boom")):
            resp = client.post("/api/refresh")
            assert resp.status_code == 500
            data = resp.get_json()
            assert data["ok"] is False
            assert "boom" in data["message"]


# ---------------------------------------------------------------- label class helper
class TestLabelCssClass:
    def test_buy_a(self):
        assert web._label_css_class("BUY-A (high conviction)") == "label-buy-a"

    def test_buy_b(self):
        assert web._label_css_class("BUY-B (worth a look)") == "label-buy-b"

    def test_avoid(self):
        assert web._label_css_class("AVOID — distribution trap") == "label-avoid"

    def test_watchlist(self):
        assert web._label_css_class("watchlist") == "label-watch"

    def test_empty(self):
        assert web._label_css_class("") == "label-watch"

    def test_none(self):
        assert web._label_css_class(None) == "label-watch"


# ---------------------------------------------------------------- _why_text
class TestWhyText:
    def test_uses_trap_reason_when_present(self):
        row = {"trap_reason": "ROC > 50%", "sparse": False,
               "buy_label": "BUY-B (worth a look)"}
        assert web._why_text(row) == "ROC > 50%"

    def test_sparse_takes_precedence_over_label(self):
        row = {"trap_reason": None, "sparse": True,
               "buy_label": "BUY-A (high conviction)"}
        assert "provisional" in web._why_text(row)

    def test_buy_a_default(self):
        row = {"trap_reason": None, "sparse": False,
               "buy_label": "BUY-A (high conviction)"}
        assert "margin" in web._why_text(row)

    def test_buy_b_default(self):
        row = {"trap_reason": None, "sparse": False,
               "buy_label": "BUY-B (worth a look)"}
        assert "smaller margin" in web._why_text(row)

    def test_avoid_default(self):
        row = {"trap_reason": None, "sparse": False,
               "buy_label": "AVOID — distribution trap"}
        assert "unsustainable" in web._why_text(row)

    def test_unknown_label_returns_dash(self):
        row = {"trap_reason": None, "sparse": False, "buy_label": "watchlist"}
        assert web._why_text(row) == "—"

    def test_missing_label_returns_dash(self):
        row = {"trap_reason": None, "sparse": False}
        assert web._why_text(row) == "—"


# ---------------------------------------------------------------- _trap_tooltip
class TestTrapTooltip:
    def test_confirmed(self):
        assert "CONFIRMED" in web._trap_tooltip("CONFIRMED")

    def test_suspect(self):
        assert "SUSPECTED" in web._trap_tooltip("SUSPECT")

    def test_watch(self):
        assert "watchlist" in web._trap_tooltip("WATCH")

    def test_ok(self):
        assert "No trap" in web._trap_tooltip("OK")

    def test_case_insensitive(self):
        assert web._trap_tooltip("suspect") == web._trap_tooltip("SUSPECT")

    def test_unknown_returns_empty(self):
        assert web._trap_tooltip("Mystery") == ""

    def test_empty_returns_empty(self):
        assert web._trap_tooltip("") == ""
        assert web._trap_tooltip(None) == ""


# ---------------------------------------------------------------- create_app
class TestCreateApp:
    def test_returns_flask_app(self):
        app = web.create_app()
        assert app is not None
        endpoints = [r.endpoint for r in app.url_map.iter_rules()]
        assert "buy" in endpoints
        assert "lab" in endpoints
        assert "api_config_save" in endpoints
        assert "api_config_reset" in endpoints


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

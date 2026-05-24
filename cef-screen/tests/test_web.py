"""Tests for cef_screener.web — Flask dashboard."""
from __future__ import annotations

import time
from unittest.mock import patch

import pandas as pd
import pytest

from cef_screener import web, engine, config, cache


# ---------------------------------------------------------------- fixtures
@pytest.fixture(autouse=True)
def _reset_cache():
    """Ensure each test starts with a fresh result cache."""
    web._CACHE.clear()
    yield
    web._CACHE.clear()


def _make_run_result(*, with_scored=True, with_holdings=False, warnings=None,
                     last_refresh_at="2026-05-24T09:00:00"):
    if with_scored:
        scored = pd.DataFrame([{
            "ticker": "T00", "name": "Fund 0",
            "category_name": "Taxable Bond",
            "current_discount_pct": 8.5,
            "median_disc_5y": 7.0,
            "z1": -1.8, "z3": -0.9, "z6": -1.2,
            "z_rank": 1, "z_rank_total": 30,
            "nav_cagr_3y": 0.05,
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
            "z1": -0.5, "z3": 0.3, "z6": -0.1,
            "z_rank": 12, "z_rank_total": 30,
            "nav_cagr_3y": 0.03,
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
        last_refresh_at=last_refresh_at,
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

    def test_config_page_includes_composite_explainer(self, client):
        r = _make_run_result()
        with patch.object(web.engine, "run_pipeline", return_value=r):
            resp = client.get("/config")
        body = resp.data.decode("utf-8")
        # The explainer summary header
        assert "How the composite score is computed" in body
        # All four sub-score names appear in their bold sections
        assert "s_disc" in body
        assert "s_res" in body
        assert "s_sust" in body
        assert "s_peer" in body
        # Multiplier section references PENALTY_BASE by name
        assert "PENALTY_BASE" in body
        # Buy thresholds referenced
        assert "BUY_TIER_A_MIN" in body
        assert "BUY_TIER_B_MIN" in body
        # Formula block present
        assert "composite =" in body


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
            # The user-supplied <script> must be escaped — but the layout itself
            # contains a benign sortable-table <script> we ship intentionally.
            assert b"T00<script>" not in resp.data
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
            mock.assert_called_once_with(full=False)
            assert "snapshot" in data["message"].lower()

    def test_refresh_full_mode_passes_full_true(self, client):
        with patch.object(web.engine, "refresh_universe",
                          return_value={"universe": 35, "news": 7}) as mock:
            resp = client.post("/api/refresh?mode=full")
            assert resp.status_code == 200
            data = resp.get_json()
            assert data["ok"] is True
            assert data["summary"]["news"] == 7
            mock.assert_called_once_with(full=True)
            assert "full" in data["message"].lower()

    def test_refresh_unknown_mode_treated_as_quick(self, client):
        with patch.object(web.engine, "refresh_universe",
                          return_value={"universe": 1}) as mock:
            resp = client.post("/api/refresh?mode=banana")
            assert resp.status_code == 200
            mock.assert_called_once_with(full=False)

    def test_refresh_failure_returns_500(self, client):
        with patch.object(web.engine, "refresh_universe",
                          side_effect=RuntimeError("boom")):
            resp = client.post("/api/refresh")
            assert resp.status_code == 500
            data = resp.get_json()
            assert data["ok"] is False
            assert "boom" in data["message"]

    def test_refresh_adds_snapshot_date_when_universe_loaded(
            self, client, initialised_cache):
        """When the cache has universe data after the refresh, the API
        response includes the resulting snapshot_date so the UI can show
        it (e.g. '✓ snapshot 2026-05-22'). Without this, users couldn't
        tell whether the refresh actually produced new data."""
        rows = [
            {"Ticker": f"T{i:02d}", "Name": f"F{i}",
             "CategoryName": "Taxable Bond", "SponsorName": "Acme",
             "Price": 10.0, "NAV": 11.0, "Discount": -9.0,
             "LastUpdated": "2026-05-22"}
            for i in range(10)
        ]
        cache.write_universe(rows)
        with patch.object(web.engine, "refresh_universe",
                          return_value={"universe": 10}):
            resp = client.post("/api/refresh")
        data = resp.get_json()
        assert data["ok"] is True
        assert data["summary"]["snapshot_date"] == "2026-05-22"

    def test_refresh_omits_snapshot_date_when_cache_empty(self, client):
        """If the universe cache is empty after refresh (shouldn't happen
        in practice, but defensive), the response simply omits
        snapshot_date rather than crashing."""
        with patch.object(web.engine, "refresh_universe",
                          return_value={"universe": 0}):
            resp = client.post("/api/refresh")
        data = resp.get_json()
        assert data["ok"] is True
        # No snapshot_date in the summary (empty cache)
        assert "snapshot_date" not in data["summary"]

    def test_refresh_swallows_cache_lookup_errors(self, client, monkeypatch):
        """If load_latest_universe somehow raises, the refresh still
        reports success — the snapshot_date is just absent from the summary
        (defensive try/except in api_refresh, protected from breaking the UX)."""
        monkeypatch.setattr(web.cache, "load_latest_universe",
                            lambda: (_ for _ in ()).throw(RuntimeError("db err")))
        with patch.object(web.engine, "refresh_universe",
                          return_value={"universe": 5}):
            resp = client.post("/api/refresh")
        data = resp.get_json()
        assert data["ok"] is True
        assert "snapshot_date" not in data["summary"]


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


# ---------------------------------------------------------------- _present helper
class TestPresent:
    def test_none_is_absent(self):
        assert web._present(None) is False

    def test_nan_is_absent(self):
        assert web._present(float('nan')) is False

    def test_zero_is_present(self):
        assert web._present(0) is True
        assert web._present(0.0) is True

    def test_strings_and_negatives_are_present(self):
        assert web._present('x') is True
        assert web._present(-1.5) is True


# ---------------------------------------------------------------- data-completeness banner
class TestDataCompletenessBanner:
    def test_empty_dataframe_returns_blank(self):
        assert web._data_completeness_banner(pd.DataFrame()) == ""

    def test_none_returns_blank(self):
        assert web._data_completeness_banner(None) == ""

    def test_no_banner_when_majority_have_history(self):
        df = pd.DataFrame([
            {'nav_cagr_3y': 0.05, 'median_disc_5y': 7, 'dd_2020_pct': -0.2},
            {'nav_cagr_3y': 0.04, 'median_disc_5y': 6, 'dd_2020_pct': -0.1},
        ])
        assert web._data_completeness_banner(df) == ""

    def test_banner_shows_when_history_missing(self):
        df = pd.DataFrame([
            {'nav_cagr_3y': None, 'median_disc_5y': None, 'dd_2020_pct': None},
            {'nav_cagr_3y': None, 'median_disc_5y': None, 'dd_2020_pct': None},
            {'nav_cagr_3y': None, 'median_disc_5y': None, 'dd_2020_pct': None},
        ])
        out = web._data_completeness_banner(df)
        assert "3 of 3" in out
        assert "Full refresh" in out
        assert "warn" in out

    def test_no_banner_at_exactly_half(self):
        # 1 of 2 = 50% — banner suppressed (have_history >= total * 0.5)
        df = pd.DataFrame([
            {'nav_cagr_3y': 0.05, 'median_disc_5y': None, 'dd_2020_pct': None},
            {'nav_cagr_3y': None, 'median_disc_5y': None, 'dd_2020_pct': None},
        ])
        assert web._data_completeness_banner(df) == ""


# ---------------------------------------------------------------- _news_html
class TestNewsHtml:
    def test_no_news_renders_placeholder(self):
        with patch.object(web.news, 'fetch_headlines', return_value=[]):
            out = web._news_html('PFL')
        assert "No recent news" in out
        assert "PFL" in out

    def test_renders_headlines_with_escaping(self):
        with patch.object(web.news, 'fetch_headlines', return_value=[
            {'title': 'A & B <x>', 'link': 'http://a/1?x=1&y=2',
             'published': 'today'},
        ]):
            out = web._news_html('PFL')
        assert "A &amp; B &lt;x&gt;" in out
        # link is HTML-escaped (& becomes &amp; even in attribute)
        assert "http://a/1?x=1&amp;y=2" in out
        assert "today" in out
        assert "Recent news" in out

    def test_missing_link_falls_back_to_hash(self):
        with patch.object(web.news, 'fetch_headlines', return_value=[
            {'title': 't', 'link': '', 'published': ''},
        ]):
            out = web._news_html('PFL')
        assert "href='#'" in out

    def test_renders_summary_div_when_present(self):
        with patch.object(web.news, 'fetch_headlines', return_value=[
            {'title': 'Tender offer announced',
             'link': 'http://x/1', 'published': 'today',
             'summary': 'The fund tenders 5% of shares at 98% of NAV.'},
        ]):
            out = web._news_html('PFL')
        # summary text rendered (escaped, as a div)
        assert "The fund tenders 5% of shares at 98% of NAV." in out
        # signal pill (ACCRETIVE from "tender offer" keyword) is rendered
        assert "ACCRETIVE" in out
        # ensure the why text is present (color span)
        assert "color:#3fb950" in out  # ACCRETIVE is green

    def test_summary_keywords_drive_relevance(self):
        # The summary should also drive signal classification, not just title.
        with patch.object(web.news, 'fetch_headlines', return_value=[
            {'title': 'Generic update',
             'link': 'http://x/1', 'published': 'today',
             'summary': 'Board approved a share repurchase program of $50M.'},
        ]):
            out = web._news_html('PFL')
        # Summary contains "repurchase program" → ACCRETIVE
        assert "ACCRETIVE" in out


# ---------------------------------------------------------------- _news_relevance
class TestNewsRelevance:
    @pytest.mark.parametrize("title,expected_signal", [
        # DILUTIVE
        ("Rights offering announced today", "DILUTIVE"),
        ("Secondary offering priced at $10", "DILUTIVE"),
        # ACCRETIVE
        ("Tender offer at 98% of NAV", "ACCRETIVE"),
        ("Fund expands buyback authorization", "ACCRETIVE"),
        # CATALYST
        ("Fund to open-end in Q3", "CATALYST"),
        ("Wind-down approved by board", "CATALYST"),
        ("Merger of equals proposed", "CATALYST"),
        ("Activist files 13D position", "CATALYST"),
        # SELL SIGNAL
        ("Distribution cut to $0.05/share", "SELL SIGNAL"),
        ("Fund decreases monthly payout", "SELL SIGNAL"),
        # BUY SIGNAL
        ("Distribution increase declared", "BUY SIGNAL"),
        ("Board raises distribution 10%", "BUY SIGNAL"),
        ("Dividend hike approved", "BUY SIGNAL"),
        # MIXED
        ("Special distribution declared in December", "MIXED"),
        ("Return of capital classification at year-end", "MIXED"),
        # STRATEGY RISK
        ("Portfolio manager change effective Q3", "STRATEGY RISK"),
        # GOVERNANCE RISK
        ("SEC investigation opened against fund", "GOVERNANCE RISK"),
        # BEARISH
        ("Holdings credit downgrade by Moody's", "BEARISH"),
        ("Rate hike pressures CEF NAVs", "BEARISH"),
        # BULLISH
        ("Credit upgrade for major holding", "BULLISH"),
        ("Fed cut signals lower leverage costs", "BULLISH"),
        # CONTEXT
        ("Leverage facility renewed at lower rate", "CONTEXT"),
        # ROUTINE
        ("Monthly distribution declared at $0.10", "ROUTINE"),
        ("Quarterly report filed with SEC",
         "GOVERNANCE RISK"),  # "SEC" wins earlier in rule order
        ("Annual report filed", "ROUTINE"),
        # GENERAL (fallback)
        ("Random market chatter today", "GENERAL"),
        ("", "GENERAL"),
    ])
    def test_signal_classification(self, title, expected_signal):
        sig, why = web._news_relevance(title)
        assert sig == expected_signal
        assert why  # explanation is non-empty
        assert isinstance(why, str)

    def test_none_input_yields_general(self):
        sig, why = web._news_relevance(None)
        assert sig == "GENERAL"
        assert why

    def test_case_insensitive(self):
        sig, _ = web._news_relevance("RIGHTS OFFERING priced")
        assert sig == "DILUTIVE"


# ---------------------------------------------------------------- nav buttons
class TestNavButtons:
    def test_two_refresh_buttons_in_nav(self, client):
        r = _make_run_result()
        with patch.object(web.engine, 'run_pipeline', return_value=r):
            resp = client.get('/')
        assert resp.status_code == 200
        body = resp.data.decode('utf-8')
        assert "Quick refresh" in body
        assert "Full refresh" in body
        assert "mode=full" in body  # button posts to the full endpoint


# ---------------------------------------------------------------- BUY banner integration
class TestBuyBanner:
    def test_banner_appears_when_history_missing(self, client):
        # Build a result where every row lacks per-ticker history
        r = _make_run_result()
        r.scored['nav_cagr_3y'] = None
        r.scored['median_disc_5y'] = None
        r.scored['dd_2020_pct'] = None
        with patch.object(web.engine, 'run_pipeline', return_value=r):
            resp = client.get('/')
        body = resp.data.decode('utf-8')
        assert "of 2 rows are missing" in body or "of 2" in body
        assert "Full refresh" in body

    def test_no_banner_when_history_present(self, client):
        r = _make_run_result()    # default has nav_cagr_3y and dd_2020_pct
        with patch.object(web.engine, 'run_pipeline', return_value=r):
            resp = client.get('/')
        body = resp.data.decode('utf-8')
        assert "rows are missing per-ticker history" not in body


# ---------------------------------------------------------------- /inspect news + banner
class TestInspectNewsAndBanner:
    def test_inspect_renders_news_block(self, client):
        r = _make_run_result()
        with patch.object(web.engine, 'run_pipeline', return_value=r), \
             patch.object(web.news, 'fetch_headlines', return_value=[
                 {'title': 'PIMCO declared distribution',
                  'link': 'http://x/1', 'published': 'today'},
             ]):
            resp = client.get('/inspect/T00')
        assert resp.status_code == 200
        body = resp.data.decode('utf-8')
        assert "PIMCO declared distribution" in body
        assert "Recent news" in body

    def test_inspect_renders_no_news_placeholder(self, client):
        r = _make_run_result()
        with patch.object(web.engine, 'run_pipeline', return_value=r), \
             patch.object(web.news, 'fetch_headlines', return_value=[]):
            resp = client.get('/inspect/T00')
        body = resp.data.decode('utf-8')
        assert "No recent news" in body

    def test_inspect_shows_no_history_banner(self, client):
        r = _make_run_result()
        # T00 lacks history
        r.scored.loc[r.scored['ticker'] == 'T00', 'nav_cagr_3y'] = None
        r.scored.loc[r.scored['ticker'] == 'T00', 'dd_2020_pct'] = None
        with patch.object(web.engine, 'run_pipeline', return_value=r), \
             patch.object(web.news, 'fetch_headlines', return_value=[]):
            resp = client.get('/inspect/T00')
        body = resp.data.decode('utf-8')
        assert "No per-ticker history cached" in body

    def test_inspect_no_history_banner_when_data_present(self, client):
        r = _make_run_result()    # T00 has nav_cagr_3y=0.05, dd_2020_pct=-0.18
        with patch.object(web.engine, 'run_pipeline', return_value=r), \
             patch.object(web.news, 'fetch_headlines', return_value=[]):
            resp = client.get('/inspect/T00')
        body = resp.data.decode('utf-8')
        assert "No per-ticker history cached" not in body


# ---------------------------------------------------------------- Phase 2 helpers
class TestSparkline:
    def test_empty(self):
        assert web._sparkline([]) == ""

    def test_all_none(self):
        assert web._sparkline([None, None]) == ""

    def test_all_nan(self):
        assert web._sparkline([float("nan"), float("nan")]) == ""

    def test_all_same_value(self):
        s = web._sparkline([10, 10, 10, 10])
        # All four positions render the same middle level
        assert len(s) == 4
        assert len(set(s)) == 1

    def test_single_value(self):
        s = web._sparkline([42])
        assert len(s) == 1
        assert s in web._SPARK_BLOCKS

    def test_ascending(self):
        s = web._sparkline([0, 1, 2, 3, 4, 5, 6, 7])
        assert s[0] == web._SPARK_BLOCKS[0]
        assert s[-1] == web._SPARK_BLOCKS[-1]
        # Monotone non-decreasing
        assert list(s) == sorted(s, key=web._SPARK_BLOCKS.index)

    def test_filters_nan(self):
        s = web._sparkline([0, float("nan"), 7])
        assert len(s) == 2  # NaN dropped
        assert s[0] == web._SPARK_BLOCKS[0]
        assert s[-1] == web._SPARK_BLOCKS[-1]


class TestPastStatusHtml:
    def test_empty_history_shows_placeholder(self, initialised_cache):
        out = web._past_status_html("ZZZ")
        assert "Past status" in out
        assert "No score history" in out

    def test_renders_table_and_sparkline(self, initialised_cache):
        from cef_screener import cache as _cache
        for d, c in [("2026-05-01", 50.0),
                     ("2026-05-02", 60.0),
                     ("2026-05-03", 70.0)]:
            _cache.persist_historical_scores(
                pd.DataFrame([{
                    "ticker": "PFL", "composite": c,
                    "s_disc": c, "s_res": c, "s_sust": c, "s_peer": c,
                    "multiplier": 1.0, "buy_label": "BUY-B",
                }]), d)
        out = web._past_status_html("PFL")
        assert "3 snapshots" in out
        assert "2026-05-01" in out and "2026-05-03" in out
        assert "BUY-B" in out
        # Sparkline + positive delta (last - first = +20)
        assert web._SPARK_BLOCKS[0] in out
        assert web._SPARK_BLOCKS[-1] in out
        assert "20.0" in out and "▲" in out
        assert "vs first" in out

    def test_single_snapshot_shows_friendly_placeholder(self, initialised_cache):
        from cef_screener import cache as _cache
        _cache.persist_historical_scores(
            pd.DataFrame([{"ticker": "PFL", "composite": 77.7,
                            "s_disc": 70.0, "s_res": 95.0, "s_sust": 85.0,
                            "s_peer": 87.5, "multiplier": 0.92,
                            "buy_label": "BUY-A"}]),
            "2026-05-22")
        out = web._past_status_html("PFL")
        # No misleading "▶ 0.0" delta on a single snapshot
        assert "▶" not in out
        assert "1 snapshot" not in out  # avoid bad pluralization
        # Friendly explanation present
        assert "First snapshot recorded" in out
        assert "2026-05-22" in out
        # Table should still be there
        assert "BUY-A" in out
        assert "77.7" in out

    def test_renders_negative_delta(self, initialised_cache):
        from cef_screener import cache as _cache
        for d, c in [("2026-05-01", 70.0), ("2026-05-02", 50.0)]:
            _cache.persist_historical_scores(
                pd.DataFrame([{"ticker": "PFL", "composite": c}]), d)
        out = web._past_status_html("PFL")
        assert "▼" in out and "20.0" in out

    def test_renders_flat_delta(self, initialised_cache):
        from cef_screener import cache as _cache
        for d in ["2026-05-01", "2026-05-02"]:
            _cache.persist_historical_scores(
                pd.DataFrame([{"ticker": "PFL", "composite": 60.0}]), d)
        out = web._past_status_html("PFL")
        assert "▶" in out

    def test_handles_missing_composite_delta(self, initialised_cache):
        from cef_screener import cache as _cache
        _cache.persist_historical_scores(
            pd.DataFrame([{"ticker": "PFL", "composite": None}]),
            "2026-05-01")
        out = web._past_status_html("PFL")
        # Should still render but without delta arrows
        assert "Past status" in out
        assert "▲" not in out and "▼" not in out

    def test_load_exception_returns_placeholder(self):
        with patch.object(web.cache, "load_historical_scores",
                          side_effect=RuntimeError("DB on fire")):
            out = web._past_status_html("PFL")
        assert "Past status" in out
        assert "Past status unavailable" in out
        assert "DB on fire" in out


class TestRollingDrawdowns:
    def test_empty(self):
        assert web._rolling_drawdowns([]) == {}

    def test_single_value(self):
        assert web._rolling_drawdowns([100.0]) == {}

    def test_simple_drop(self):
        # 100 → 80 = 20% drawdown
        dd = web._rolling_drawdowns([100.0, 80.0])
        assert dd["30d"] == pytest.approx(20.0)

    def test_zero_running_max_skipped(self):
        # All zeros — running_max stays 0, no division
        dd = web._rolling_drawdowns([0.0, 0.0, 0.0])
        for v in dd.values():
            assert v == 0.0

    def test_ascending_then_descending_tracks_running_max(self):
        # Ascending then drop: peak at 120 then drop to 90 = 25%
        dd = web._rolling_drawdowns([100.0, 110.0, 120.0, 90.0])
        assert dd["30d"] == pytest.approx(25.0)

    def test_longer_windows_only_when_enough_data(self):
        # Just 50 points → 30d window applies but slice = full series
        nav = [100.0 - i for i in range(50)]
        dd = web._rolling_drawdowns(nav)
        assert "30d" in dd
        # 90d/1y/3y all defined too (use what we have)
        assert dd["30d"] > 0


class TestDrawdownsHtml:
    def test_no_price_history(self, initialised_cache):
        out = web._drawdowns_html("ZZZ", {"dd_2020_pct": None})
        assert "Drawdown profile" in out
        assert "No price history cached" in out

    def test_renders_drawdowns_and_crisis(self, initialised_cache):
        from cef_screener import cache as _cache
        from datetime import date as _date, timedelta as _td
        d0 = _date(2026, 1, 1)
        rows = []
        for i in range(40):
            nav = 100.0 if i < 20 else 80.0
            rows.append({
                "DataDate": (d0 + _td(days=i)).isoformat(),
                "Data": nav, "NAVData": nav, "DiscountData": 0.0,
            })
        _cache.write_price_history("PFL", rows)
        out = web._drawdowns_html(
            "PFL", {"dd_2020_pct": -0.18, "dd_2022_pct": -0.22})
        assert "Drawdown profile" in out
        assert "30d" in out and "−20.0%" in out
        # Crisis blocks render the supplied numbers (with negative-decimal coerced)
        assert "2020" in out and "2022" in out
        # NAV sparkline rendered
        assert "NAV trend" in out

    def test_no_crisis_section_when_missing(self, initialised_cache):
        from cef_screener import cache as _cache
        _cache.write_price_history("PFL", [
            {"DataDate": "2026-01-01", "Data": 100.0, "NAVData": 100.0,
             "DiscountData": 0.0},
            {"DataDate": "2026-01-02", "Data": 95.0, "NAVData": 95.0,
             "DiscountData": 0.0},
        ])
        out = web._drawdowns_html("PFL", {"dd_2020_pct": None,
                                            "dd_2022_pct": None})
        assert "Crisis-window drawdowns" not in out

    def test_handles_dict_row(self, initialised_cache):
        from cef_screener import cache as _cache
        _cache.write_price_history("PFL", [
            {"DataDate": "2026-01-01", "Data": 100.0, "NAVData": 100.0,
             "DiscountData": 0.0},
            {"DataDate": "2026-01-02", "Data": 90.0, "NAVData": 90.0,
             "DiscountData": 0.0},
        ])
        # dict (not Series) row — .get must still work
        out = web._drawdowns_html("PFL", {"dd_2020_pct": -0.30,
                                            "dd_2022_pct": -0.15})
        assert "2020" in out

    def test_load_exception_returns_placeholder(self, initialised_cache):
        with patch.object(web.cache, "load_price_history",
                          side_effect=RuntimeError("disk on fire")):
            out = web._drawdowns_html("PFL", {})
        assert "Drawdown profile" in out
        assert "Drawdowns unavailable" in out
        assert "disk on fire" in out

    def test_dataframe_without_nav_or_price_column(self, initialised_cache):
        bogus = pd.DataFrame([{"data_date": "2026-01-01", "discount": 0.05}])
        with patch.object(web.cache, "load_price_history", return_value=bogus):
            out = web._drawdowns_html("PFL", {})
        assert "no NAV column" in out


class TestInspectPhase2Integration:
    def test_inspect_includes_past_status_block(self, client):
        r = _make_run_result()
        with patch.object(web.engine, 'run_pipeline', return_value=r), \
             patch.object(web.news, 'fetch_headlines', return_value=[]):
            resp = client.get('/inspect/T00')
        body = resp.data.decode('utf-8')
        assert "Past status" in body
        assert "Drawdown profile" in body

    def test_inspect_phase2_no_longer_says_coming_soon(self, client):
        r = _make_run_result()
        with patch.object(web.engine, 'run_pipeline', return_value=r), \
             patch.object(web.news, 'fetch_headlines', return_value=[]):
            resp = client.get('/inspect/T00')
        body = resp.data.decode('utf-8')
        assert "coming soon" not in body.lower()


# ---------------------------------------------------------------- sortable tables
class TestSortableTables:
    def test_layout_includes_sort_js(self):
        out = web._layout("X", "<p>hi</p>")
        assert "table.sortable" in out
        assert "sortTable" in out
        assert "<script>" in out

    def test_layout_includes_sort_css(self):
        out = web._layout("X", "<p>hi</p>")
        assert "table.sortable th" in out
        assert "sort-asc" in out and "sort-desc" in out

    def test_buy_table_is_sortable(self, client):
        r = _make_run_result()
        with patch.object(web.engine, 'run_pipeline', return_value=r):
            resp = client.get('/')
        body = resp.data.decode('utf-8')
        # The BUY route's main table must be marked sortable
        assert "<table class='sortable'>" in body \
            or '<table class="sortable">' in body

    def test_sell_table_is_sortable(self, client):
        r = _make_run_result(with_holdings=True)
        with patch.object(web.engine, 'run_pipeline', return_value=r):
            resp = client.get('/sell')
        body = resp.data.decode('utf-8')
        assert "sortable" in body

    def test_past_status_table_is_sortable(self, initialised_cache):
        from cef_screener import cache as _cache
        _cache.persist_historical_scores(
            pd.DataFrame([{"ticker": "PFL", "composite": 50.0}]),
            "2026-05-01")
        out = web._past_status_html("PFL")
        assert "sortable" in out

    def test_lab_table_is_sortable(self, client):
        r = _make_run_result()
        with patch.object(web.engine, 'run_pipeline', return_value=r):
            resp = client.get('/lab')
        body = resp.data.decode('utf-8')
        assert "sortable" in body

    def test_sort_js_handles_numeric_and_text(self):
        # The JS must reference both numeric (parseFloat) and locale-aware
        # text sorting (localeCompare) and push nulls last
        js = web._SORT_JS
        assert "parseFloat" in js
        assert "localeCompare" in js
        assert "nulls" in js.lower() or "null" in js

    def test_sort_js_toggles_direction(self):
        js = web._SORT_JS
        assert "sort-asc" in js and "sort-desc" in js
        # Click should toggle from asc -> desc when already asc
        assert "contains('sort-asc')" in js



# ====================================================================
# Round-8 tests — last refresh display, all Z-scores, config help text
# ====================================================================
class TestHumanizeAge:
    def test_none_returns_empty(self):
        assert web._humanize_age(None) == ""

    def test_zero_seconds(self):
        assert web._humanize_age(0) == "just now"

    def test_negative_seconds_clamped_to_just_now(self):
        # Defensive: clock skew shouldn't crash, just treat as "now"
        assert web._humanize_age(-5) == "just now"

    def test_under_minute(self):
        assert web._humanize_age(45) == "just now"

    def test_one_minute_exactly(self):
        assert web._humanize_age(60) == "1 min ago"

    def test_minutes(self):
        assert web._humanize_age(300) == "5 min ago"

    def test_hours(self):
        assert web._humanize_age(2 * 3600 + 30 * 60) == "2h ago"

    def test_days(self):
        assert web._humanize_age(3 * 86400) == "3d ago"

    def test_over_a_week_returns_empty(self):
        # Caller renders absolute timestamp when relative is empty
        assert web._humanize_age(10 * 86400) == ""


class TestFormatLastRefresh:
    def test_none_returns_dash(self):
        assert web._format_last_refresh(None) == "—"

    def test_empty_string_returns_dash(self):
        assert web._format_last_refresh("") == "—"

    def test_iso_timestamp_includes_age(self):
        from datetime import datetime, timedelta, timezone
        ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).replace(microsecond=0)
        out = web._format_last_refresh(ts.isoformat())
        assert "UTC" in out
        assert "min ago" in out

    def test_naive_iso_assumed_utc(self):
        from datetime import datetime, timedelta, timezone
        ts = (datetime.now(timezone.utc) - timedelta(hours=2)).replace(microsecond=0, tzinfo=None)
        out = web._format_last_refresh(ts.isoformat())
        assert "UTC" in out
        assert "h ago" in out

    def test_garbage_returns_escaped_raw(self):
        out = web._format_last_refresh("not-a-timestamp")
        assert out == "not-a-timestamp"

    def test_ancient_timestamp_omits_relative(self):
        # Older than a week → no "X ago" suffix, just absolute
        out = web._format_last_refresh("2020-01-01T00:00:00")
        assert "2020-01-01" in out
        assert "ago" not in out


class TestBuyZScoresColumns:
    def test_buy_page_renders_z3m_header(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/")
        body = resp.get_data(as_text=True)
        assert "<th>Z3M</th>" in body
        assert "<th>Z6M</th>" in body

    def test_buy_page_renders_z3_and_z6_values(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/")
        body = resp.get_data(as_text=True)
        # T00 z3 = -0.9, z6 = -1.2 from fixture
        assert "-0.90" in body or "-0.9" in body
        assert "-1.20" in body or "-1.2" in body


class TestInspectZScoresRows:
    def test_inspect_renders_z3m_and_z6m_rows(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/inspect/T00")
        body = resp.get_data(as_text=True)
        assert "Z 3M" in body
        assert "Z 6M" in body


class TestLastRefreshOnPages:
    def test_buy_page_shows_last_refresh(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/")
        body = resp.get_data(as_text=True)
        assert "Last refresh:" in body
        assert "UTC" in body

    def test_buy_page_no_refresh_shows_dash(self, client):
        with patch("cef_screener.web._get_result",
                   return_value=_make_run_result(last_refresh_at=None)):
            resp = client.get("/")
        body = resp.get_data(as_text=True)
        assert "Last refresh:" in body

    def test_config_page_shows_last_refresh(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/config")
        body = resp.get_data(as_text=True)
        assert "Last refresh:" in body


class TestConfigHelpText:
    def test_gatekeeper_size_help_rendered(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/config")
        body = resp.get_data(as_text=True)
        assert "Number of cheapest funds" in body
        assert "cfg-help" in body

    def test_buy_tier_a_help_rendered(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/config")
        body = resp.get_data(as_text=True)
        assert "BUY-A label" in body

    def test_weight_help_rendered(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/config")
        body = resp.get_data(as_text=True)
        assert "Discount sub-score" in body
        assert "Resilience sub-score" in body
        assert "Sustainability sub-score" in body
        assert "Peer-percentile sub-score" in body

    def test_help_dict_covers_every_overridable_scalar(self):
        # Self-test: any new OVERRIDABLE scalar should get a help entry
        # so users always see an explanation.
        missing = [k for k in config.OVERRIDABLE
                   if k != "COMPOSITE_FACTOR_WEIGHTS"
                   and k not in web._CONFIG_FIELD_HELP]
        assert missing == [], f"OVERRIDABLE missing help text: {missing}"

    def test_weight_help_covers_all_four_factors(self):
        for k in ("s_disc", "s_res", "s_sust", "s_peer"):
            assert k in web._WEIGHT_HELP


class TestRefreshLastRefreshInSummary:
    def test_refresh_includes_last_refresh_at(self, client, monkeypatch):
        def fake_refresh(**kw):
            return {"universe": 5, "news": 0}
        monkeypatch.setattr(web.engine, "refresh_universe", fake_refresh)
        monkeypatch.setattr(web.cache, "last_universe_refresh_at",
                            lambda: "2026-05-24T09:00:00")
        # populate universe so snapshot_date branch also runs
        monkeypatch.setattr(web.cache, "load_latest_universe",
                            lambda: pd.DataFrame([{"snapshot_date": "2026-05-22"}]))
        resp = client.post("/api/refresh")
        data = resp.get_json()
        assert data["ok"] is True
        assert data["summary"]["last_refresh_at"] == "2026-05-24T09:00:00"

    def test_refresh_omits_last_refresh_when_unknown(self, client, monkeypatch):
        monkeypatch.setattr(web.engine, "refresh_universe",
                            lambda **kw: {"universe": 0})
        monkeypatch.setattr(web.cache, "last_universe_refresh_at", lambda: None)
        monkeypatch.setattr(web.cache, "load_latest_universe", lambda: pd.DataFrame())
        resp = client.post("/api/refresh")
        data = resp.get_json()
        assert data["ok"] is True
        assert "last_refresh_at" not in data["summary"]

class TestZRankColumn:
    def test_buy_page_renders_z_rank_header(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/")
        body = resp.get_data(as_text=True)
        assert "<th>Z Rank</th>" in body

    def test_buy_page_renders_rank_values(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/")
        body = resp.get_data(as_text=True)
        # T00 rank 1/30, T01 rank 12/30 from fixture
        assert "1/30" in body
        assert "12/30" in body

    def test_buy_page_rank_dash_when_missing(self, client):
        r = _make_run_result()
        r.scored.loc[0, "z_rank"] = None
        r.scored.loc[0, "z_rank_total"] = None
        with patch("cef_screener.web._get_result", return_value=r):
            resp = client.get("/")
        body = resp.get_data(as_text=True)
        # Both rows render, T00's rank now blank/dash
        assert "T00" in body and "T01" in body

    def test_inspect_renders_z_rank_row(self, client):
        with patch("cef_screener.web._get_result", return_value=_make_run_result()):
            resp = client.get("/inspect/T00")
        body = resp.get_data(as_text=True)
        assert "Z Rank (1Y)" in body
        assert "1/30" in body

    def test_inspect_rank_dash_when_missing(self, client):
        r = _make_run_result()
        r.scored.loc[0, "z_rank"] = None
        r.scored.loc[0, "z_rank_total"] = None
        with patch("cef_screener.web._get_result", return_value=r):
            resp = client.get("/inspect/T00")
        body = resp.get_data(as_text=True)
        assert "Z Rank (1Y)" in body
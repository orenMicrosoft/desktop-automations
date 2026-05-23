"""Tests for cef_screener.ingest — HTTP client + 4 fetchers + diagnose."""
from __future__ import annotations

import json
from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import requests

from cef_screener import ingest


# ---------------------------------------------------------------- session
def test_session_is_lazy_singleton():
    ingest._SESSION = None
    s1 = ingest.session()
    s2 = ingest.session()
    assert s1 is s2
    assert isinstance(s1, requests.Session)
    assert s1.headers["User-Agent"]
    assert s1.headers["X-Requested-With"] == "XMLHttpRequest"


def test_build_session_mounts_adapters():
    s = ingest._build_session()
    assert "https://" in s.adapters
    assert "http://" in s.adapters


# ---------------------------------------------------------------- _get_json
class _FakeResponse:
    def __init__(self, status_code: int = 200, payload=None, raise_for=False):
        self.status_code = status_code
        self._payload = payload
        self._raise = raise_for

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self._raise:
            raise requests.HTTPError("500 server error")


def test_get_json_success(monkeypatch):
    captured = {}

    def fake_get(url, headers=None, timeout=None):
        captured.update({"url": url, "headers": headers, "timeout": timeout})
        return _FakeResponse(payload={"ok": True})

    fake_sess = MagicMock()
    fake_sess.get = fake_get
    monkeypatch.setattr(ingest, "session", lambda: fake_sess)
    data = ingest._get_json("/DailyPricing")
    assert data == {"ok": True}
    assert captured["url"].endswith("/DailyPricing")
    assert captured["headers"]["Referer"] == "https://www.cefconnect.com/"


def test_get_json_custom_referer(monkeypatch):
    captured = {}

    def fake_get(url, headers=None, timeout=None):
        captured["headers"] = headers
        return _FakeResponse(payload=[])

    fake_sess = MagicMock()
    fake_sess.get = fake_get
    monkeypatch.setattr(ingest, "session", lambda: fake_sess)
    ingest._get_json("/PricingHistory/PFL/1Y", referer="https://x/fund/PFL")
    assert captured["headers"]["Referer"] == "https://x/fund/PFL"


def test_get_json_404_raises_endpoint_gone(monkeypatch):
    fake_sess = MagicMock()
    fake_sess.get = lambda *a, **kw: _FakeResponse(status_code=404)
    monkeypatch.setattr(ingest, "session", lambda: fake_sess)
    with pytest.raises(ingest.EndpointGone, match="404"):
        ingest._get_json("/Missing")


def test_get_json_500_raises_http_error(monkeypatch):
    fake_sess = MagicMock()
    fake_sess.get = lambda *a, **kw: _FakeResponse(status_code=500, raise_for=True)
    monkeypatch.setattr(ingest, "session", lambda: fake_sess)
    with pytest.raises(requests.HTTPError):
        ingest._get_json("/Boom")


# ---------------------------------------------------------------- fetch_universe
def test_fetch_universe_success(monkeypatch):
    rows = [{"Ticker": f"T{i}"} for i in range(200)]
    monkeypatch.setattr(ingest, "_get_json", lambda path: rows)
    assert ingest.fetch_universe() == rows


def test_fetch_universe_too_small_raises(monkeypatch):
    monkeypatch.setattr(ingest, "_get_json", lambda path: [{"Ticker": "X"}])
    with pytest.raises(ingest.EndpointGone, match="unexpected shape"):
        ingest.fetch_universe()


def test_fetch_universe_wrong_type_raises(monkeypatch):
    monkeypatch.setattr(ingest, "_get_json", lambda path: {"unexpected": "dict"})
    with pytest.raises(ingest.EndpointGone, match="unexpected shape"):
        ingest.fetch_universe()


# ---------------------------------------------------------------- fetch_price_history
def test_fetch_price_history_sorted_ascending(monkeypatch):
    payload = {"Data": {"PriceHistory": [
        {"DataDate": "2025-05-05", "Data": 1},
        {"DataDate": "2025-05-01", "Data": 2},
        {"DataDate": "2025-05-10", "Data": 3},
    ]}}
    monkeypatch.setattr(ingest, "_get_json", lambda path, referer=None: payload)
    rows = ingest.fetch_price_history("PFL", "1M")
    assert [r["DataDate"] for r in rows] == ["2025-05-01", "2025-05-05", "2025-05-10"]


def test_fetch_price_history_default_period_is_1m(monkeypatch):
    seen = {}

    def fake(path, referer=None):
        seen["path"] = path
        return {"Data": {"PriceHistory": []}}

    monkeypatch.setattr(ingest, "_get_json", fake)
    ingest.fetch_price_history("PFL")
    assert seen["path"].endswith("/1M")


def test_fetch_price_history_invalid_period():
    with pytest.raises(ValueError, match="Invalid pricing period"):
        ingest.fetch_price_history("PFL", "7D")


def test_fetch_price_history_handles_null_payload(monkeypatch):
    monkeypatch.setattr(ingest, "_get_json", lambda *a, **k: None)
    assert ingest.fetch_price_history("PFL", "1M") == []


def test_fetch_price_history_handles_empty_data(monkeypatch):
    monkeypatch.setattr(ingest, "_get_json", lambda *a, **k: {"Data": {}})
    assert ingest.fetch_price_history("PFL", "1M") == []


# ---------------------------------------------------------------- fetch_discount_history
def test_fetch_discount_history_sorted(monkeypatch):
    payload = {"Data": [
        {"DataDate": "2025-04-01", "Data": -7},
        {"DataDate": "2025-03-01", "Data": -5},
    ]}
    monkeypatch.setattr(ingest, "_get_json", lambda *a, **k: payload)
    rows = ingest.fetch_discount_history("PFL", "1Y")
    assert [r["DataDate"] for r in rows] == ["2025-03-01", "2025-04-01"]


def test_fetch_discount_history_invalid_period():
    with pytest.raises(ValueError, match="Invalid discount period"):
        ingest.fetch_discount_history("PFL", "1D")


def test_fetch_discount_history_handles_null(monkeypatch):
    monkeypatch.setattr(ingest, "_get_json", lambda *a, **k: None)
    assert ingest.fetch_discount_history("PFL", "1Y") == []


# ---------------------------------------------------------------- fetch_distribution_history
def test_fetch_distribution_history_with_date_objects(monkeypatch):
    seen = {}

    def fake(path, referer=None):
        seen["path"] = path
        return {"Data": [{"TotDiv": 0.1}]}

    monkeypatch.setattr(ingest, "_get_json", fake)
    rows = ingest.fetch_distribution_history("PFL", date(2024, 1, 1), date(2025, 1, 1))
    assert rows == [{"TotDiv": 0.1}]
    assert "2024-01-01/2025-01-01" in seen["path"]


def test_fetch_distribution_history_with_string_dates(monkeypatch):
    seen = {}

    def fake(path, referer=None):
        seen["path"] = path
        return {"Data": []}

    monkeypatch.setattr(ingest, "_get_json", fake)
    ingest.fetch_distribution_history("PFL", "2024-01-01", "2025-01-01")
    assert "2024-01-01/2025-01-01" in seen["path"]


def test_fetch_distribution_history_non_dict_payload(monkeypatch):
    monkeypatch.setattr(ingest, "_get_json", lambda *a, **k: [])
    assert ingest.fetch_distribution_history("PFL", "2024-01-01", "2025-01-01") == []


# ---------------------------------------------------------------- diagnose
def test_diagnose_all_ok(monkeypatch):
    monkeypatch.setattr(ingest, "fetch_universe", lambda: [{} for _ in range(361)])
    monkeypatch.setattr(ingest, "fetch_price_history", lambda t, p: [{} for _ in range(22)])
    monkeypatch.setattr(ingest, "fetch_discount_history", lambda t, p: [{} for _ in range(52)])
    monkeypatch.setattr(ingest, "fetch_distribution_history", lambda t, s, e: [{} for _ in range(12)])
    out = ingest.diagnose("PFL")
    assert out["DailyPricing"].startswith("ok (361")
    assert out["PricingHistory"].startswith("ok (22")
    assert out["DiscountCharter"].startswith("ok (52")
    assert out["distributionhistory"].startswith("ok (12")


def test_diagnose_handles_errors_gracefully(monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("network down")

    monkeypatch.setattr(ingest, "fetch_universe", boom)
    monkeypatch.setattr(ingest, "fetch_price_history", boom)
    monkeypatch.setattr(ingest, "fetch_discount_history", boom)
    monkeypatch.setattr(ingest, "fetch_distribution_history", boom)
    out = ingest.diagnose("PFL")
    for k, v in out.items():
        assert v.startswith("FAIL:"), f"{k}: {v}"

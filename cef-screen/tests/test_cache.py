"""Tests for cef_screener.cache — schema, writers, readers, upsert rules,
incremental refresh orchestrator."""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest

from cef_screener import cache, ingest


# ---------------------------------------------------------------- coercion helpers
class TestCoercion:
    @pytest.mark.parametrize("v,expected", [
        (None, None), ("", None), ("N/A", None),
        (1.5, 1.5), ("3.14", 3.14), (0, 0.0),
        ("not a number", None), ([], None),
    ])
    def test_f(self, v, expected):
        assert cache._f(v) == expected

    @pytest.mark.parametrize("v,expected", [
        (None, None), ("", None),
        (3, 3), ("7", 7),
        (True, 1), (False, 0),
        ("bad", None), ([], None),
    ])
    def test_i(self, v, expected):
        assert cache._i(v) == expected

    @pytest.mark.parametrize("v,expected", [
        (None, None),
        (True, 1), (False, 0),
        ("true", 1), ("True", 1), ("YES", 1), ("1", 1),
        ("no", 0), ("false", 0), ("0", 0),
        (1, 1), (0, 0),
    ])
    def test_b(self, v, expected):
        assert cache._b(v) == expected

    @pytest.mark.parametrize("v,expected", [
        (None, None), ("", None), ("   ", None),
        ("hello", "hello"), ("  spaced  ", "spaced"),
        (123, "123"),
    ])
    def test_s(self, v, expected):
        assert cache._s(v) == expected

    @pytest.mark.parametrize("v,expected", [
        (None, None), ("", None),
        ("2025-05-22", "2025-05-22"),
        ("2025-05-22T00:00:00", "2025-05-22"),
        ("2025-05-22T00:00:00Z", "2025-05-22"),
        ("5/22/2025", "2025-05-22"),
        ("05-22-2025", "2025-05-22"),
        ("2025/05/22", "2025-05-22"),
        ("garbage", None),
        ("  ", None),
    ])
    def test_date_iso(self, v, expected):
        assert cache._date_iso(v) == expected


# ---------------------------------------------------------------- init / connect
def test_init_db_creates_schema(cache_path):
    cache.init_db(cache_path)
    assert cache_path.exists()
    with sqlite3.connect(str(cache_path)) as c:
        tables = {row[0] for row in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
    expected = {"universe_snapshot", "price_history", "discount_history",
                "distribution_history", "computed_metrics", "fetch_meta",
                "schema_version"}
    assert expected.issubset(tables)


def test_init_db_idempotent(cache_path):
    cache.init_db(cache_path)
    cache.init_db(cache_path)  # second call should not raise or duplicate
    with sqlite3.connect(str(cache_path)) as c:
        n = c.execute("SELECT COUNT(*) FROM schema_version").fetchone()[0]
    assert n == 1


def test_init_db_uses_default_path_when_none(initialised_cache):
    # config.cache_db_path() returns the env-overridden path; init_db should use it
    p = cache.init_db()
    assert p == initialised_cache


def test_connect_yields_row_factory(initialised_cache):
    with cache.connect(initialised_cache) as conn:
        assert conn.row_factory is sqlite3.Row


def test_connect_uses_default_path(initialised_cache):
    with cache.connect() as conn:  # no arg → uses config.cache_db_path()
        result = conn.execute("SELECT COUNT(*) FROM universe_snapshot").fetchone()
    assert result[0] == 0


# ---------------------------------------------------------------- write_universe
def test_write_universe_basic(initialised_cache, mock_universe):
    snap = cache.write_universe(mock_universe)
    assert snap == "2026-05-22"
    with cache.connect(initialised_cache) as conn:
        n = conn.execute("SELECT COUNT(*) FROM universe_snapshot").fetchone()[0]
    assert n == len(mock_universe)


def test_write_universe_empty_raises(initialised_cache):
    with pytest.raises(ValueError, match="Empty universe"):
        cache.write_universe([])


def test_write_universe_too_many_missing_dates_raises(initialised_cache):
    rows = [{"Ticker": f"T{i}", "Name": "X"} for i in range(100)]  # no LastUpdated
    with pytest.raises(RuntimeError, match="missing LastUpdated"):
        cache.write_universe(rows)


def test_write_universe_tolerates_some_missing_dates(initialised_cache, mock_universe):
    # Remove date from 2 rows out of 50 → 4% missing → below threshold
    mock_universe[0].pop("LastUpdated")
    mock_universe[0].pop("NAVPublished")
    mock_universe[1].pop("LastUpdated")
    mock_universe[1].pop("NAVPublished")
    snap = cache.write_universe(mock_universe)
    assert snap == "2026-05-22"


def test_write_universe_skips_orphan_rows(initialised_cache, mock_universe):
    mock_universe.append({"Name": "Orphan", "LastUpdated": "2026-05-22T00:00:00"})
    snap = cache.write_universe(mock_universe)
    with cache.connect(initialised_cache) as conn:
        n = conn.execute("SELECT COUNT(*) FROM universe_snapshot").fetchone()[0]
    assert n == 50  # orphan dropped


def test_write_universe_with_existing_conn(initialised_cache, mock_universe):
    with cache.connect(initialised_cache) as conn:
        cache.write_universe(mock_universe, conn=conn)
        n = conn.execute("SELECT COUNT(*) FROM universe_snapshot").fetchone()[0]
    assert n == len(mock_universe)


def test_write_universe_falls_back_to_navpublished(initialised_cache, mock_universe):
    for r in mock_universe:
        r.pop("LastUpdated")
    snap = cache.write_universe(mock_universe)
    assert snap == "2026-05-22"  # got it from NAVPublished


def test_write_universe_replaces_on_duplicate_pk(initialised_cache, mock_universe):
    cache.write_universe(mock_universe)
    mock_universe[0]["Price"] = 999.99
    cache.write_universe(mock_universe)  # should REPLACE
    with cache.connect(initialised_cache) as conn:
        row = conn.execute(
            "SELECT price FROM universe_snapshot WHERE ticker=?",
            (mock_universe[0]["Ticker"],),
        ).fetchone()
    assert row[0] == 999.99


# ---------------------------------------------------------------- write_price_history
def test_write_price_history(initialised_cache, mock_price_history_rows):
    n = cache.write_price_history("PFL", mock_price_history_rows)
    assert n == len(mock_price_history_rows)
    df = cache.load_price_history("PFL")
    assert len(df) == len(mock_price_history_rows)
    assert {"data_date", "price", "nav", "discount"}.issubset(df.columns)


def test_write_price_history_skips_undated(initialised_cache):
    rows = [
        {"DataDate": "2025-05-01", "Data": 10, "NAVData": 11, "DiscountData": -9},
        {"DataDate": None, "Data": 999, "NAVData": 999, "DiscountData": 0},
        {"DataDate": "", "Data": 999, "NAVData": 999, "DiscountData": 0},
    ]
    n = cache.write_price_history("PFL", rows)
    assert n == 1


def test_write_price_history_empty_rows(initialised_cache):
    assert cache.write_price_history("PFL", []) == 0


def test_write_price_history_with_external_conn(initialised_cache, mock_price_history_rows):
    with cache.connect(initialised_cache) as conn:
        n = cache.write_price_history("PFL", mock_price_history_rows, conn=conn)
    assert n == len(mock_price_history_rows)


# ---------------------------------------------------------------- write_discount_history
def test_write_discount_history(initialised_cache, mock_discount_history_rows):
    n = cache.write_discount_history("PFL", mock_discount_history_rows)
    assert n == len(mock_discount_history_rows)
    df = cache.load_discount_history("PFL")
    assert len(df) == len(mock_discount_history_rows)


def test_write_discount_history_empty(initialised_cache):
    assert cache.write_discount_history("PFL", []) == 0


def test_write_discount_history_with_conn(initialised_cache, mock_discount_history_rows):
    with cache.connect(initialised_cache) as conn:
        n = cache.write_discount_history("PFL", mock_discount_history_rows, conn=conn)
    assert n == len(mock_discount_history_rows)


# ---------------------------------------------------------------- write_distribution_history
def test_write_distribution_history(initialised_cache, mock_distribution_history_rows):
    n = cache.write_distribution_history("PFL", mock_distribution_history_rows)
    assert n == len(mock_distribution_history_rows)
    df = cache.load_distribution_history("PFL")
    assert len(df) == len(mock_distribution_history_rows)
    assert df["tot_div"].iloc[0] == 0.08


def test_write_distribution_history_empty(initialised_cache):
    assert cache.write_distribution_history("PFL", []) == 0


def test_write_distribution_history_skips_undeclared(initialised_cache):
    rows = [
        {"DeclaredDateDisplay": "2025-05-15", "TotDiv": 0.08},
        {"DeclaredDateDisplay": None, "TotDiv": 0.10},  # skipped
    ]
    n = cache.write_distribution_history("PFL", rows)
    assert n == 1


def test_write_distribution_history_with_conn(initialised_cache, mock_distribution_history_rows):
    with cache.connect(initialised_cache) as conn:
        n = cache.write_distribution_history("PFL", mock_distribution_history_rows, conn=conn)
    assert n == len(mock_distribution_history_rows)


def test_write_distribution_history_alt_field_names(initialised_cache):
    rows = [{
        "DeclaredDate": "2025-05-15",
        "ExDivDate": "2025-05-16",
        "PayDate": "2025-05-20",
        "TotDiv": 0.08, "Income": 0.06, "CapitalReturn": 0.02,
    }]
    assert cache.write_distribution_history("PFL", rows) == 1


# ---------------------------------------------------------------- _upsert_history rules
def test_upsert_history_recent_vs_older(initialised_cache):
    """Recent rows (≤90d) REPLACE; older ones IGNORE."""
    today = date.today()
    very_old = (today - timedelta(days=400)).isoformat()
    recent = (today - timedelta(days=10)).isoformat()
    # Seed older row with value 1
    cache.write_discount_history("PFL", [{"DataDate": very_old, "Data": 1.0}])
    cache.write_discount_history("PFL", [{"DataDate": recent, "Data": 2.0}])
    # Now re-write both with different values
    cache.write_discount_history("PFL", [
        {"DataDate": very_old, "Data": 999.0},  # should be IGNORED (older path)
        {"DataDate": recent, "Data": 999.0},    # should REPLACE
    ])
    df = cache.load_discount_history("PFL")
    older_val = df.loc[df["data_date"] == pd.to_datetime(very_old), "discount"].iloc[0]
    recent_val = df.loc[df["data_date"] == pd.to_datetime(recent), "discount"].iloc[0]
    assert older_val == 1.0  # IGNORE kept original
    assert recent_val == 999.0  # REPLACE applied


# ---------------------------------------------------------------- readers / fetch_meta
def test_load_latest_universe(initialised_cache, mock_universe):
    cache.write_universe(mock_universe)
    df = cache.load_latest_universe()
    assert len(df) == len(mock_universe)
    assert "ticker" in df.columns


def test_load_price_history_empty(initialised_cache):
    df = cache.load_price_history("NONEXISTENT")
    assert df.empty


def test_load_discount_history_empty(initialised_cache):
    df = cache.load_discount_history("NONEXISTENT")
    assert df.empty


def test_load_distribution_history_empty(initialised_cache):
    df = cache.load_distribution_history("NONEXISTENT")
    assert df.empty


def test_fetch_meta_row_after_write(initialised_cache, mock_price_history_rows):
    cache.write_price_history("PFL", mock_price_history_rows)
    meta = cache.fetch_meta_row("PFL", "price_history")
    assert meta is not None
    assert meta["ticker"] == "PFL"
    assert meta["last_data_date"] is not None
    assert meta["history_years"] is not None and meta["history_years"] > 0


def test_fetch_meta_row_none(initialised_cache):
    assert cache.fetch_meta_row("ZZZZ", "price_history") is None


# ---------------------------------------------------------------- refresh_ticker_deep
def test_refresh_ticker_deep_cold(initialised_cache, monkeypatch,
                                  mock_price_history_rows, mock_discount_history_rows,
                                  mock_distribution_history_rows):
    seen = {"price_period": None, "discount_period": None}

    def fake_price(t, p):
        seen["price_period"] = p
        return mock_price_history_rows

    def fake_discount(t, p):
        seen["discount_period"] = p
        return mock_discount_history_rows

    def fake_dist(t, s, e):
        return mock_distribution_history_rows

    monkeypatch.setattr(cache.ingest, "fetch_price_history", fake_price)
    monkeypatch.setattr(cache.ingest, "fetch_discount_history", fake_discount)
    monkeypatch.setattr(cache.ingest, "fetch_distribution_history", fake_dist)

    out = cache.refresh_ticker_deep("PFL")
    assert out["price_history"] == len(mock_price_history_rows)
    assert out["discount_history"] == len(mock_discount_history_rows)
    assert out["distribution_history"] == len(mock_distribution_history_rows)
    # Cold → /All for both
    assert seen["price_period"] == "All"
    assert seen["discount_period"] == "All"


def test_refresh_ticker_deep_warm_uses_increment(initialised_cache, monkeypatch,
                                                  mock_price_history_rows,
                                                  mock_discount_history_rows,
                                                  mock_distribution_history_rows):
    # Seed cache so meta exists, with last_data_date = today
    today_iso = date.today().isoformat()
    seed = [{"DataDate": today_iso, "Data": 10, "NAVData": 11, "DiscountData": -9}]
    cache.write_price_history("PFL", seed)
    cache.write_discount_history("PFL", [{"DataDate": today_iso, "Data": -8}])
    cache.write_distribution_history("PFL", [{
        "DeclaredDateDisplay": today_iso, "TotDiv": 0.08,
    }])

    seen = {"price_period": None, "discount_period": None}
    monkeypatch.setattr(cache.ingest, "fetch_price_history",
                        lambda t, p: (seen.__setitem__("price_period", p), mock_price_history_rows)[1])
    monkeypatch.setattr(cache.ingest, "fetch_discount_history",
                        lambda t, p: (seen.__setitem__("discount_period", p), mock_discount_history_rows)[1])
    monkeypatch.setattr(cache.ingest, "fetch_distribution_history",
                        lambda t, s, e: mock_distribution_history_rows)

    out = cache.refresh_ticker_deep("PFL")
    assert seen["price_period"] == "1M"      # warm + fresh → /1M
    assert seen["discount_period"] == "1Y"   # warm → /1Y
    assert "error" not in out


def test_refresh_ticker_deep_stale_uses_1y(initialised_cache, monkeypatch,
                                            mock_price_history_rows):
    """If last price data > 30 days old, refresh should escalate to /1Y."""
    stale_iso = (date.today() - timedelta(days=120)).isoformat()
    cache.write_price_history("PFL", [
        {"DataDate": stale_iso, "Data": 10, "NAVData": 11, "DiscountData": -9}
    ])
    seen = {}
    monkeypatch.setattr(cache.ingest, "fetch_price_history",
                        lambda t, p: (seen.__setitem__("p", p), [])[1])
    monkeypatch.setattr(cache.ingest, "fetch_discount_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_distribution_history",
                        lambda t, s, e: [])
    cache.refresh_ticker_deep("PFL")
    assert seen["p"] == "1Y"


def test_refresh_ticker_deep_force_full(initialised_cache, monkeypatch,
                                         mock_price_history_rows):
    cache.write_price_history("PFL", [
        {"DataDate": date.today().isoformat(), "Data": 10, "NAVData": 11, "DiscountData": -9}
    ])
    seen = {}
    monkeypatch.setattr(cache.ingest, "fetch_price_history",
                        lambda t, p: (seen.__setitem__("p", p), mock_price_history_rows)[1])
    monkeypatch.setattr(cache.ingest, "fetch_discount_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_distribution_history",
                        lambda t, s, e: [])
    cache.refresh_ticker_deep("PFL", force_full=True)
    assert seen["p"] == "All"


def test_refresh_ticker_deep_error_recorded(initialised_cache, monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError("network gone")
    monkeypatch.setattr(cache.ingest, "fetch_price_history", boom)
    out = cache.refresh_ticker_deep("PFL")
    assert "error" in out and "network gone" in out["error"]


def test_refresh_ticker_deep_with_external_conn(initialised_cache, monkeypatch):
    monkeypatch.setattr(cache.ingest, "fetch_price_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_discount_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_distribution_history", lambda t, s, e: [])
    with cache.connect(initialised_cache) as conn:
        out = cache.refresh_ticker_deep("PFL", conn=conn)
    assert out["price_history"] == 0


def test_refresh_ticker_deep_bad_meta_date(initialised_cache, monkeypatch):
    """Garbage last_data_date should be treated as ancient → /1Y."""
    cache.init_db(initialised_cache)
    with cache.connect(initialised_cache) as conn:
        conn.execute(
            "INSERT INTO fetch_meta (ticker, series_type, last_data_date, last_fetched_at) "
            "VALUES ('PFL','price_history','not-a-date', 'also-bad')"
        )
        conn.commit()
    seen = {}
    monkeypatch.setattr(cache.ingest, "fetch_price_history",
                        lambda t, p: (seen.__setitem__("p", p), [])[1])
    monkeypatch.setattr(cache.ingest, "fetch_discount_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_distribution_history",
                        lambda t, s, e: [])
    cache.refresh_ticker_deep("PFL")
    assert seen["p"] == "1Y"  # treated as 9999d old


def test_refresh_ticker_deep_distribution_weekly_skip(initialised_cache, monkeypatch):
    """If distribution last_fetched_at is fresh and today is not Friday, skip."""
    # Set fetch_meta with recent fetched_at
    cache.write_distribution_history("PFL", [{
        "DeclaredDateDisplay": date.today().isoformat(), "TotDiv": 0.08,
    }])
    calls = {"dist": 0}

    def fake_dist(t, s, e):
        calls["dist"] += 1
        return []

    monkeypatch.setattr(cache.ingest, "fetch_price_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_discount_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_distribution_history", fake_dist)

    # Mock date.today() to a non-Friday
    class _MockDate(date):
        @classmethod
        def today(cls):
            return date(2025, 6, 9)  # Monday

    monkeypatch.setattr(cache, "date", _MockDate)
    cache.refresh_ticker_deep("PFL")
    # Should skip the fetch (fresh + not Friday)
    assert calls["dist"] == 0


def test_refresh_ticker_deep_distribution_friday_fetches(initialised_cache, monkeypatch):
    """Even if data is fresh, Friday should trigger a distribution refresh."""
    cache.write_distribution_history("PFL", [{
        "DeclaredDateDisplay": date.today().isoformat(), "TotDiv": 0.08,
    }])
    calls = {"dist": 0}

    def fake_dist(t, s, e):
        calls["dist"] += 1
        return []

    monkeypatch.setattr(cache.ingest, "fetch_price_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_discount_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_distribution_history", fake_dist)

    class _MockDate(date):
        @classmethod
        def today(cls):
            return date(2025, 6, 13)  # Friday

    monkeypatch.setattr(cache, "date", _MockDate)
    cache.refresh_ticker_deep("PFL")
    assert calls["dist"] == 1


def test_refresh_ticker_deep_distribution_bad_last_fetched(initialised_cache, monkeypatch):
    """Garbage last_fetched_at should be treated as stale → fetch."""
    cache.write_distribution_history("PFL", [{
        "DeclaredDateDisplay": date.today().isoformat(), "TotDiv": 0.08,
    }])
    # Corrupt the meta
    with cache.connect(initialised_cache) as conn:
        conn.execute(
            "UPDATE fetch_meta SET last_fetched_at='not-a-timestamp' "
            "WHERE ticker='PFL' AND series_type='distribution_history'"
        )
        conn.commit()
    calls = {"dist": 0}
    monkeypatch.setattr(cache.ingest, "fetch_price_history", lambda t, p: [])
    monkeypatch.setattr(cache.ingest, "fetch_discount_history", lambda t, p: [])

    def fake_dist(t, s, e):
        calls["dist"] += 1
        return []

    monkeypatch.setattr(cache.ingest, "fetch_distribution_history", fake_dist)
    cache.refresh_ticker_deep("PFL")
    assert calls["dist"] == 1  # bad date → treated as 9999d stale → fetch


def test_touch_meta_skipped_when_all_dates_null(initialised_cache):
    """_touch_meta returns early if every row has a None date — covers line 474."""
    with cache.connect(initialised_cache) as conn:
        # Pass non-empty rows but with None at index 1 (data_date)
        cache._touch_meta(conn, "PFL", "price_history", [("PFL", None, 1.0, 1.0, 0.0)])
        # Should not have inserted anything
        n = conn.execute(
            "SELECT COUNT(*) FROM fetch_meta WHERE ticker='PFL'"
        ).fetchone()[0]
    assert n == 0


# ---------------------------------------------------------------- maintenance
def test_wal_checkpoint_runs_clean(initialised_cache):
    cache.wal_checkpoint()  # should not raise


def test_wal_checkpoint_swallows_errors(initialised_cache, monkeypatch):
    def bad_connect(*a, **kw):
        raise sqlite3.Error("disk gone")

    # connect is a context manager; emulate by raising on entry
    class Bad:
        def __enter__(self):
            raise sqlite3.Error("disk gone")

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(cache, "connect", lambda *a, **kw: Bad())
    cache.wal_checkpoint()  # should not raise


def test_cache_stats_empty(initialised_cache):
    s = cache.cache_stats()
    assert s["universe_snapshot"] == 0
    assert s["price_history"] == 0
    assert s["latest_snapshot"] is None
    assert s["latest_computation"] is None


def test_cache_stats_populated(initialised_cache, mock_universe, mock_price_history_rows):
    cache.write_universe(mock_universe)
    cache.write_price_history("PFL", mock_price_history_rows)
    s = cache.cache_stats()
    assert s["universe_snapshot"] == len(mock_universe)
    assert s["price_history"] == len(mock_price_history_rows)
    assert s["latest_snapshot"] == "2026-05-22"


# ---------------------------------------------------------------- news cache
class TestNewsCache:
    def test_write_and_load_roundtrip(self, initialised_cache):
        items = [
            {"title": "first", "link": "http://x/1", "published": "2025-05-01"},
            {"title": "second", "link": "http://x/2", "published": "2025-05-02"},
        ]
        n = cache.write_news("PFL", items)
        assert n == 2
        out = cache.load_news("PFL")
        assert out is not None
        assert [o["title"] for o in out] == ["first", "second"]
        assert out[0]["link"] == "http://x/1"

    def test_load_returns_none_when_no_rows(self, initialised_cache):
        assert cache.load_news("ZZZ") is None

    def test_load_returns_none_when_stale(self, initialised_cache, monkeypatch):
        cache.write_news("PFL", [{"title": "old"}])
        # Force the row's fetched_at far in the past
        from cef_screener.cache import connect
        from datetime import datetime, timedelta
        old = (datetime.utcnow() - timedelta(hours=5)).isoformat(timespec="seconds")
        with connect() as conn:
            conn.execute("UPDATE news_headlines SET fetched_at=? WHERE ticker=?",
                         (old, "PFL"))
            conn.commit()
        # Stale → None
        assert cache.load_news("PFL", max_age_seconds=3600) is None
        # But still readable with a generous TTL
        assert cache.load_news("PFL", max_age_seconds=999999) is not None

    def test_load_returns_none_on_unparseable_fetched_at(self, initialised_cache):
        cache.write_news("PFL", [{"title": "x"}])
        from cef_screener.cache import connect
        with connect() as conn:
            conn.execute("UPDATE news_headlines SET fetched_at=? WHERE ticker=?",
                         ("not-a-date", "PFL"))
            conn.commit()
        assert cache.load_news("PFL") is None

    def test_write_replaces_existing(self, initialised_cache):
        cache.write_news("PFL", [{"title": "a"}, {"title": "b"}])
        cache.write_news("PFL", [{"title": "c"}])
        out = cache.load_news("PFL")
        assert [o["title"] for o in out] == ["c"]

    def test_write_filters_empty_titles(self, initialised_cache):
        n = cache.write_news("PFL", [
            {"title": ""},
            {"title": "   "},
            {"title": "ok"},
            {},
        ])
        assert n == 1
        out = cache.load_news("PFL")
        assert [o["title"] for o in out] == ["ok"]

    def test_write_with_all_empty_titles_writes_nothing(self, initialised_cache):
        n = cache.write_news("PFL", [{"title": ""}, {}])
        assert n == 0
        assert cache.load_news("PFL") is None

    def test_write_empty_ticker_returns_zero(self, initialised_cache):
        assert cache.write_news("", [{"title": "x"}]) == 0
        assert cache.write_news("   ", [{"title": "x"}]) == 0

    def test_load_empty_ticker_returns_none(self, initialised_cache):
        assert cache.load_news("") is None
        assert cache.load_news("   ") is None

    def test_ensure_news_table_idempotent(self, initialised_cache):
        # Calling _ensure_news_table multiple times should not fail
        from cef_screener.cache import connect
        with connect() as conn:
            cache._ensure_news_table(conn)
            cache._ensure_news_table(conn)
            cache._ensure_news_table(conn)
        # And we can still write/read
        assert cache.write_news("PFL", [{"title": "z"}]) == 1

    def test_ticker_normalised_to_uppercase(self, initialised_cache):
        cache.write_news(" pfl ", [{"title": "ok"}])
        out = cache.load_news("PFL")
        assert out is not None and out[0]["title"] == "ok"


# ---------------------------------------------------------------- historical scores
class TestHistoricalScores:
    def _scored_frame(self, *rows):
        return pd.DataFrame(rows)

    def test_write_and_load_roundtrip(self, initialised_cache):
        df = self._scored_frame(
            {"ticker": "PFL", "composite": 72.5, "s_disc": 80.0,
             "s_res": 60.0, "s_sust": 70.0, "s_peer": 65.0,
             "multiplier": 1.0, "buy_label": "BUY-B", "trap_tier": "ok"},
            {"ticker": "STEW", "composite": 80.0, "s_disc": 90.0,
             "s_res": 75.0, "s_sust": 70.0, "s_peer": 80.0,
             "multiplier": 1.05, "buy_label": "BUY-A", "trap_tier": None},
        )
        n = cache.persist_historical_scores(df, "2026-05-23")
        assert n == 2
        out = cache.load_historical_scores("PFL")
        assert not out.empty
        assert list(out["snapshot_date"]) == ["2026-05-23"]
        assert float(out.iloc[0]["composite"]) == pytest.approx(72.5)
        assert out.iloc[0]["buy_label"] == "BUY-B"

    def test_load_returns_empty_when_no_rows(self, initialised_cache):
        out = cache.load_historical_scores("ZZZ")
        assert out.empty

    def test_load_returns_empty_for_empty_ticker(self, initialised_cache):
        assert cache.load_historical_scores("").empty
        assert cache.load_historical_scores("   ").empty

    def test_load_sorted_ascending_by_date(self, initialised_cache):
        for d, c in [("2026-05-01", 50.0), ("2026-05-10", 55.0),
                     ("2026-05-05", 52.0)]:
            cache.persist_historical_scores(
                self._scored_frame({"ticker": "PFL", "composite": c}), d)
        out = cache.load_historical_scores("PFL")
        assert list(out["snapshot_date"]) == [
            "2026-05-01", "2026-05-05", "2026-05-10"]

    def test_persist_empty_df_returns_zero(self, initialised_cache):
        assert cache.persist_historical_scores(pd.DataFrame(), "2026-05-23") == 0

    def test_persist_none_df_returns_zero(self, initialised_cache):
        assert cache.persist_historical_scores(None, "2026-05-23") == 0

    def test_persist_blank_snapshot_returns_zero(self, initialised_cache):
        df = self._scored_frame({"ticker": "PFL", "composite": 50.0})
        assert cache.persist_historical_scores(df, "") == 0
        assert cache.persist_historical_scores(df, "   ") == 0
        assert cache.persist_historical_scores(df, None) == 0

    def test_persist_skips_blank_ticker(self, initialised_cache):
        df = self._scored_frame(
            {"ticker": "", "composite": 50.0},
            {"ticker": None, "composite": 51.0},
        )
        assert cache.persist_historical_scores(df, "2026-05-23") == 0

    def test_persist_normalises_ticker_to_uppercase(self, initialised_cache):
        df = self._scored_frame({"ticker": " pfl ", "composite": 60.0})
        cache.persist_historical_scores(df, "2026-05-23")
        assert not cache.load_historical_scores("PFL").empty

    def test_insert_or_replace_on_same_date(self, initialised_cache):
        df1 = self._scored_frame({"ticker": "PFL", "composite": 50.0,
                                   "buy_label": "AVOID"})
        cache.persist_historical_scores(df1, "2026-05-23")
        df2 = self._scored_frame({"ticker": "PFL", "composite": 80.0,
                                   "buy_label": "BUY-A"})
        cache.persist_historical_scores(df2, "2026-05-23")
        out = cache.load_historical_scores("PFL")
        assert len(out) == 1
        assert float(out.iloc[0]["composite"]) == pytest.approx(80.0)
        assert out.iloc[0]["buy_label"] == "BUY-A"

    def test_persist_handles_missing_optional_columns(self, initialised_cache):
        df = self._scored_frame({"ticker": "PFL", "composite": 60.0})
        cache.persist_historical_scores(df, "2026-05-23")
        out = cache.load_historical_scores("PFL")
        assert out.iloc[0]["s_disc"] is None
        assert out.iloc[0]["buy_label"] is None

    def test_ensure_historical_scores_table_idempotent(self, initialised_cache):
        with cache.connect(initialised_cache) as conn:
            cache._ensure_historical_scores_table(conn)
            cache._ensure_historical_scores_table(conn)
        df = self._scored_frame({"ticker": "PFL", "composite": 70.0})
        assert cache.persist_historical_scores(df, "2026-05-23") == 1



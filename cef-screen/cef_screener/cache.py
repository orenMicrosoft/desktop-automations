"""SQLite cache: schema, upserts, incremental refresh orchestration.

The cache is the *single source of historical truth* — once a ticker's All
backfill is written, subsequent runs only fetch the recent delta. Schema and
upsert rules follow plan §4.1 / §4.2.

Engine modules (metrics, scoring) never call this — they get a hydrated
DataFrame from ``load_*`` helpers and stay pure.
"""
from __future__ import annotations

import logging
import sqlite3
from collections.abc import Iterable, Sequence
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from . import config, ingest

log = logging.getLogger(__name__)

SCHEMA_VERSION = 4  # bumps with every breaking schema change

# =====================================================================
# Schema (plan §4.1)
# =====================================================================
_SCHEMA_SQL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS universe_snapshot (
    snapshot_date              TEXT NOT NULL,
    ticker                     TEXT NOT NULL,
    run_timestamp              TEXT NOT NULL,
    category_name_at_snapshot  TEXT,
    name                       TEXT,
    sponsor_name               TEXT,
    category_name              TEXT,
    price                      REAL,
    nav                        REAL,
    discount                   REAL,
    distribution_rate_price    REAL,
    distribution_rate_nav      REAL,
    return_on_nav              REAL,
    yr1_ret_on_nav             REAL,
    yr3_ret_on_nav             REAL,
    yr5_ret_on_nav             REAL,
    z_score_1yr                REAL,
    z_score_3m                 REAL,
    z_score_6m                 REAL,
    discount_52wk_avg          REAL,
    unii_per_share             REAL,
    eps                        REAL,
    current_distribution       REAL,
    distribution_frequency     TEXT,
    leverage_ratio             REAL,
    is_leveraged               INTEGER,
    market_cap_usd_m           REAL,
    avg_daily_volume           REAL,
    expense_ratio              REAL,
    nav_ticker                 TEXT,
    is_managed_distribution    INTEGER,
    PRIMARY KEY (snapshot_date, ticker)
);

CREATE INDEX IF NOT EXISTS idx_universe_ticker ON universe_snapshot(ticker);

CREATE TABLE IF NOT EXISTS price_history (
    ticker     TEXT NOT NULL,
    data_date  TEXT NOT NULL,
    price      REAL,
    nav        REAL,
    discount   REAL,
    PRIMARY KEY (ticker, data_date)
);

CREATE TABLE IF NOT EXISTS discount_history (
    ticker     TEXT NOT NULL,
    data_date  TEXT NOT NULL,
    discount   REAL,
    PRIMARY KEY (ticker, data_date)
);

CREATE TABLE IF NOT EXISTS distribution_history (
    ticker         TEXT NOT NULL,
    declared_date  TEXT NOT NULL,
    ex_date        TEXT,
    pay_date       TEXT,
    tot_div        REAL,
    income         REAL,
    capital_return REAL,
    capital_lt     REAL,
    capital_st     REAL,
    special        REAL,
    PRIMARY KEY (ticker, declared_date)
);

CREATE TABLE IF NOT EXISTS computed_metrics (
    computation_date          TEXT NOT NULL,
    ticker                    TEXT NOT NULL,
    s_disc                    REAL,
    s_res                     REAL,
    s_sust                    REAL,
    s_peer                    REAL,
    composite                 REAL,
    penalty_total_severity    REAL,
    penalty_mult              REAL,
    roc_pct_12m               REAL,
    roc_pct_36m               REAL,
    dist_cuts_5y              INTEGER,
    dist_cagr_5y              REAL,
    leverage_tier             TEXT,
    quality_flags             TEXT,
    PRIMARY KEY (computation_date, ticker)
);

CREATE TABLE IF NOT EXISTS fetch_meta (
    ticker            TEXT NOT NULL,
    series_type       TEXT NOT NULL,
    last_data_date    TEXT,
    last_fetched_at   TEXT,
    full_backfill_at  TEXT,
    history_years     REAL,
    PRIMARY KEY (ticker, series_type)
);

CREATE TABLE IF NOT EXISTS news_headlines (
    ticker       TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    idx          INTEGER NOT NULL,
    title        TEXT NOT NULL,
    link         TEXT,
    published    TEXT,
    summary      TEXT,
    PRIMARY KEY (ticker, idx)
);

CREATE TABLE IF NOT EXISTS historical_scores (
    ticker          TEXT NOT NULL,
    snapshot_date   TEXT NOT NULL,
    composite       REAL,
    s_disc          REAL,
    s_res           REAL,
    s_sust          REAL,
    s_peer          REAL,
    multiplier      REAL,
    buy_label       TEXT,
    trap_tier       TEXT,
    PRIMARY KEY (ticker, snapshot_date)
);

CREATE TABLE IF NOT EXISTS schema_version (v INTEGER NOT NULL);
"""


def init_db(db_path: Path | None = None) -> Path:
    """Create the cache file + schema if missing. Idempotent."""
    p = db_path or config.cache_db_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with connect(p) as conn:
        cur = conn.cursor()
        for stmt in _SCHEMA_SQL.strip().split(";\n"):
            if stmt.strip():
                cur.execute(stmt)
        row = cur.execute("SELECT v FROM schema_version LIMIT 1").fetchone()
        if row is None:
            cur.execute("INSERT INTO schema_version(v) VALUES (?)", (SCHEMA_VERSION,))
        conn.commit()
    return p


@contextmanager
def connect(db_path: Path | None = None):
    """Yield a sqlite3 connection with BUSY timeout + WAL pragma applied."""
    p = db_path or config.cache_db_path()
    conn = sqlite3.connect(str(p), timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("PRAGMA busy_timeout = 30000")
    cur.execute("PRAGMA synchronous = NORMAL")
    try:
        yield conn
    finally:
        conn.close()


# =====================================================================
# JSON → row coercion helpers (RD-6 #10: never trust upstream types)
# =====================================================================
def _f(v: Any) -> float | None:
    if v is None or v == "" or v == "N/A":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _i(v: Any) -> int | None:
    if v is None or v == "":
        return None
    try:
        if isinstance(v, bool):
            return int(v)
        return int(v)
    except (TypeError, ValueError):
        return None


def _b(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, str):
        return 1 if v.strip().lower() in ("true", "1", "yes") else 0
    return _i(v)


def _s(v: Any) -> str | None:
    if v is None:
        return None
    return str(v).strip() or None


def _date_iso(v: Any) -> str | None:
    """Coerce a CEFConnect date-like value to ``YYYY-MM-DD`` (ISO).

    Inputs we've seen: ``2026-05-22``, ``2026-05-22T00:00:00``,
    ``2026-05-22T00:00:00Z``, ``5/22/2026``, ``05/22/2026``.
    """
    if v is None or v == "":
        return None
    s = str(v).strip()
    if not s:
        return None
    # Try ISO first
    try:
        return datetime.fromisoformat(s.replace("Z", "")).date().isoformat()
    except ValueError:
        pass
    # MM/DD/YYYY
    for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    log.debug("Could not parse date %r", v)
    return None


# =====================================================================
# Universe snapshot writer
# =====================================================================
_UNIVERSE_INSERT = """
INSERT OR REPLACE INTO universe_snapshot (
    snapshot_date, ticker, run_timestamp, category_name_at_snapshot,
    name, sponsor_name, category_name,
    price, nav, discount, distribution_rate_price, distribution_rate_nav,
    return_on_nav, yr1_ret_on_nav, yr3_ret_on_nav, yr5_ret_on_nav,
    z_score_1yr, z_score_3m, z_score_6m, discount_52wk_avg,
    unii_per_share, eps, current_distribution, distribution_frequency,
    leverage_ratio, is_leveraged, market_cap_usd_m, avg_daily_volume,
    expense_ratio, nav_ticker, is_managed_distribution
) VALUES (?,?,?,?, ?,?,?, ?,?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?,?, ?,?,?)
"""


def write_universe(universe: list[dict], conn: sqlite3.Connection | None = None) -> str:
    """Upsert one DailyPricing snapshot. Returns the snapshot_date used.

    Per RD-6 BLOCKER 2: snapshot_date comes from the API's own ``DataDate``,
    never from ``datetime.now()``. If >5% of rows are missing it, abort.
    """
    if not universe:
        raise ValueError("Empty universe — refusing to write")

    # Determine canonical snapshot_date from API.
    # DailyPricing uses `LastUpdated` (per-fund snapshot time); fall back to
    # `NAVPublished` if LastUpdated is missing. Never use system clock — that
    # corrupts weekend/UTC-midnight runs (RD-6 BLOCKER 2).
    dates = [
        _date_iso(r.get("LastUpdated") or r.get("NAVPublished") or r.get("DataDate"))
        for r in universe
    ]
    missing = sum(1 for d in dates if d is None)
    if missing > 0.05 * len(universe):
        raise RuntimeError(
            f"CEFConnect feed missing LastUpdated/NAVPublished on {missing}/{len(universe)} rows; "
            "refusing to write potentially-misdated snapshot."
        )
    # Use the mode (most common) date
    from collections import Counter
    snap_date = Counter(d for d in dates if d).most_common(1)[0][0]
    run_ts = datetime.utcnow().isoformat(timespec="seconds")

    rows = [
        (
            snap_date,
            _s(r.get("Ticker")),
            run_ts,
            _s(r.get("CategoryName")),
            _s(r.get("Name")),
            _s(r.get("SponsorName") or r.get("Sponsor") or r.get("FundFamily")),
            _s(r.get("CategoryName")),
            _f(r.get("Price")),
            _f(r.get("NAV")),
            _f(r.get("Discount")),
            _f(r.get("DistributionRatePrice")),
            _f(r.get("DistributionRateNAV")),
            _f(r.get("ReturnOnNAV")),
            # CEFConnect doesn't expose a separate Yr1RetOnNav — ReturnOnNAV
            # IS the 1Y NAV total return (verified vs the live UI labels).
            _f(r.get("Yr1RetOnNav") if r.get("Yr1RetOnNav") is not None else r.get("ReturnOnNAV")),
            _f(r.get("Yr3RetOnNav")),
            _f(r.get("Yr5RetOnNav")),
            _f(r.get("ZScore1Yr")),
            _f(r.get("ZScore3M")),
            _f(r.get("ZScore6M")),
            _f(r.get("Discount52WkAvg")),
            _f(r.get("UNIIPerShare")),
            _f(r.get("EarningsPerShare")),
            _f(r.get("CurrentDistribution")),
            _s(r.get("DistributionFrequency")),
            _f(r.get("LeverageRatioPercentage")),
            _b(r.get("IsLeveraged")),
            _f(r.get("MarketCapUSDm")),
            _f(r.get("AvgDailyVolume")),
            _f(r.get("ExpenseRatio")),
            _s(r.get("NavTicker")),
            _b(r.get("IsManagedDistribution")),
        )
        for r in universe
        if r.get("Ticker")  # skip orphan rows
    ]
    _owned = conn is None
    if _owned:
        conn = sqlite3.connect(str(config.cache_db_path()), timeout=30.0)
        conn.row_factory = sqlite3.Row
    try:
        conn.executemany(_UNIVERSE_INSERT, rows)
        conn.commit()
    finally:
        if _owned:
            conn.close()
    log.info("Wrote %d universe_snapshot rows for %s", len(rows), snap_date)
    return snap_date


# =====================================================================
# History writers
# =====================================================================
def _upsert_history(
    conn: sqlite3.Connection,
    table: str,
    ticker: str,
    rows: Iterable[tuple[str, ...]],
    columns: Sequence[str],
) -> int:
    """Apply upsert rule per RD-2 #6: recent (≤90d) REPLACE, older IGNORE."""
    if not rows:
        return 0
    cutoff = (date.today() - timedelta(days=90)).isoformat()
    recent: list[tuple] = []
    older: list[tuple] = []
    for row in rows:
        data_date = row[1] if len(row) > 1 else None  # (ticker, data_date, ...)
        if data_date and data_date >= cutoff:
            recent.append(row)
        else:
            older.append(row)
    placeholders = ",".join("?" * len(columns))
    cols = ",".join(columns)
    n = 0
    if recent:
        conn.executemany(
            f"INSERT OR REPLACE INTO {table} ({cols}) VALUES ({placeholders})", recent
        )
        n += len(recent)
    if older:
        conn.executemany(
            f"INSERT OR IGNORE INTO {table} ({cols}) VALUES ({placeholders})", older
        )
        n += len(older)
    return n


def write_price_history(ticker: str, rows: list[dict], conn: sqlite3.Connection | None = None) -> int:
    _owned = conn is None
    if _owned:
        conn = sqlite3.connect(str(config.cache_db_path()), timeout=30.0)
    try:
        normalised = [
            (
                ticker,
                _date_iso(r.get("DataDate")),
                _f(r.get("Data")),
                _f(r.get("NAVData")),
                _f(r.get("DiscountData")),
            )
            for r in rows
            if _date_iso(r.get("DataDate"))
        ]
        n = _upsert_history(
            conn, "price_history", ticker, normalised,
            columns=("ticker", "data_date", "price", "nav", "discount"),
        )
        _touch_meta(conn, ticker, "price_history", normalised)
        conn.commit()
        return n
    finally:
        if _owned:
            conn.close()


def write_discount_history(ticker: str, rows: list[dict], conn: sqlite3.Connection | None = None) -> int:
    _owned = conn is None
    if _owned:
        conn = sqlite3.connect(str(config.cache_db_path()), timeout=30.0)
    try:
        normalised = [
            (ticker, _date_iso(r.get("DataDate")), _f(r.get("Data")))
            for r in rows
            if _date_iso(r.get("DataDate"))
        ]
        n = _upsert_history(
            conn, "discount_history", ticker, normalised,
            columns=("ticker", "data_date", "discount"),
        )
        _touch_meta(conn, ticker, "discount_history", normalised)
        conn.commit()
        return n
    finally:
        if _owned:
            conn.close()


def write_distribution_history(ticker: str, rows: list[dict], conn: sqlite3.Connection | None = None) -> int:
    _owned = conn is None
    if _owned:
        conn = sqlite3.connect(str(config.cache_db_path()), timeout=30.0)
    try:
        normalised = []
        for r in rows:
            declared = _date_iso(r.get("DeclaredDateDisplay") or r.get("DeclaredDate"))
            if not declared:
                continue
            normalised.append((
                ticker,
                declared,
                _date_iso(r.get("ExDivDateDisplay") or r.get("ExDivDate")),
                _date_iso(r.get("PayDateDisplay") or r.get("PayDate")),
                _f(r.get("TotDiv")),
                _f(r.get("Income")),
                _f(r.get("CapitalReturn")),
                _f(r.get("CapitalLT")),
                _f(r.get("CapitalST")),
                _f(r.get("Special")),
            ))
        if not normalised:
            return 0
        conn.executemany(
            """INSERT OR REPLACE INTO distribution_history (
                ticker, declared_date, ex_date, pay_date, tot_div,
                income, capital_return, capital_lt, capital_st, special
            ) VALUES (?,?,?,?,?, ?,?,?,?,?)""",
            normalised,
        )
        # Touch meta with last declared_date
        latest = max(r[1] for r in normalised)
        conn.execute(
            """INSERT OR REPLACE INTO fetch_meta
               (ticker, series_type, last_data_date, last_fetched_at, full_backfill_at, history_years)
               VALUES (?, 'distribution_history', ?, ?,
                       COALESCE((SELECT full_backfill_at FROM fetch_meta WHERE ticker=? AND series_type='distribution_history'), ?),
                       (julianday(?) - julianday((SELECT MIN(declared_date) FROM distribution_history WHERE ticker=?))) / 365.25)""",
            (ticker, latest, datetime.utcnow().isoformat(timespec="seconds"),
             ticker, datetime.utcnow().isoformat(timespec="seconds"),
             latest, ticker),
        )
        conn.commit()
        return len(normalised)
    finally:
        if _owned:
            conn.close()


def _touch_meta(conn: sqlite3.Connection, ticker: str, series_type: str, rows: list[tuple]) -> None:
    """Update fetch_meta watermark + history depth from the rows just written."""
    if not rows:
        return
    dates = sorted(r[1] for r in rows if r[1])
    if not dates:
        return
    first, last = dates[0], dates[-1]
    now = datetime.utcnow().isoformat(timespec="seconds")
    conn.execute(
        f"""INSERT OR REPLACE INTO fetch_meta
            (ticker, series_type, last_data_date, last_fetched_at, full_backfill_at, history_years)
            VALUES (?, ?, ?, ?,
                    COALESCE((SELECT full_backfill_at FROM fetch_meta WHERE ticker=? AND series_type=?), ?),
                    (julianday(?) - julianday(?)) / 365.25)""",
        (ticker, series_type, last, now, ticker, series_type, now, last, first),
    )


# =====================================================================
# Readers — hydrate pandas frames for the engine
# =====================================================================
def load_latest_universe() -> pd.DataFrame:
    with connect() as conn:
        df = pd.read_sql_query(
            """SELECT * FROM universe_snapshot
               WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM universe_snapshot)""",
            conn,
        )
    return df


def last_universe_refresh_at() -> str | None:
    """Return the timestamp of the most recent successful universe write,
    or ``None`` if the table is empty. This is what the dashboard shows
    as 'last refreshed at' so users can tell when the data was last
    pulled from CEFConnect (distinct from the snapshot_date, which is
    the API's own published date for the market data)."""
    with connect() as conn:
        row = conn.execute(
            "SELECT MAX(run_timestamp) AS ts FROM universe_snapshot"
        ).fetchone()
    if row is None:    # pragma: no cover - aggregate always returns one row
        return None
    ts = row["ts"] if hasattr(row, "keys") else row[0]
    return ts if ts else None


def load_price_history(ticker: str) -> pd.DataFrame:
    with connect() as conn:
        df = pd.read_sql_query(
            """SELECT data_date, price, nav, discount
               FROM price_history WHERE ticker = ? ORDER BY data_date""",
            conn, params=(ticker,),
        )
    if not df.empty:
        df["data_date"] = pd.to_datetime(df["data_date"])
    return df


def load_discount_history(ticker: str) -> pd.DataFrame:
    with connect() as conn:
        df = pd.read_sql_query(
            """SELECT data_date, discount FROM discount_history
               WHERE ticker = ? ORDER BY data_date""",
            conn, params=(ticker,),
        )
    if not df.empty:
        df["data_date"] = pd.to_datetime(df["data_date"])
    return df


def load_distribution_history(ticker: str) -> pd.DataFrame:
    with connect() as conn:
        df = pd.read_sql_query(
            """SELECT declared_date, ex_date, pay_date, tot_div, income,
                      capital_return, capital_lt, capital_st, special
               FROM distribution_history WHERE ticker = ? ORDER BY declared_date""",
            conn, params=(ticker,),
        )
    if not df.empty:
        for col in ("declared_date", "ex_date", "pay_date"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def fetch_meta_row(ticker: str, series_type: str) -> dict | None:
    with connect() as conn:
        row = conn.execute(
            "SELECT * FROM fetch_meta WHERE ticker=? AND series_type=?",
            (ticker, series_type),
        ).fetchone()
    return dict(row) if row else None


# =====================================================================
# News headlines (plan §11 — Phase 2 visibility)
# =====================================================================
_NEWS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS news_headlines (
    ticker       TEXT NOT NULL,
    fetched_at   TEXT NOT NULL,
    idx          INTEGER NOT NULL,
    title        TEXT NOT NULL,
    link         TEXT,
    published    TEXT,
    summary      TEXT,
    PRIMARY KEY (ticker, idx)
)
"""


def _ensure_news_table(conn: sqlite3.Connection) -> None:
    """Idempotent: create news_headlines if missing, add summary column
    on legacy DBs (schema upgrade without bumping SCHEMA_VERSION)."""
    conn.execute(_NEWS_TABLE_DDL)
    cols = {row["name"] for row in conn.execute(
        "PRAGMA table_info(news_headlines)").fetchall()}
    if "summary" not in cols:
        conn.execute("ALTER TABLE news_headlines ADD COLUMN summary TEXT")


def load_news(ticker: str, *, max_age_seconds: int = 3600) -> list[dict] | None:
    """Return cached headlines or ``None`` if missing/stale.

    Stale = newest ``fetched_at`` older than ``max_age_seconds``.
    """
    ticker_u = (ticker or "").strip().upper()
    if not ticker_u:
        return None
    with connect() as conn:
        _ensure_news_table(conn)
        rows = conn.execute(
            "SELECT fetched_at, title, link, published, summary "
            "FROM news_headlines "
            "WHERE ticker = ? ORDER BY idx",
            (ticker_u,),
        ).fetchall()
    if not rows:
        return None
    fetched = rows[0]["fetched_at"]
    try:
        fetched_dt = datetime.fromisoformat(fetched)
    except (TypeError, ValueError):
        return None
    age = (datetime.utcnow() - fetched_dt).total_seconds()
    if age > max_age_seconds:
        return None
    return [{"title": r["title"], "link": r["link"],
             "published": r["published"],
             "summary": r["summary"] or ""}
            for r in rows]


def write_news(ticker: str, items: list[dict]) -> int:
    """Replace cached headlines for ``ticker`` with the provided list."""
    ticker_u = (ticker or "").strip().upper()
    if not ticker_u:
        return 0
    now_iso = datetime.utcnow().isoformat(timespec="seconds")
    with connect() as conn:
        _ensure_news_table(conn)
        conn.execute("DELETE FROM news_headlines WHERE ticker = ?", (ticker_u,))
        rows = [
            (ticker_u, now_iso, i,
             (it.get("title") or "").strip(),
             (it.get("link") or "").strip() or None,
             (it.get("published") or "").strip() or None,
             (it.get("summary") or "").strip() or None)
            for i, it in enumerate(items)
            if (it.get("title") or "").strip()
        ]
        if rows:
            conn.executemany(
                "INSERT INTO news_headlines "
                "(ticker, fetched_at, idx, title, link, published, summary) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)", rows,
            )
        conn.commit()
    return len(rows)


# =====================================================================
# Historical scores (Phase 2 — score-drift over time)
# =====================================================================
_HISTORICAL_SCORES_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS historical_scores (
    ticker          TEXT NOT NULL,
    snapshot_date   TEXT NOT NULL,
    composite       REAL,
    s_disc          REAL,
    s_res           REAL,
    s_sust          REAL,
    s_peer          REAL,
    multiplier      REAL,
    buy_label       TEXT,
    trap_tier       TEXT,
    PRIMARY KEY (ticker, snapshot_date)
);
"""

_HISTORICAL_SCORES_COLS = (
    "ticker", "composite", "s_disc", "s_res", "s_sust", "s_peer",
    "multiplier", "buy_label", "trap_tier",
)


def _ensure_historical_scores_table(conn: sqlite3.Connection) -> None:
    """Idempotent: create historical_scores if a legacy DB is missing it."""
    conn.execute(_HISTORICAL_SCORES_TABLE_DDL)


def persist_historical_scores(scored: pd.DataFrame,
                              snapshot_date: str | None) -> int:
    """Append (or replace) one row per ticker for this snapshot date.

    Silently no-ops if ``scored`` is empty or ``snapshot_date`` is falsy
    (so engine.run_pipeline can call it unconditionally).
    Returns the number of rows written.
    """
    if scored is None or scored.empty or not snapshot_date:
        return 0
    snap = str(snapshot_date).strip()
    if not snap:
        return 0
    rows: list[tuple] = []
    for _, r in scored.iterrows():
        ticker = str(r.get("ticker") or "").strip().upper()
        if not ticker:
            continue
        rows.append((
            ticker, snap,
            _f(r.get("composite")),
            _f(r.get("s_disc")),
            _f(r.get("s_res")),
            _f(r.get("s_sust")),
            _f(r.get("s_peer")),
            _f(r.get("multiplier")),
            (str(r.get("buy_label")) if r.get("buy_label") is not None else None),
            (str(r.get("trap_tier")) if r.get("trap_tier") is not None else None),
        ))
    if not rows:
        return 0
    with connect() as conn:
        _ensure_historical_scores_table(conn)
        conn.executemany(
            "INSERT OR REPLACE INTO historical_scores "
            "(ticker, snapshot_date, composite, s_disc, s_res, s_sust, "
            "s_peer, multiplier, buy_label, trap_tier) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        conn.commit()
    return len(rows)


def load_historical_scores(ticker: str) -> pd.DataFrame:
    """Return all stored snapshots for ``ticker`` sorted ascending by date."""
    ticker_u = (ticker or "").strip().upper()
    if not ticker_u:
        return pd.DataFrame()
    with connect() as conn:
        _ensure_historical_scores_table(conn)
        rows = conn.execute(
            "SELECT snapshot_date, composite, s_disc, s_res, s_sust, "
            "s_peer, multiplier, buy_label, trap_tier "
            "FROM historical_scores WHERE ticker = ? "
            "ORDER BY snapshot_date ASC",
            (ticker_u,),
        ).fetchall()
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


# =====================================================================
# Incremental refresh orchestrator (plan §4.2)
# =====================================================================
def refresh_ticker_deep(
    ticker: str,
    conn: sqlite3.Connection | None = None,
    force_full: bool = False,
) -> dict:
    """Refresh price + discount + distribution history for one ticker.

    Cold (no fetch_meta) → /All backfill for price+discount, /5Y for distributions.
    Warm + last < 30d ago → /1M increment for price; /1Y for discount; weekly
    refresh for distributions.

    Returns a dict of counts per series.
    """
    out: dict[str, int] = {}
    _owned = conn is None
    if _owned:
        conn = sqlite3.connect(str(config.cache_db_path()), timeout=30.0)
    try:
        # ---- Price history ----
        meta = fetch_meta_row(ticker, "price_history")
        if force_full or meta is None:
            rows = ingest.fetch_price_history(ticker, "All")
        else:
            last = meta.get("last_data_date") or ""
            try:
                days_old = (date.today() - date.fromisoformat(last)).days
            except (ValueError, TypeError):
                days_old = 9999
            period = "1Y" if days_old > config.PRICE_HISTORY_INCREMENTAL_DAYS else "1M"
            rows = ingest.fetch_price_history(ticker, period)
        out["price_history"] = write_price_history(ticker, rows, conn=conn)

        # ---- Discount history ----
        meta = fetch_meta_row(ticker, "discount_history")
        period = "All" if (force_full or meta is None) else "1Y"
        rows = ingest.fetch_discount_history(ticker, period)
        out["discount_history"] = write_discount_history(ticker, rows, conn=conn)

        # ---- Distribution history (weekly only) ----
        meta = fetch_meta_row(ticker, "distribution_history")
        today = date.today()
        if force_full or meta is None:
            start = today.replace(year=today.year - config.COLD_BACKFILL_HISTORY_YEARS)
            rows = ingest.fetch_distribution_history(ticker, start, today)
        else:
            last_fetched = meta.get("last_fetched_at") or ""
            try:
                last_dt = datetime.fromisoformat(last_fetched).date()
                days_stale = (today - last_dt).days
            except (ValueError, TypeError):
                days_stale = 9999
            if days_stale >= config.DISTRIBUTION_REFRESH_DAYS or today.weekday() == 4:
                start = today - timedelta(days=90)
                rows = ingest.fetch_distribution_history(ticker, start, today)
            else:
                rows = []  # skip — distributions don't change daily
        out["distribution_history"] = write_distribution_history(ticker, rows, conn=conn)
    except Exception as e:  # noqa: BLE001
        log.warning("refresh_ticker_deep(%s) failed: %r", ticker, e)
        out["error"] = str(e)
    finally:
        if _owned:
            conn.close()
    return out


# =====================================================================
# Maintenance
# =====================================================================
def wal_checkpoint() -> None:
    """Try to compact the WAL. Non-fatal if it fails."""
    try:
        with connect() as conn:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    except sqlite3.Error as e:
        log.warning("wal_checkpoint failed (non-fatal): %r", e)


def cache_stats() -> dict:
    """Return row counts per table + freshness banner data."""
    with connect() as conn:
        out: dict[str, Any] = {}
        for tbl in ("universe_snapshot", "price_history", "discount_history",
                    "distribution_history", "computed_metrics", "fetch_meta"):
            cur = conn.execute(f"SELECT COUNT(*) FROM {tbl}")
            out[tbl] = cur.fetchone()[0]
        cur = conn.execute("SELECT MAX(snapshot_date) FROM universe_snapshot")
        out["latest_snapshot"] = cur.fetchone()[0]
        cur = conn.execute("SELECT MAX(computation_date) FROM computed_metrics")
        out["latest_computation"] = cur.fetchone()[0]
    return out

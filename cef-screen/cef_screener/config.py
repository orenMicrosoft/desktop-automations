"""Centralised configuration: thresholds, weights, paths, benchmark map.

Everything in this module is intentionally a *prior* — equal-weight factor blend,
penalty base 0.75, sell triggers per the plan §5.7.2. Override via env vars or
the CONFIG tab in the web app.
"""
from __future__ import annotations

import os
from pathlib import Path

# Patchable for tests: avoid mutating the real `os.name`, which would
# break pytest's path machinery on Windows.
_OS_NAME = os.name


# =====================================================================
# CACHE LOCATION (RD-6 BLOCKER 1: never under user home — OneDrive eats WAL)
# =====================================================================
def cache_dir() -> Path:
    override = os.environ.get("CEF_SCREENER_CACHE_DIR")
    if override:
        p = Path(override)
    elif _OS_NAME == "nt":
        base = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        p = Path(base) / "cef_screener"
    else:
        base = os.environ.get("XDG_DATA_HOME") or str(Path.home() / ".local" / "share")
        p = Path(base) / "cef_screener"
    p.mkdir(parents=True, exist_ok=True)
    return p


def cache_db_path() -> Path:
    return cache_dir() / "cache.sqlite"


def positions_path() -> Path:
    """positions.json sits next to the cache (so it's never sync-corrupted)."""
    return cache_dir() / "positions.json"


def sell_log_path() -> Path:
    return cache_dir() / "sell_log.csv"


def exclusions_path() -> Path:
    return cache_dir() / "exclusions.yaml"


def lock_path() -> Path:
    return cache_dir() / "run.lock"


# =====================================================================
# HTTP CLIENT
# =====================================================================
CEFCONNECT_BASE = "https://www.cefconnect.com/api/v3"
HTTP_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)
HTTP_TIMEOUT_SEC = 25
HTTP_RETRY_TOTAL = 3
HTTP_RETRY_BACKOFF = 0.5  # exponential: 0.5, 1.0, 2.0


# =====================================================================
# GATEKEEPER + LIQUIDITY
# =====================================================================
GATEKEEPER_SIZE = 30                 # plan §5.0: top-30 cheapest by Z1Y
LIQUIDITY_MIN_MARKET_CAP_USDM = 10.0  # $10M
LIQUIDITY_MIN_AVG_DAILY_VOL = 10_000  # 10k shares/day


# =====================================================================
# DRAWDOWN WINDOWS (plan §5.2)
# =====================================================================
DRAWDOWN_WINDOWS = [
    ("2020-02-19", "2020-04-30"),  # COVID
    ("2022-01-03", "2022-12-31"),  # Rate selloff
]
DD_2008_WINDOW = ("2007-10-01", "2009-06-30")  # display-only credit


# =====================================================================
# SCORING WEIGHTS + PENALTY (plan §5.5, §5.1, §5.3)
# =====================================================================
# Composite — equal-weight prior per prompt mandate
COMPOSITE_FACTOR_WEIGHTS = {
    "s_disc": 0.25,
    "s_res":  0.25,
    "s_sust": 0.25,
    "s_peer": 0.25,
}
PENALTY_BASE = 0.75   # multiplier = base ** total_severity ; total_severity in [0..8]

# Statistical-discount sub-score weights (§5.1)
SDISC_Z_WEIGHT      = 0.55   # tactical Z
SDISC_ABS_WEIGHT    = 0.20   # absolute current discount
SDISC_STRUCT_WEIGHT = 0.25   # structural (5Y median) discount

# Sustainability sub-score weights when composition data is FULL (§5.3)
SUST_WEIGHTS_FULL = {
    "roc":          0.20,
    "cuts":         0.11,
    "growth":       0.08,
    "coverage":     0.22,
    "nav":          0.16,
    "unii":         0.08,
    "crisis_maint": 0.15,
}
# Fallback weights when distribution composition is sparse
SUST_WEIGHTS_FALLBACK = {
    "coverage": 0.40,
    "nav":      0.30,
    "unii":     0.30,
}

# Resilience leverage multiplier (§5.2)
LEV_MULT_COEF = 0.5
LEV_MULT_CAP  = 1.40
UNIVERSE_MEDIAN_LEV_PCT = 29.0  # for funds with null leverage (RD-7)

# Sustainability scoring scales
COVERAGE_SCALE  = 0.5
NAV_CAGR_SCALE  = 0.05
UNII_SCALE      = 0.5


# =====================================================================
# SELL TRIGGERS (plan §5.7.2)
# =====================================================================
SELL_TARGET_GAIN_PCT          = 0.10    # +10% from cost basis → TARGET-HIT
SELL_TARGET_FULL_EXIT_Z       = 1.0     # +10% AND z1≥this → escalate to SELL-NOW
SELL_Z1_MEAN_REVERT           = 1.5
SELL_Z3_MEAN_REVERT_CONFIRM   = 1.0
SELL_Z1_HARD                  = 2.0
SELL_Z1_MEAN_REVERT_HILEV     = 1.2     # Extreme-leverage funds
SELL_Z3_MEAN_REVERT_HILEV     = 0.8
SELL_STOP_LOSS_PCT            = -0.20
SELL_DIST_CUT_PCT             = 0.15
SELL_LEVERAGE_JUMP_PCT        = 0.10
HOLD_STILL_CHEAP_Z1           = -1.0
SELL_OVERDUE_DAYS             = 30


# =====================================================================
# BUY TIERS (plan §5.6) — composite-score thresholds
# =====================================================================
BUY_TIER_A_MIN = 75.0   # high-conviction BUY
BUY_TIER_B_MIN = 60.0   # review/watch


# =====================================================================
# BENCHMARK 3Y CAGR ESTIMATES (Phase-1 static; Phase-2 will live-fetch)
# Keyed by benchmark ETF ticker. Approximate trailing-3Y annualised TR
# as of plan v3.1 — used solely to gate peer-penalty + trap logic.
# =====================================================================
BENCHMARK_CAGR_3Y: dict[str, float] = {
    "AGG":  0.005,   # core US bonds
    "LQD":  0.012,   # IG corp
    "HYG":  0.045,   # HY corp
    "MUB":  0.000,   # muni
    "EMB":  0.020,   # EM bond
    "BKLN": 0.060,   # bank loans
    "PFF":  0.025,   # preferreds
    "VNQ":  0.020,   # REITs
    "AMLP": 0.150,   # MLP
    "VTI":  0.085,   # broad US equity
    "EFA":  0.060,   # int'l developed
    "EEM":  0.020,   # EM
}


# =====================================================================
# LEVERAGE TIERS (plan §2b-LEVERAGE)
# =====================================================================
def leverage_tier(lev_pct: float | None) -> str:
    """Return None/Low/Moderate/High/Extreme/Unknown."""
    if lev_pct is None:
        return "Unknown"
    if lev_pct <= 0.5:
        return "None"
    if lev_pct <= 15:
        return "Low"
    if lev_pct <= 30:
        return "Moderate"
    if lev_pct <= 45:
        return "High"
    return "Extreme"


# =====================================================================
# CATEGORY → BENCHMARK ETF MAP (plan §5.6)
# Substring match, evaluated in order; first match wins.
# =====================================================================
BENCHMARK_MAP: list[tuple[str, str]] = [
    # Equity buckets first (more specific)
    ("Covered Call",          "XYLD"),
    ("Option Strategy",       "XYLD"),
    ("Buy-Write",             "XYLD"),
    ("MLP",                   "MLPX"),  # MLPX preferred to AMLP (no C-Corp drag)
    ("Energy Infrastructure", "MLPX"),
    ("Real Estate",           "VNQ"),
    ("REIT",                  "VNQ"),
    ("Utility",               "XLU"),
    ("Infrastructure",        "XLU"),
    ("Convertible",           "CWB"),
    ("BDC",                   "BIZD"),
    ("Business Development",  "BIZD"),
    ("Small Cap",             "IJR"),
    ("Mid Cap",               "IJH"),
    ("US Equity",             "SPY"),
    ("Large Cap",             "SPY"),
    ("Core Equity",           "SPY"),
    ("Global Equity",         "ACWI"),
    ("International Equity",  "ACWI"),
    ("World",                 "ACWI"),

    # Fixed-income buckets
    ("High Yield",            "HYG"),
    ("Senior Loan",           "BKLN"),
    ("Floating Rate",         "BKLN"),
    ("Bank Loan",             "BKLN"),
    ("National Muni",         "MUB"),
    ("Single-State",          "MUB"),
    ("Muni",                  "MUB"),
    ("Municipal",             "MUB"),
    ("Preferred",             "PFF"),
    ("Emerging Markets",      "EMB"),
    ("EM Debt",               "EMB"),
    ("Emerging Bond",         "EMB"),
    ("Taxable Bond",          "AGG"),
    ("Multi-Sector",          "AGG"),
    ("Investment Grade",      "AGG"),
]


def benchmark_for(category: str | None) -> str:
    """Return the benchmark ETF for a CEFConnect CategoryName.

    Fallback: SPY for equity-flavoured, AGG for bond-flavoured (string check).
    """
    if not category:
        return "SPY"
    for needle, etf in BENCHMARK_MAP:
        if needle.lower() in category.lower():
            return etf
    bond_words = ("bond", "income", "loan", "muni", "yield", "fixed", "credit")
    if any(w in category.lower() for w in bond_words):
        return "AGG"
    return "SPY"


# Fixed-income kinds for NII-vs-EPS coverage choice (RD-7)
FIXED_INCOME_CATEGORY_TOKENS = (
    "bond", "muni", "municipal", "preferred", "high yield",
    "senior loan", "loan", "em debt", "convertible", "income",
)


def is_fixed_income(category: str | None) -> bool:
    if not category:
        return False
    cat = category.lower()
    return any(t in cat for t in FIXED_INCOME_CATEGORY_TOKENS)


# =====================================================================
# WEB SERVER
# =====================================================================
WEB_DEFAULT_PORT = 8100
WEB_DEFAULT_HOST = "127.0.0.1"


# =====================================================================
# FRESHNESS / CACHE TTL
# =====================================================================
SNAPSHOT_TTL_HOURS              = 12     # DailyPricing snapshot
DISTRIBUTION_REFRESH_DAYS       = 7      # weekly cadence (or Friday)
PRICE_HISTORY_INCREMENTAL_DAYS  = 30     # use /1M; older gap → /1Y
COLD_BACKFILL_HISTORY_YEARS     = 5      # distributionhistory cold range

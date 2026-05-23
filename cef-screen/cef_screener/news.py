"""Yahoo Finance RSS news headlines for a ticker.

Tiny, dependency-free wrapper around the Yahoo Finance headline RSS feed:

    https://feeds.finance.yahoo.com/rss/2.0/headline?s={ticker}&region=US&lang=en-US

Headlines are cached in SQLite (``cache.write_news`` / ``cache.load_news``)
with a 1-hour TTL so we never hit Yahoo more than once an hour per ticker.
"""
from __future__ import annotations

import logging
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

from . import cache

log = logging.getLogger(__name__)

YAHOO_RSS_URL_TEMPLATE = (
    "https://feeds.finance.yahoo.com/rss/2.0/headline"
    "?s={ticker}&region=US&lang=en-US"
)
TIMEOUT_SECONDS = 5
CACHE_TTL_SECONDS = 60 * 60  # 1 hour
DEFAULT_MAX_ITEMS = 5
USER_AGENT = "CefScreener/1.0 (+https://github.com/local/cef-screener)"


def fetch_headlines(
    ticker: str,
    *,
    max_items: int = DEFAULT_MAX_ITEMS,
    force_refresh: bool = False,
) -> list[dict]:
    """Return up to ``max_items`` news headlines for ``ticker``.

    Uses the cache when available (1-hour TTL). On any network / parse
    failure returns an empty list — the caller can render that as "no news".
    """
    ticker = (ticker or "").strip().upper()
    if not ticker:
        return []
    if not force_refresh:
        cached = cache.load_news(ticker, max_age_seconds=CACHE_TTL_SECONDS)
        if cached is not None:
            return cached[:max_items]
    raw = _fetch_raw(ticker)
    if not raw:
        return []
    items = _parse_rss(raw)
    cache.write_news(ticker, items)
    return items[:max_items]


def _fetch_raw(ticker: str) -> str:
    url = YAHOO_RSS_URL_TEMPLATE.format(ticker=ticker)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as resp:
            return resp.read().decode("utf-8", errors="replace")
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        log.warning("News fetch failed for %s: %s", ticker, e)
        return ""


def _parse_rss(xml_text: str) -> list[dict]:
    """Convert RSS 2.0 XML into a list of ``{title, link, published}`` dicts."""
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        log.warning("News RSS parse failed: %s", e)
        return []
    items: list[dict] = []
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        pub = (it.findtext("pubDate") or "").strip()
        if title:
            items.append({"title": title, "link": link, "published": pub})
    return items

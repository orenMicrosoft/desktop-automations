"""Tests for cef_screener.news — Yahoo Finance RSS headline fetcher."""
from __future__ import annotations

import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from cef_screener import news, cache


# ---------------------------------------------------------------- _parse_rss
class TestParseRss:
    def test_well_formed_returns_dicts(self):
        xml = """<?xml version='1.0'?>
        <rss version='2.0'><channel>
          <item>
            <title>PIMCO declares distribution</title>
            <link>https://example.com/a</link>
            <pubDate>Mon, 12 May 2025 12:00:00 +0000</pubDate>
          </item>
          <item>
            <title>Another headline</title>
            <link>https://example.com/b</link>
            <pubDate>Tue, 13 May 2025 12:00:00 +0000</pubDate>
          </item>
        </channel></rss>"""
        out = news._parse_rss(xml)
        assert len(out) == 2
        assert out[0]["title"] == "PIMCO declares distribution"
        assert out[0]["link"] == "https://example.com/a"
        assert "May 2025" in out[0]["published"]

    def test_malformed_returns_empty(self):
        assert news._parse_rss("<<<not xml>>>") == []

    def test_no_items_returns_empty(self):
        xml = "<rss><channel></channel></rss>"
        assert news._parse_rss(xml) == []

    def test_items_missing_title_are_dropped(self):
        xml = """<rss><channel>
          <item><link>http://x</link></item>
          <item><title>  </title><link>http://y</link></item>
          <item><title>kept</title><link>http://z</link></item>
        </channel></rss>"""
        out = news._parse_rss(xml)
        assert len(out) == 1
        assert out[0]["title"] == "kept"

    def test_missing_link_and_pub_are_blank_strings(self):
        xml = "<rss><channel><item><title>solo</title></item></channel></rss>"
        out = news._parse_rss(xml)
        assert out == [{"title": "solo", "link": "",
                        "published": "", "summary": ""}]


# ---------------------------------------------------------------- _fetch_raw
class TestFetchRaw:
    def test_success_returns_decoded_text(self):
        fake = MagicMock()
        fake.read.return_value = b"<rss>ok</rss>"
        fake.__enter__.return_value = fake
        fake.__exit__.return_value = False
        with patch("urllib.request.urlopen", return_value=fake) as op:
            out = news._fetch_raw("PFL")
        assert out == "<rss>ok</rss>"
        op.assert_called_once()
        # User-Agent header was set
        req = op.call_args[0][0]
        assert req.get_header("User-agent") == news.USER_AGENT
        # Timeout argument
        assert op.call_args[1].get("timeout") == news.TIMEOUT_SECONDS

    def test_url_error_returns_empty(self):
        with patch("urllib.request.urlopen",
                   side_effect=urllib.error.URLError("nope")):
            assert news._fetch_raw("PFL") == ""

    def test_timeout_returns_empty(self):
        with patch("urllib.request.urlopen",
                   side_effect=TimeoutError("slow")):
            assert news._fetch_raw("PFL") == ""

    def test_oserror_returns_empty(self):
        with patch("urllib.request.urlopen",
                   side_effect=OSError("dns")):
            assert news._fetch_raw("PFL") == ""


# ---------------------------------------------------------------- fetch_headlines
RSS_FIXTURE = """<rss><channel>
  <item><title>h1</title><link>http://x/1</link><pubDate>p1</pubDate></item>
  <item><title>h2</title><link>http://x/2</link><pubDate>p2</pubDate></item>
  <item><title>h3</title><link>http://x/3</link><pubDate>p3</pubDate></item>
</channel></rss>"""


class TestFetchHeadlines:
    def test_empty_ticker_returns_empty(self, initialised_cache):
        assert news.fetch_headlines("") == []
        assert news.fetch_headlines("   ") == []

    def test_cache_hit_returns_cached_without_fetch(self, initialised_cache):
        cache.write_news("PFL", [
            {"title": "cached", "link": "http://c", "published": "yesterday"},
        ])
        with patch.object(news, "_fetch_raw") as raw:
            out = news.fetch_headlines("PFL")
        raw.assert_not_called()
        assert out == [{"title": "cached", "link": "http://c",
                        "published": "yesterday", "summary": ""}]

    def test_cache_miss_fetches_and_writes(self, initialised_cache):
        with patch.object(news, "_fetch_raw", return_value=RSS_FIXTURE) as raw:
            out = news.fetch_headlines("PFL")
        raw.assert_called_once_with("PFL")
        assert len(out) == 3
        # And it persists
        cached = cache.load_news("PFL")
        assert cached is not None
        assert [c["title"] for c in cached] == ["h1", "h2", "h3"]

    def test_empty_fetch_returns_empty(self, initialised_cache):
        with patch.object(news, "_fetch_raw", return_value=""):
            assert news.fetch_headlines("PFL") == []

    def test_force_refresh_bypasses_cache(self, initialised_cache):
        cache.write_news("PFL", [
            {"title": "stale", "link": "http://old", "published": ""},
        ])
        with patch.object(news, "_fetch_raw", return_value=RSS_FIXTURE) as raw:
            out = news.fetch_headlines("PFL", force_refresh=True)
        raw.assert_called_once()
        assert [o["title"] for o in out] == ["h1", "h2", "h3"]

    def test_max_items_truncates(self, initialised_cache):
        with patch.object(news, "_fetch_raw", return_value=RSS_FIXTURE):
            out = news.fetch_headlines("PFL", max_items=2)
        assert len(out) == 2
        assert [o["title"] for o in out] == ["h1", "h2"]

    def test_ticker_is_normalised_to_uppercase(self, initialised_cache):
        with patch.object(news, "_fetch_raw", return_value=RSS_FIXTURE) as raw:
            news.fetch_headlines("  pfl  ")
        raw.assert_called_once_with("PFL")


# ---------------------------------------------------------------- _clean_summary
class TestCleanSummary:
    def test_empty(self):
        assert news._clean_summary("") == ""
        assert news._clean_summary(None) == ""

    def test_strips_html_tags(self):
        assert news._clean_summary("<p>Hello <b>world</b></p>") == "Hello world"

    def test_collapses_whitespace(self):
        assert news._clean_summary("foo\n\n   bar\tbaz") == "foo bar baz"

    def test_strips_inline_markup(self):
        raw = '<a href="x">Click</a> here <img src="y"/> end'
        out = news._clean_summary(raw)
        assert "<" not in out and ">" not in out
        assert "Click here" in out and "end" in out

    def test_truncates_long_summaries(self):
        text = "word " * 200
        out = news._clean_summary(text)
        assert len(out) <= news.MAX_SUMMARY_CHARS + 1
        assert out.endswith("…")

    def test_short_summary_not_truncated(self):
        out = news._clean_summary("Short and sweet.")
        assert out == "Short and sweet."
        assert not out.endswith("…")


# ---------------------------------------------------------------- description in RSS
class TestRssDescription:
    def test_extracts_description_as_summary(self):
        xml = ("<rss><channel><item>"
               "<title>Tender announced</title>"
               "<link>http://x</link>"
               "<description>The fund will repurchase up to 5% of shares "
               "at 98% of NAV.</description>"
               "</item></channel></rss>")
        out = news._parse_rss(xml)
        assert "summary" in out[0]
        assert "repurchase" in out[0]["summary"]

    def test_html_description_is_stripped(self):
        xml = ("<rss><channel><item><title>t</title>"
               "<description>&lt;p&gt;Hello&lt;/p&gt;</description>"
               "</item></channel></rss>")
        out = news._parse_rss(xml)
        assert out[0]["summary"] == "Hello"

    def test_missing_description_is_empty_string(self):
        xml = "<rss><channel><item><title>t</title></item></channel></rss>"
        out = news._parse_rss(xml)
        assert out[0]["summary"] == ""

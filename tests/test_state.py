from __future__ import annotations

from alpaca_news_mcp.models import NewsArticle
from alpaca_news_mcp.state import State


def _article(id: int, *, latency_ms: int | None) -> NewsArticle:
    return NewsArticle(
        id=id,
        headline="h",
        first_seen_at="2026-04-28T20:00:00+00:00",
        last_seen_at="2026-04-28T20:00:00+00:00",
        last_seen_source="ws",
        latency_ms=latency_ms,
    )


def test_update_health_clears_nullable_field_with_none():
    """Passing `last_error=None` after recovery must actually clear the field;
    previously the None filter dropped the kwarg, so once an error was recorded
    health kept reporting it indefinitely."""
    s = State(max_recent_articles=10)
    s.update_health(last_error="boom")
    assert s.stream_health.last_error == "boom"

    s.update_health(last_error=None)
    assert s.stream_health.last_error is None


def test_update_health_omitted_keys_left_unchanged():
    """Omitting a kwarg must still leave the existing value alone."""
    s = State(max_recent_articles=10)
    s.update_health(last_error="boom", connected=True)
    assert s.stream_health.last_error == "boom"
    assert s.stream_health.connected is True

    s.update_health(connected=False)
    assert s.stream_health.connected is False
    assert s.stream_health.last_error == "boom"


def test_record_article_skips_negative_latency():
    """Clock skew between Alpaca's publisher and the host can produce a
    negative latency_ms; those samples should not enter the latency window
    or they drag avg/percentile metrics toward zero."""
    s = State(max_recent_articles=10)
    s.record_article(_article(1, latency_ms=100), was_new=True)
    s.record_article(_article(2, latency_ms=-50), was_new=True)
    s.record_article(_article(3, latency_ms=200), was_new=True)
    s.record_article(_article(4, latency_ms=None), was_new=True)
    assert list(s.latency_window) == [100, 200]

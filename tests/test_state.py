from __future__ import annotations

from alpaca_news_mcp.models import NewsArticle
from alpaca_news_mcp.state import State


def _article(id: int, *, updated_at: str | None = None) -> NewsArticle:
    return NewsArticle(
        id=id,
        headline="h",
        updated_at=updated_at,
        first_seen_at="2026-04-28T20:00:00+00:00",
        last_seen_at="2026-04-28T20:00:00+00:00",
        last_seen_source="ws",
    )


def test_update_health_clears_nullable_field_with_none():
    """Passing `last_error=None` after recovery must actually clear the field;
    previously the None filter dropped the kwarg, so once an error was recorded
    health kept reporting it indefinitely."""
    s = State()
    s.update_health(last_error="boom")
    assert s.snapshot_health().last_error == "boom"

    s.update_health(last_error=None)
    assert s.snapshot_health().last_error is None


def test_update_health_omitted_keys_left_unchanged():
    """Omitting a kwarg must still leave the existing value alone."""
    s = State()
    s.update_health(last_error="boom", connected=True)
    assert s.snapshot_health().last_error == "boom"
    assert s.snapshot_health().connected is True

    s.update_health(connected=False)
    assert s.snapshot_health().connected is False
    assert s.snapshot_health().last_error == "boom"


def test_per_stream_health_is_independent():
    s = State()
    s.update_health("news", connected=True)
    s.update_health("stocks", last_error="stock boom")
    assert s.snapshot_health("news").connected is True
    assert s.snapshot_health("news").last_error is None
    assert s.snapshot_health("stocks").connected is False
    assert s.snapshot_health("stocks").last_error == "stock boom"
    assert s.snapshot_health("stocks").service == "alpaca-stock-stream"


def test_record_article_drop_per_stream():
    s = State()
    assert s.record_article_drop("news") == 1
    assert s.record_article_drop("news") == 2
    assert s.record_article_drop("stocks") == 1
    assert s.snapshot_health("news").articles_dropped == 2
    assert s.snapshot_health("stocks").articles_dropped == 1


def test_record_article_tracks_counts_and_last_seen():
    s = State()
    s.record_article(_article(1, updated_at="2026-04-28T20:00:00+00:00"), was_new=True)
    s.record_article(_article(2, updated_at="2026-04-28T21:00:00+00:00"), was_new=True)
    # Re-delivery of an older article must not regress last_seen_updated_at.
    s.record_article(_article(1, updated_at="2026-04-28T20:00:00+00:00"), was_new=False)
    assert s.article_count == 2
    assert s.last_seen_updated_at == "2026-04-28T21:00:00+00:00"
    assert s.snapshot_health().article_count == 2


def test_hot_path_health_fields_round_trip(state):
    """last_message_at / last_article_at / articles_dropped bypass the
    pydantic round trip on write but still appear in snapshots."""
    state.update_health("stocks", last_message_at="2026-07-04T00:00:00+00:00")
    state.update_health("stocks", last_article_at="2026-07-04T00:00:01+00:00")
    n = state.record_article_drop("stocks")
    assert n == 1
    snap = state.snapshot_health("stocks")
    assert snap.last_message_at == "2026-07-04T00:00:00+00:00"
    assert snap.last_article_at == "2026-07-04T00:00:01+00:00"
    assert snap.articles_dropped == 1
    # Mixed updates still work.
    state.update_health("stocks", connected=True, last_message_at="x")
    snap = state.snapshot_health("stocks")
    assert snap.connected is True and snap.last_message_at == "x"

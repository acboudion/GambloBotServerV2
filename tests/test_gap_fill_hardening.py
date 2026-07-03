"""Gap-fill retry/alerting, coverage-gap alerts, and dropped-article replay."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import orjson
import pytest

import alpaca_news_mcp.ws_base as ws_base
from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.state import State
from alpaca_news_mcp.store import Store
from alpaca_news_mcp.stream import NewsStreamWorker


@pytest.fixture
async def wired(tmp_path, monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "x.sqlite"))
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "false")
    monkeypatch.setenv("RECONNECT_MIN_SECONDS", "1")
    monkeypatch.setenv("RECONNECT_MAX_SECONDS", "1")
    cfg = Config.from_env()
    store = await Store.open(cfg.storage_path)
    await store.init_schema()
    state = State()
    alerts = AlertEngine()
    yield cfg, store, state, alerts
    await store.close()
    NewsStreamWorker.reset_singleton()


@pytest.mark.asyncio
async def test_gap_fill_retries_and_alerts(wired, monkeypatch):
    """A failing gap-fill must retry GAP_FILL_MAX_ATTEMPTS times, bump the
    health counter each failure, alert high on first failure and critical on
    exhaustion."""
    cfg, store, state, alerts = wired
    monkeypatch.setattr(ws_base, "GAP_FILL_RETRY_WAIT_SECONDS", 0.01)
    calls = 0

    async def failing_gap_fill():
        nonlocal calls
        calls += 1
        raise RuntimeError("REST is down")

    worker = NewsStreamWorker(cfg, store, state, alerts, gap_fill_callback=failing_gap_fill)
    await worker._gap_fill_with_retries()

    assert calls == 3
    assert state.snapshot_health().gap_fill_failures == 3
    stored = await store.get_alerts(minutes=5, categories=["gap_fill_failure"], limit=10)
    severities = sorted(a.severity for a in stored)
    assert severities == ["critical", "high"]
    assert any("retries exhausted" in a.reason for a in stored)


@pytest.mark.asyncio
async def test_gap_fill_success_stops_retrying(wired):
    cfg, store, state, alerts = wired
    calls = 0

    async def flaky_gap_fill():
        nonlocal calls
        calls += 1

    worker = NewsStreamWorker(cfg, store, state, alerts, gap_fill_callback=flaky_gap_fill)
    await worker._gap_fill_with_retries()
    assert calls == 1
    assert state.snapshot_health().gap_fill_failures == 0


@pytest.mark.asyncio
async def test_coverage_gap_alert_when_backfill_disabled(wired):
    """Disconnect with backfill disabled → permanent-loss window must surface
    as a coverage_gap alert."""
    cfg, store, state, alerts = wired
    assert cfg.enable_rest_backfill is False
    state.last_seen_updated_at = "2026-04-28T20:00:00+00:00"
    worker = NewsStreamWorker(cfg, store, state, alerts)

    await worker.on_session_ended(authed=True)
    stored = await store.get_alerts(minutes=5, categories=["coverage_gap"], limit=10)
    assert len(stored) == 1
    assert "2026-04-28T20:00:00+00:00" in stored[0].reason
    assert stored[0].severity == "medium"

    # Sessions that never authenticated (e.g. connect-fail loops) must not spam.
    await worker.on_session_ended(authed=False)
    stored = await store.get_alerts(minutes=5, categories=["coverage_gap"], limit=10)
    assert len(stored) == 1


@pytest.mark.asyncio
async def test_coverage_gap_not_alerted_when_backfill_enabled(wired):
    cfg, store, state, alerts = wired
    cfg2 = replace(cfg, enable_rest_backfill=True)
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    await worker.on_session_ended(authed=True)
    stored = await store.get_alerts(minutes=5, categories=["coverage_gap"], limit=10)
    assert stored == []


@pytest.mark.asyncio
async def test_dropped_articles_are_replayed(wired):
    """queue_overflow_drop raw events must be re-ingested, marked replayed,
    and counted in health."""
    cfg, store, state, alerts = wired
    article = {
        "T": "n",
        "id": 777,
        "headline": "dropped then recovered",
        "created_at": "2026-04-28T19:00:00Z",
        "updated_at": "2026-04-28T19:00:00Z",
        "symbols": ["AAPL"],
        "source": "benzinga",
    }
    await store.record_raw_event(
        endpoint="news_ws",
        message_type="queue_overflow_drop",
        raw_json=orjson.dumps(article).decode(),
    )
    # An unparseable drop must not wedge the loop.
    await store.record_raw_event(
        endpoint="news_ws",
        message_type="queue_overflow_drop",
        raw_json="not-json{{{",
    )

    worker = NewsStreamWorker(cfg, store, state, alerts)
    replayed = await worker._replay_dropped_once()
    assert replayed == 1
    assert (await store.get_article(777)) is not None
    assert state.snapshot_health().dropped_replayed == 1

    # Both events (including the bad one) are marked, so a second pass is a no-op.
    assert await worker._replay_dropped_once() == 0
    remaining = await store.unreplayed_dropped_events(limit=10)
    assert remaining == []


@pytest.mark.asyncio
async def test_replay_loop_task_lifecycle(wired):
    """start() spawns the replay loop; stop() tears it down cleanly."""
    cfg, store, state, alerts = wired
    cfg2 = replace(cfg, alpaca_news_stream_url="ws://127.0.0.1:1/nowhere")
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    await worker.start()
    assert worker._replay_task is not None
    await asyncio.sleep(0.05)
    await worker.stop()
    assert worker._replay_task is None

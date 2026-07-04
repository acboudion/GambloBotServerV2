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

    async def failing_gap_fill(watermark=None):
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

    async def flaky_gap_fill(watermark=None):
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


@pytest.mark.asyncio
async def test_gap_fill_receives_captured_watermark(wired):
    """The watermark captured before the receive loop must be handed to the
    gap-fill callback — a lazily-read watermark could be advanced by a
    post-reconnect article, silently skipping the outage window."""
    cfg, store, state, alerts = wired
    received: list = []

    async def recording_gap_fill(watermark=None):
        received.append(watermark)

    worker = NewsStreamWorker(cfg, store, state, alerts, gap_fill_callback=recording_gap_fill)
    state.last_seen_updated_at = "2026-04-28T20:00:00+00:00"
    captured = await worker.capture_gap_fill_watermark()
    # Simulate a post-reconnect article advancing the live watermark.
    state.last_seen_updated_at = "2026-04-28T21:00:00+00:00"
    await worker._gap_fill_with_retries(captured)
    assert received == ["2026-04-28T20:00:00+00:00"]


@pytest.mark.asyncio
async def test_stop_drains_queue_before_cancelling_persister(wired, monkeypatch):
    """Graceful shutdown must persist items already accepted into the queue
    instead of discarding them with the persister cancellation."""
    cfg, store, state, alerts = wired
    worker = NewsStreamWorker(cfg, store, state, alerts)

    original = worker.persist_batch

    async def slow_persist(batch):
        await asyncio.sleep(0.05)
        await original(batch)

    worker.persist_batch = slow_persist  # type: ignore[method-assign]
    # Start only the persister (no websocket) and enqueue directly.
    worker._persister_task = asyncio.create_task(worker._persister_loop())
    worker._main_task = asyncio.create_task(asyncio.sleep(3600))
    for i in range(3):
        worker._queue.put_nowait(("n", {
            "T": "n", "id": 5000 + i, "headline": f"queued {i}",
            "created_at": "2026-04-28T19:00:00Z", "updated_at": "2026-04-28T19:00:00Z",
        }))
    await worker.stop()
    for i in range(3):
        assert (await store.get_article(5000 + i)) is not None, i


# ---- bar gap-fill windowing + idempotency (stock stream) -------------------


@pytest.mark.asyncio
async def test_bar_gap_fill_uses_per_symbol_windows(tmp_path, monkeypatch):
    """One stale symbol must not drag the fetch window back for everyone:
    symbols are bucketed by their own watermark (clamped to the max
    lookback), and rows below a symbol's own watermark are dropped."""
    from datetime import UTC, datetime, timedelta

    from alpaca_news_mcp.market_store import MarketStore
    from alpaca_news_mcp.stock_stream import StockStreamWorker

    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "n.sqlite"))
    monkeypatch.setenv("MARKET_STORAGE_PATH", str(tmp_path / "m.sqlite"))
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "false")
    monkeypatch.setenv("STOCK_WATCHLIST_SYMBOLS", "AAPL,STALE")
    monkeypatch.setenv("GAP_FILL_MAX_LOOKBACK_MINUTES", "120")
    cfg = Config.from_env()
    store = await Store.open(cfg.storage_path)
    await store.init_schema()
    market_store = await MarketStore.open(cfg.market_storage_path)
    await market_store.init_schema()
    state = State()
    alerts_engine = AlertEngine()

    now = datetime.now(UTC)
    now_min = int(now.timestamp()) // 60 * 60

    calls: list[tuple[list[str], str]] = []

    class _Client:
        async def bars(self, symbols, *, start_iso, end_iso=None,
                       timeframe="1Min", limit_per_page=1000):
            calls.append((sorted(symbols), start_iso))
            fresh = (now - timedelta(minutes=2)).isoformat()
            old = (now - timedelta(minutes=115)).isoformat()
            out = {}
            for s in symbols:
                # The fake returns BOTH a fresh row and a very old row for
                # every requested symbol — per-symbol filtering must keep
                # the old row only for the symbol whose window covers it.
                out[s] = [
                    {"t": fresh, "o": 1, "h": 2, "l": 0.5, "c": 1.5,
                     "v": 100, "n": 5, "vw": 1.1},
                    {"t": old, "o": 9, "h": 9, "l": 9, "c": 9,
                     "v": 9, "n": 9, "vw": 9.0},
                ]
            return out

        async def is_market_open(self):
            return True

    worker = StockStreamWorker(
        cfg, store, market_store, state, alerts_engine, market_client=_Client()
    )
    try:
        # AAPL has a fresh watermark (5 min ago); STALE has none, so it gets
        # the 60-minute fallback — two different buckets, two REST calls.
        await worker._bar_gap_fill(watermark={"AAPL": now_min - 300})
        assert len(calls) == 2
        called_symbols = sorted(c[0][0] for c in calls)
        assert called_symbols == [["AAPL"], ["STALE"]] or called_symbols == ["AAPL", "STALE"]

        # AAPL's 115-min-old row is below its 5-min watermark -> dropped;
        # STALE's is below its 60-min fallback -> dropped too.
        rows, _, _ = await market_store.bars_since(cursor=0, limit=50)
        by_symbol: dict[str, list[int]] = {}
        for r in rows:
            by_symbol.setdefault(r["symbol"], []).append(r["ts"])
        assert set(by_symbol) == {"AAPL", "STALE"}
        for ts_list in by_symbol.values():
            assert len(ts_list) == 1  # only the fresh row survived
    finally:
        StockStreamWorker.reset_singleton()
        await market_store.close()
        await store.close()


@pytest.mark.asyncio
async def test_unchanged_bar_redelivery_keeps_seq(tmp_path):
    """Gap-fill redelivering identical bars must not bump seq — cursors
    would otherwise re-deliver the whole fetched window after every
    reconnect."""
    from alpaca_news_mcp.market_store import MarketStore

    store = await MarketStore.open(str(tmp_path / "m.sqlite"))
    await store.init_schema()
    try:
        bar = ("AAPL", "1min", 1_700_000_000, 1.0, 2.0, 0.5, 1.5, 1000, 10, 1.2)
        await store.persist_stream_batch(bars=[bar])
        rows, cursor, _ = await store.bars_since(cursor=0, limit=10)
        assert len(rows) == 1

        # Identical redelivery: invisible to the cursor.
        await store.upsert_bars([bar])
        rows, _, _ = await store.bars_since(cursor=cursor, limit=10)
        assert rows == []

        # A real correction re-surfaces with a fresh seq.
        changed = ("AAPL", "1min", 1_700_000_000, 1.0, 2.5, 0.5, 2.0, 1500, 15, 1.4)
        await store.upsert_bars([changed])
        rows, _, _ = await store.bars_since(cursor=cursor, limit=10)
        assert len(rows) == 1 and rows[0]["close"] == 2.0
    finally:
        await store.close()

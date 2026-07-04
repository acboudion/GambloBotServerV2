"""BaseStreamWorker reliability tests: watchdog, backoff reset, fatal-auth retry."""

from __future__ import annotations

import asyncio
from dataclasses import replace

import orjson
import pytest
import websockets

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.state import State
from alpaca_news_mcp.store import Store
from alpaca_news_mcp.stream import NewsStreamWorker


async def _start_fake_ws(handler):
    server = await websockets.serve(
        handler, "127.0.0.1", 0, ping_interval=None, ping_timeout=None
    )
    sock = next(iter(server.sockets))
    port = sock.getsockname()[1]
    return server, port


async def _handshake(ws, *, subscription=True):
    await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
    await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
    if subscription:
        await ws.recv()
        await ws.send(orjson.dumps([{"T": "subscription", "news": ["*"]}]).decode())


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


async def _wait_for(predicate, *, deadline_s: float = 8.0, interval: float = 0.05):
    deadline = asyncio.get_event_loop().time() + deadline_s
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return False


@pytest.mark.asyncio
async def test_idle_watchdog_forces_reconnect_and_alerts(wired):
    """A silent-but-open connection must be detected: the watchdog reconnects,
    bumps stale_reconnects, and records a stream_stale alert."""
    cfg, store, state, alerts = wired
    connections = 0

    async def handler(ws):
        nonlocal connections
        connections += 1
        await _handshake(ws)
        # ... then go silent forever (open but no messages).
        await ws.wait_closed()

    server, port = await _start_fake_ws(handler)
    cfg2 = replace(
        cfg,
        alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
        news_idle_reconnect_seconds=1,
    )
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    try:
        await worker.start()
        assert await _wait_for(lambda: connections >= 2)
        assert state.snapshot_health().stale_reconnects >= 1
        stored = await store.get_alerts(minutes=5, categories=["stream_stale"], limit=10)
        assert len(stored) >= 1
        assert "watchdog" in stored[0].reason
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_watchdog_disabled_by_default(wired):
    """With NEWS_IDLE_RECONNECT_SECONDS=0 (default) a quiet feed must NOT
    trigger reconnects."""
    cfg, store, state, alerts = wired
    connections = 0

    async def handler(ws):
        nonlocal connections
        connections += 1
        await _handshake(ws)
        await ws.wait_closed()

    server, port = await _start_fake_ws(handler)
    cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news")
    assert cfg2.news_idle_reconnect_seconds == 0
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    try:
        await worker.start()
        await asyncio.sleep(2.5)
        assert connections == 1
        assert state.snapshot_health().stale_reconnects == 0
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_backoff_resets_after_stable_session(wired):
    """After a session that lived >= stable_connection_seconds, the next
    reconnect must restart from attempt 1 (base backoff), not keep escalating."""
    cfg, store, state, alerts = wired
    connections = 0

    async def handler(ws):
        nonlocal connections
        connections += 1
        await _handshake(ws)
        # Stay connected past the stability threshold, then drop.
        await asyncio.sleep(0.7)

    server, port = await _start_fake_ws(handler)
    cfg2 = replace(
        cfg,
        alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
        stable_connection_seconds=0,  # every completed session counts as stable
        reconnect_min_seconds=1,
        reconnect_max_seconds=64,
    )
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    try:
        await worker.start()
        # Three full sessions in ~6s is only possible if each backoff stayed at
        # ~1s (attempt reset). With cumulative attempts the third wait alone
        # would be >= 2.8s (4s * 0.7 jitter floor) after two prior sessions.
        assert await _wait_for(lambda: connections >= 3, deadline_s=8.0)
        # connection_attempts in health reflects the *reset* counter.
        assert state.snapshot_health().connection_attempts <= 2
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_fatal_auth_retries_on_slow_cadence_with_alert(wired):
    """A 402 must not silently stall the worker forever: it retries on the
    AUTH_RETRY_SECONDS cadence, flags auth_failed in health, and records a
    critical alert."""
    cfg, store, state, alerts = wired
    connections = 0

    async def handler(ws):
        nonlocal connections
        connections += 1
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(
            orjson.dumps([{"T": "error", "code": 402, "msg": "auth failed"}]).decode()
        )
        # Message-auth fallback will also fail.
        try:
            await ws.recv()
            await ws.send(
                orjson.dumps([{"T": "error", "code": 402, "msg": "auth failed"}]).decode()
            )
        except websockets.exceptions.ConnectionClosed:
            return
        await asyncio.sleep(1)

    server, port = await _start_fake_ws(handler)
    cfg2 = replace(
        cfg,
        alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
        auth_retry_seconds=1,
    )
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    try:
        await worker.start()
        assert await _wait_for(lambda: state.snapshot_health().auth_failed)
        # It must RETRY (not sleep forever): expect a second connection within
        # a couple of auth_retry_seconds periods.
        assert await _wait_for(lambda: connections >= 2)
        stored = await store.get_alerts(minutes=5, categories=["stream_error"], limit=50)
        assert any("authentication failing" in a.reason for a in stored)
        assert any(a.severity == "critical" for a in stored)
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_auth_recovery_clears_auth_failed(wired):
    """Once credentials work again, auth_failed must clear."""
    cfg, store, state, alerts = wired
    connections = 0

    async def handler(ws):
        nonlocal connections
        connections += 1
        if connections == 1:
            await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
            await ws.send(
                orjson.dumps([{"T": "error", "code": 402, "msg": "auth failed"}]).decode()
            )
            try:
                await ws.recv()
                await ws.send(
                    orjson.dumps(
                        [{"T": "error", "code": 402, "msg": "auth failed"}]
                    ).decode()
                )
            except websockets.exceptions.ConnectionClosed:
                return
            await asyncio.sleep(0.5)
        else:
            await _handshake(ws)
            await ws.wait_closed()

    server, port = await _start_fake_ws(handler)
    cfg2 = replace(
        cfg,
        alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
        auth_retry_seconds=1,
    )
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    try:
        await worker.start()
        assert await _wait_for(lambda: state.snapshot_health().auth_failed)
        assert await _wait_for(
            lambda: state.snapshot_health().authenticated
            and not state.snapshot_health().auth_failed
        )
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_ws_published_before_subscription_replay(wired):
    """The live socket must be visible (_ws set) before on_authenticated runs
    its subscription replay — a watchlist-style mutation racing the handshake
    otherwise persists its change but never sends the delta."""
    cfg, store, state, alerts = wired
    ws_seen: list[bool] = []

    async def handler(ws):
        await _handshake(ws)
        await asyncio.sleep(0.3)

    server, port = await _start_fake_ws(handler)
    cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news")
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    orig = worker.on_authenticated

    async def spy(ws):
        ws_seen.append(worker._ws is not None)
        await orig(ws)

    worker.on_authenticated = spy  # type: ignore[method-assign]
    try:
        await worker.start()
        assert await _wait_for(lambda: len(ws_seen) >= 1)
        assert ws_seen[0] is True
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_auth_alert_every_n_zero_does_not_crash_retry_loop(wired):
    """AUTH_ALERT_EVERY_N=0 disables repeated alerts — the second consecutive
    402 must not ZeroDivisionError the stream task out of its retry cadence."""
    cfg, store, state, alerts = wired
    connections = 0

    async def handler(ws):
        nonlocal connections
        connections += 1
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(
            orjson.dumps([{"T": "error", "code": 402, "msg": "auth failed"}]).decode()
        )
        try:
            await ws.recv()
            await ws.send(
                orjson.dumps([{"T": "error", "code": 402, "msg": "auth failed"}]).decode()
            )
        except websockets.exceptions.ConnectionClosed:
            return
        await asyncio.sleep(1)

    server, port = await _start_fake_ws(handler)
    cfg2 = replace(
        cfg,
        alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
        auth_retry_seconds=1,
        auth_alert_every_n=0,
    )
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    try:
        await worker.start()
        # Reaching a third connection proves the loop survived the second
        # failure (where the modulo used to divide by zero).
        assert await _wait_for(lambda: connections >= 3, deadline_s=15.0)
        stored = await store.get_alerts(minutes=5, categories=["stream_error"], limit=50)
        # First failure still alerts; no periodic re-alerts required.
        assert any("authentication failing" in a.reason for a in stored)
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_persister_retries_failed_batch_once(wired):
    """A transient persist failure must not lose the batch: one retry."""
    cfg, store, state, alerts = wired
    worker = NewsStreamWorker(cfg, store, state, alerts)

    calls = {"n": 0}
    real_persist = worker.persist_batch

    async def flaky(batch):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("transient")
        await real_persist(batch)

    worker.persist_batch = flaky  # type: ignore[method-assign]
    task = asyncio.create_task(worker._persister_loop())
    try:
        await worker._queue.put(("n", {"T": "n", "id": 7101, "headline": "retry me"}))
        assert await _wait_for(lambda: calls["n"] >= 2)
        await worker._queue.join()
        article = await store.get_article(7101)
        assert article is not None
        assert state.snapshot_health("news").persist_failures == 0
    finally:
        task.cancel()
        NewsStreamWorker.reset_singleton()


@pytest.mark.asyncio
async def test_persister_double_failure_routes_to_replayable_recovery(wired):
    """Two persist failures: batch becomes replayable raw events, the
    persist_failures counter increments, and a critical alert is recorded
    (once per dedup window)."""
    cfg, store, state, alerts = wired
    worker = NewsStreamWorker(cfg, store, state, alerts)

    async def always_fail(batch):
        raise RuntimeError("disk on fire")

    worker.persist_batch = always_fail  # type: ignore[method-assign]
    task = asyncio.create_task(worker._persister_loop())
    try:
        await worker._queue.put(("n", {"T": "n", "id": 7201, "headline": "lost?"}))
        await worker._queue.put(("n", {"T": "n", "id": 7202, "headline": "lost too?"}))
        assert await _wait_for(
            lambda: state.snapshot_health("news").persist_failures >= 1
        )
        await worker._queue.join()

        # Batch payloads landed as replayable drop events.
        events = await store.unreplayed_dropped_events(limit=10)
        replay_ids = {e["payload"]["id"] for e in events if e["payload"]}
        assert {7201, 7202} <= replay_ids

        # Exactly one persist_failure alert despite repeated failures
        # (stream_alert_deduped window).
        rows, _, _ = await store.alerts_since(cursor=0, limit=50)
        pf = [a for a in rows if a["category"] == "persist_failure"]
        assert len(pf) == 1
        assert pf[0]["severity"] == "critical"

        # The replay loop can now re-ingest them with the real persist path.
        worker.persist_batch = NewsStreamWorker.persist_batch.__get__(worker)  # type: ignore[method-assign]
        replayed = await worker._replay_dropped_once()
        assert replayed >= 2
        assert await store.get_article(7201) is not None
    finally:
        task.cancel()
        NewsStreamWorker.reset_singleton()


@pytest.mark.asyncio
async def test_stream_error_alerts_are_deduped(wired):
    """A stuck 406 loop must not flood the alert feed: one stream_error
    alert per (stream, code) dedup window."""
    cfg, store, state, alerts = wired
    cfg = replace(cfg, connection_limit_backoff_seconds=0)
    worker = NewsStreamWorker(cfg, store, state, alerts)
    try:
        for _ in range(3):
            await worker.handle_error({"T": "error", "code": 406, "msg": "limit"})
        rows, _, _ = await store.alerts_since(cursor=0, limit=50)
        errs = [a for a in rows if a["category"] == "stream_error"]
        assert len(errs) == 1
    finally:
        NewsStreamWorker.reset_singleton()

"""WebSocket worker tests using a local websockets server as a stand-in for Alpaca."""

from __future__ import annotations

import asyncio

import orjson
import pytest
import websockets

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.state import State
from alpaca_news_mcp.store import Store
from alpaca_news_mcp.stream import NewsStreamWorker, SingletonViolation


async def _start_fake_ws(handler):
    server = await websockets.serve(
        handler, "127.0.0.1", 0, ping_interval=None, ping_timeout=None
    )
    sock = next(iter(server.sockets))
    port = sock.getsockname()[1]
    return server, port


@pytest.fixture
async def wired(tmp_path, monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "x.sqlite"))
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "false")
    monkeypatch.setenv("BACKFILL_LOOKBACK_MINUTES", "1")
    monkeypatch.setenv("CONNECTION_LIMIT_BACKOFF_SECONDS", "1")
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
async def test_singleton_guard(wired):
    cfg, store, state, alerts = wired
    NewsStreamWorker.reset_singleton()
    NewsStreamWorker(cfg, store, state, alerts)
    with pytest.raises(SingletonViolation):
        NewsStreamWorker(cfg, store, state, alerts)


@pytest.mark.asyncio
async def test_full_happy_path_with_article(wired):
    cfg, store, state, alerts = wired

    article = {
        "T": "n",
        "id": 555,
        "headline": "fake article",
        "summary": "s",
        "created_at": "2026-04-28T19:00:00Z",
        "updated_at": "2026-04-28T19:00:00Z",
        "content": "<p>x</p>",
        "symbols": ["AAPL"],
        "source": "benzinga",
    }

    async def handler(ws):
        # Simulate connected + authenticated handshake
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        # Wait for client subscription
        sub = await ws.recv()
        assert orjson.loads(sub) == {"action": "subscribe", "news": ["*"]}
        await ws.send(orjson.dumps([{"T": "subscription", "news": ["*"]}]).decode())
        await ws.send(orjson.dumps([article]).decode())
        # Keep connection open briefly so persister can run
        await asyncio.sleep(0.5)

    from dataclasses import replace
    cfg2 = replace(cfg, alpaca_news_stream_url="ws://127.0.0.1:0/news")  # placeholder
    server, port = await _start_fake_ws(handler)
    cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news")
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    try:
        await worker.start()
        a = None
        for _ in range(40):
            a = await store.get_article(555)
            if a is not None:
                break
            await asyncio.sleep(0.1)
        assert a is not None
        assert state.snapshot_health().connected is True
        assert state.snapshot_health().authenticated is True
        assert state.subscription_state.acknowledged == {"news": ["*"]}
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_handshake_updates_last_message_at(wired):
    """During the auth/subscribe handshake the worker consumes WS frames in
    `_await_auth` and `_subscribe`, but historically only `_receive_loop`
    set `last_message_at`. Until the first post-ack message arrived,
    operators couldn't tell `WS just connected` from `WS broken/stalled`.
    Now every successful ws.recv() updates the field, so it is non-null
    immediately after the subscription ack."""
    cfg, store, state, alerts = wired

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        sub = await ws.recv()
        assert orjson.loads(sub) == {"action": "subscribe", "news": ["*"]}
        await ws.send(orjson.dumps([{"T": "subscription", "news": ["*"]}]).decode())
        # Hold the connection open without sending any T="n" article so the
        # only frames that flowed are the auth/subscribe handshake.
        await asyncio.sleep(0.5)

    from dataclasses import replace
    server, port = await _start_fake_ws(handler)
    cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news")
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    try:
        await worker.start()
        for _ in range(40):
            if state.subscription_state.acknowledged == {"news": ["*"]}:
                break
            await asyncio.sleep(0.05)
        assert state.subscription_state.acknowledged == {"news": ["*"]}
        assert state.snapshot_health().last_message_at is not None
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_406_triggers_backoff_path(wired):
    cfg, store, state, alerts = wired

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        await ws.recv()  # subscribe message
        await ws.send(orjson.dumps([{"T": "error", "code": 406, "msg": "connection limit exceeded"}]).decode())
        # Keep connection open briefly to give worker time to record alert
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
                       connection_limit_backoff_seconds=2,
                       reconnect_min_seconds=1, reconnect_max_seconds=1)
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        # Wait until 406 is observed
        for _ in range(50):
            await asyncio.sleep(0.1)
            if state.snapshot_health().connection_limit_blocked or state.snapshot_health().last_error:
                break
        # We expect at least one "stream_error" alert recorded for 406
        alerts_db = await store.get_alerts(minutes=60, severity="critical")
        assert any(al.category == "stream_error" for al in alerts_db)
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_409_marks_entitlement_error(wired):
    cfg, store, state, alerts = wired

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        await ws.recv()
        await ws.send(orjson.dumps([{"T": "error", "code": 409, "msg": "insufficient subscription"}]).decode())
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
                       reconnect_min_seconds=1, reconnect_max_seconds=1)
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        for _ in range(50):
            await asyncio.sleep(0.1)
            if state.snapshot_health().entitlement_error:
                break
        assert state.snapshot_health().entitlement_error is True
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_batched_array_with_multiple_articles(wired):
    cfg, store, state, alerts = wired

    payloads = [
        {
            "T": "n",
            "id": 1001 + i,
            "headline": f"hdr{i}",
            "created_at": "2026-04-28T19:00:00Z",
            "updated_at": "2026-04-28T19:00:00Z",
            "symbols": ["AAPL"] if i % 2 == 0 else ["MSFT"],
            "source": "benzinga",
        }
        for i in range(5)
    ]

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        await ws.recv()
        await ws.send(orjson.dumps([{"T": "subscription", "news": ["*"]}]).decode())
        await ws.send(orjson.dumps(payloads).decode())
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news")
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        for _ in range(50):
            recent = await store.get_recent_articles(minutes=60, limit=20)
            if len(recent) >= 5:
                break
            await asyncio.sleep(0.1)
        recent = await store.get_recent_articles(minutes=60, limit=20)
        assert {a.id for a in recent} >= {1001, 1002, 1003, 1004, 1005}
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_subscription_fallback_preserves_mode_after_ack(wired):
    cfg, store, state, alerts = wired

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        sub = await ws.recv()
        assert orjson.loads(sub) == {"action": "subscribe", "news": ["*"]}
        # Reject wildcard so the worker falls back to symbol list
        await ws.send(
            orjson.dumps([{"T": "error", "code": 409, "msg": "wildcard not entitled"}]).decode()
        )
        sub2 = await ws.recv()
        fallback = orjson.loads(sub2)
        assert fallback["action"] == "subscribe"
        assert fallback["news"] == ["AAPL", "MSFT"]
        # Acknowledge the fallback subscription
        await ws.send(orjson.dumps([{"T": "subscription", "news": ["AAPL", "MSFT"]}]).decode())
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(
            cfg,
            alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
            news_subscription_mode="wildcard",
            news_fallback_symbols=["AAPL", "MSFT"],
            reconnect_min_seconds=1,
            reconnect_max_seconds=1,
        )
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        for _ in range(50):
            if state.subscription_state.acknowledged is not None:
                break
            await asyncio.sleep(0.1)
        assert state.subscription_state.mode == "fallback"
        assert state.subscription_state.requested == {"news": ["AAPL", "MSFT"]}
        assert state.subscription_state.acknowledged == {"news": ["AAPL", "MSFT"]}
        assert state.snapshot_health().subscription_mode == "fallback"
        assert state.snapshot_health().requested_subscription == {"news": ["AAPL", "MSFT"]}
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_auth_failure_clears_connected_state(wired):
    """When auth fails, the websocket context exits but health must reflect that
    the stream is no longer connected — otherwise outages stay hidden."""
    cfg, store, state, alerts = wired

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        # Reject auth with code 402 to short-circuit both header and message auth.
        await ws.send(
            orjson.dumps([{"T": "error", "code": 402, "msg": "auth failed"}]).decode()
        )
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(
            cfg,
            alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
            reconnect_min_seconds=1,
            reconnect_max_seconds=1,
        )
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        for _ in range(50):
            await asyncio.sleep(0.1)
            if state.snapshot_health().authenticated is False and state.snapshot_health().connected is False:
                break
        assert state.snapshot_health().authenticated is False
        assert state.snapshot_health().connected is False
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_disconnect_clears_authenticated_flag(wired):
    """A successful session followed by a server-side close must reset both
    `connected` and `authenticated`. Leaving `authenticated=True` after the
    socket is gone misrepresents stream state to monitoring."""
    cfg, store, state, alerts = wired

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        await ws.recv()  # subscribe
        await ws.send(orjson.dumps([{"T": "subscription", "news": ["*"]}]).decode())
        # Drop the connection from the server side to simulate a normal disconnect.
        await ws.close()

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(
            cfg,
            alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
            reconnect_min_seconds=10,
            reconnect_max_seconds=10,
        )
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        # Wait until the subscription ack has been recorded — that only happens
        # after auth succeeds, so it proves the session reached `authenticated=True`.
        for _ in range(50):
            await asyncio.sleep(0.1)
            if state.subscription_state.acknowledged is not None:
                break
        assert state.subscription_state.acknowledged == {"news": ["*"]}
        # The server then closed the socket; wait for the cleanup path to run
        # and verify both flags are cleared (not just `connected`).
        for _ in range(50):
            await asyncio.sleep(0.1)
            if state.snapshot_health().connected is False:
                break
        assert state.snapshot_health().connected is False
        assert state.snapshot_health().authenticated is False
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_subscription_ack_does_not_drop_articles_in_same_frame(wired):
    """Alpaca can batch a subscription ack with `T="n"` articles in the same
    frame; returning on the first item must not drop the rest."""
    cfg, store, state, alerts = wired

    article = {
        "T": "n",
        "id": 9001,
        "headline": "ack-batched article",
        "created_at": "2026-04-28T19:00:00Z",
        "updated_at": "2026-04-28T19:00:00Z",
        "symbols": ["AAPL"],
        "source": "benzinga",
    }

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        await ws.recv()
        # Single frame: subscription ack + article batched together.
        await ws.send(
            orjson.dumps(
                [{"T": "subscription", "news": ["*"]}, article]
            ).decode()
        )
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news")
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        a = None
        for _ in range(50):
            a = await store.get_article(9001)
            if a is not None:
                break
            await asyncio.sleep(0.1)
        assert a is not None, "article batched with subscription ack was dropped"
        assert state.subscription_state.acknowledged == {"news": ["*"]}
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_queue_overflow_records_drop_instead_of_silent_eviction(wired):
    """When the persister can't keep up and the bounded queue fills, dropped
    articles must be observable: counted in stream_health and persisted as
    raw events — never silently evicted."""
    cfg, store, state, alerts = wired
    from dataclasses import replace

    cfg2 = replace(
        cfg,
        queue_maxsize=1,
        queue_backpressure_seconds=0.05,  # short backpressure window for the test
    )
    NewsStreamWorker.reset_singleton()
    worker = NewsStreamWorker(cfg2, store, state, alerts)
    try:
        # Fill the queue so the next _enqueue_article hits backpressure and
        # then must record the drop. We do NOT start the persister, so nothing
        # drains the queue.
        worker._queue.put_nowait(("n", {"id": 1, "headline": "first"}))
        assert worker._queue.full()

        await worker._enqueue_article({"id": 2, "headline": "second"})

        assert state.snapshot_health().articles_dropped == 1
        events = await store.recent_raw_events(minutes=60, limit=20)
        overflow_events = [e for e in events if e["message_type"] == "queue_overflow_drop"]
        assert overflow_events, "dropped article must be persisted as a raw event"
        assert overflow_events[0]["raw"].get("id") == 2
        # Old article still in queue — drop semantics record loss but don't
        # silently evict already-queued items.
        assert worker._queue.qsize() == 1
    finally:
        NewsStreamWorker.reset_singleton()


@pytest.mark.asyncio
async def test_non_entitlement_error_does_not_trigger_wildcard_fallback(wired):
    """A transient/unrelated subscribe-time error (e.g. 410) must not
    permanently downgrade wildcard → fallback for this session."""
    cfg, store, state, alerts = wired

    received_subs: list[dict] = []

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        sub = await ws.recv()
        received_subs.append(orjson.loads(sub))
        # Send a non-entitlement error code; the worker must NOT fall back.
        await ws.send(
            orjson.dumps([{"T": "error", "code": 410, "msg": "invalid subscribe action"}]).decode()
        )
        # Then ack the original wildcard request to confirm wildcard is preserved.
        await ws.send(orjson.dumps([{"T": "subscription", "news": ["*"]}]).decode())
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(
            cfg,
            alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
            news_subscription_mode="wildcard",
            news_fallback_symbols=["AAPL", "MSFT"],
            reconnect_min_seconds=1,
            reconnect_max_seconds=1,
        )
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        for _ in range(50):
            if state.subscription_state.acknowledged is not None:
                break
            await asyncio.sleep(0.1)
        # Only one subscribe message should have been sent (no fallback).
        assert received_subs == [{"action": "subscribe", "news": ["*"]}]
        assert state.subscription_state.mode == "wildcard"
        assert state.subscription_state.requested == {"news": ["*"]}
        assert state.snapshot_health().subscription_mode == "wildcard"
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_entitlement_flag_cleared_after_successful_subscription(wired):
    """After 409 (wildcard rejected) → fallback ack, the stale `entitlement_error`
    flag must be cleared so /healthz stops reporting the recovered failure."""
    cfg, store, state, alerts = wired

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        await ws.recv()  # initial wildcard subscribe
        # Reject wildcard with 409 — sets entitlement_error=True via _handle_error.
        await ws.send(
            orjson.dumps([{"T": "error", "code": 409, "msg": "wildcard not entitled"}]).decode()
        )
        await ws.recv()  # fallback subscribe
        # Ack the fallback — this must clear entitlement_error.
        await ws.send(orjson.dumps([{"T": "subscription", "news": ["AAPL", "MSFT"]}]).decode())
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(
            cfg,
            alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news",
            news_subscription_mode="wildcard",
            news_fallback_symbols=["AAPL", "MSFT"],
            reconnect_min_seconds=1,
            reconnect_max_seconds=1,
        )
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        for _ in range(50):
            if state.subscription_state.acknowledged is not None:
                break
            await asyncio.sleep(0.1)
        assert state.subscription_state.acknowledged == {"news": ["AAPL", "MSFT"]}
        assert state.snapshot_health().entitlement_error is False
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_malformed_message_is_recorded_and_loop_survives(wired):
    cfg, store, state, alerts = wired

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        await ws.recv()
        await ws.send(orjson.dumps([{"T": "subscription", "news": ["*"]}]).decode())
        await ws.send("not json {{{{")  # malformed
        # Then a good article afterwards
        good = {
            "T": "n",
            "id": 7777,
            "headline": "good",
            "created_at": "2026-04-28T19:00:00Z",
            "updated_at": "2026-04-28T19:00:00Z",
        }
        await ws.send(orjson.dumps([good]).decode())
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace
        cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news")
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        for _ in range(50):
            a = await store.get_article(7777)
            if a is not None:
                break
            await asyncio.sleep(0.1)
        assert a is not None
        events = await store.recent_raw_events(minutes=60, limit=20)
        assert any(e["message_type"] == "malformed" for e in events)
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_rolled_back_batch_does_not_advance_watermark(wired, monkeypatch):
    """A failed batch write must not advance in-memory article state: the
    gap-fill watermark (last_seen_updated_at) would otherwise end up past
    articles that never reached SQLite, permanently skipping them on the
    next reconnect gap-fill."""
    cfg, store, state, alerts = wired
    NewsStreamWorker.reset_singleton()
    worker = NewsStreamWorker(cfg, store, state, alerts)

    def payload(i: int, updated: str) -> dict:
        return {
            "T": "n", "id": i, "headline": f"a{i}", "summary": "s",
            "created_at": "2026-04-28T19:00:00Z", "updated_at": updated,
            "content": "", "symbols": ["AAPL"], "source": "benzinga",
        }

    calls = 0
    orig = store._upsert_article_locked

    async def flaky(normalized, *, source_kind):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("boom")
        return await orig(normalized, source_kind=source_kind)

    monkeypatch.setattr(store, "_upsert_article_locked", flaky)
    with pytest.raises(RuntimeError):
        await worker._persist_batch(
            [payload(1, "2026-04-28T19:00:00Z"), payload(2, "2026-04-28T19:00:01Z")]
        )
    # The batch rolled back: nothing committed, watermark untouched.
    assert state.last_seen_updated_at is None
    assert await store.get_article(1) is None


@pytest.mark.asyncio
async def test_wildcard_409_with_empty_fallback_uses_default_symbols(wired):
    """Default config has no NEWS_FALLBACK_SYMBOLS; an entitlement
    rejection must still downgrade to the built-in default list instead of
    leaving the session subscribed to nothing forever."""
    cfg, store, state, alerts = wired
    assert cfg.news_fallback_symbols == []
    subscribes: list[dict] = []

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        first = orjson.loads(await ws.recv())
        subscribes.append(first)
        await ws.send(orjson.dumps(
            [{"T": "error", "code": 409, "msg": "insufficient subscription"}]
        ).decode())
        second = orjson.loads(await ws.recv())
        subscribes.append(second)
        await ws.send(orjson.dumps(
            [{"T": "subscription", "news": second["news"]}]
        ).decode())
        await asyncio.sleep(0.5)

    server, port = await _start_fake_ws(handler)
    try:
        from dataclasses import replace

        from alpaca_news_mcp.stream import DEFAULT_FALLBACK_SYMBOLS
        cfg2 = replace(cfg, alpaca_news_stream_url=f"ws://127.0.0.1:{port}/news")
        worker = NewsStreamWorker(cfg2, store, state, alerts)
        await worker.start()
        for _ in range(80):
            await asyncio.sleep(0.05)
            if len(subscribes) >= 2:
                break
        assert subscribes[0]["news"] == ["*"]
        assert subscribes[1]["news"] == list(DEFAULT_FALLBACK_SYMBOLS)
        for _ in range(40):
            await asyncio.sleep(0.05)
            if state.snapshot_health().subscription_mode == "fallback":
                break
        assert state.snapshot_health().subscription_mode == "fallback"
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()

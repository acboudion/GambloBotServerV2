"""StockStreamWorker: codec decoding, routing, watchlist, alerts, persistence."""

from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import UTC, datetime

import msgpack
import orjson
import pytest
import websockets

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.market_store import MarketStore
from alpaca_news_mcp.state import State
from alpaca_news_mcp.stock_stream import StockStreamWorker, _to_epoch_us
from alpaca_news_mcp.store import Store


@pytest.fixture
async def wired(tmp_path, monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "news.sqlite"))
    monkeypatch.setenv("MARKET_STORAGE_PATH", str(tmp_path / "market.sqlite"))
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "false")
    monkeypatch.setenv("RECONNECT_MIN_SECONDS", "1")
    monkeypatch.setenv("RECONNECT_MAX_SECONDS", "1")
    monkeypatch.setenv("STOCK_WATCHLIST_SYMBOLS", "AAPL,TSLA")
    cfg = Config.from_env()
    store = await Store.open(cfg.storage_path)
    await store.init_schema()
    market_store = await MarketStore.open(cfg.market_storage_path)
    await market_store.init_schema()
    state = State()
    state.set_interest_symbols(["NVDA"], "replace")
    alerts = AlertEngine(halt_alert_dedup_seconds=300)
    yield cfg, store, market_store, state, alerts
    await store.close()
    await market_store.close()
    StockStreamWorker.reset_singleton()


def _worker(cfg, store, market_store, state, alerts, **kw) -> StockStreamWorker:
    return StockStreamWorker(cfg, store, market_store, state, alerts, **kw)


def _trade(symbol="AAPL", price=190.5, ts="2026-04-28T15:30:00.123456Z"):
    return {"T": "t", "S": symbol, "i": 1, "x": "V", "p": price, "s": 100,
            "c": ["@"], "z": "C", "t": ts}


def _quote(symbol="AAPL", ts="2026-04-28T15:30:01Z"):
    return {"T": "q", "S": symbol, "bx": "V", "bp": 190.4, "bs": 2,
            "ax": "V", "ap": 190.6, "as": 3, "c": ["R"], "z": "C", "t": ts}


def _bar(symbol="AAPL", T="b", ts="2026-04-28T15:30:00Z"):
    return {"T": T, "S": symbol, "o": 190.0, "h": 191.0, "l": 189.5,
            "c": 190.5, "v": 10000, "n": 120, "vw": 190.3, "t": ts}


def _status(symbol="AAPL", sc="H", sm="Trading Halt"):
    return {"T": "s", "S": symbol, "sc": sc, "sm": sm, "rc": "T12",
            "rm": "News Pending", "z": "C", "t": "2026-04-28T15:31:00Z"}


def _luld(symbol="AAPL"):
    return {"T": "l", "S": symbol, "u": 200.0, "d": 180.0, "i": "B", "z": "C",
            "t": "2026-04-28T15:31:05Z"}


def test_to_epoch_us_handles_nanosecond_strings_and_datetimes():
    base = datetime(2026, 4, 28, 15, 30, tzinfo=UTC)
    base_us = int(base.timestamp() * 1_000_000)
    assert _to_epoch_us("2026-04-28T15:30:00Z") == base_us
    # Nanosecond precision (Alpaca JSON) must not crash; truncates to µs.
    ns = _to_epoch_us("2026-04-28T15:30:00.123456789Z")
    assert ns == base_us + 123456
    assert _to_epoch_us(base) == base_us
    assert _to_epoch_us(None) is None
    assert _to_epoch_us("garbage") is None


@pytest.mark.asyncio
async def test_msgpack_and_json_decoding(wired):
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    trade = _trade()
    # JSON text frame decodes regardless of codec.
    items = worker.decode_frame(orjson.dumps([trade]).decode())
    assert items[0]["T"] == "t"
    # msgpack binary frame (timestamps arrive as datetime via ext type).
    packed = msgpack.packb([{**trade, "t": datetime(2026, 4, 28, 15, 30, tzinfo=UTC)}],
                           datetime=True)
    items = worker.decode_frame(packed)
    assert items[0]["S"] == "AAPL"
    assert isinstance(items[0]["t"], datetime)


@pytest.mark.asyncio
async def test_outbound_control_frames_match_negotiated_codec(wired):
    """On a msgpack-negotiated connection, auth/subscribe/unsubscribe must be
    msgpack-encoded bytes (Alpaca rejects JSON text frames with 400); in JSON
    mode they must stay text."""
    cfg, store, market_store, state, alerts = wired
    assert cfg.stock_stream_codec == "msgpack"
    worker = _worker(cfg, store, market_store, state, alerts)

    encoded = worker.encode_message({"action": "auth", "key": "k", "secret": "s"})
    assert isinstance(encoded, bytes)
    assert msgpack.unpackb(encoded, raw=False) == {
        "action": "auth", "key": "k", "secret": "s",
    }

    sent: list = []

    class FakeWS:
        async def send(self, payload) -> None:
            sent.append(payload)

    await worker._send_subscription(FakeWS(), "subscribe", ["AAPL"])
    assert isinstance(sent[0], bytes)
    assert msgpack.unpackb(sent[0], raw=False)["action"] == "subscribe"

    StockStreamWorker.reset_singleton()
    cfg_json = replace(cfg, stock_stream_codec="json")
    worker_json = _worker(cfg_json, store, market_store, state, alerts)
    encoded_json = worker_json.encode_message({"action": "auth"})
    assert isinstance(encoded_json, str)
    sent.clear()
    await worker_json._send_subscription(FakeWS(), "subscribe", ["AAPL"])
    assert isinstance(sent[0], str)
    assert orjson.loads(sent[0])["action"] == "subscribe"


@pytest.mark.asyncio
async def test_persist_batch_writes_all_types_and_snapshots(wired):
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    batch = [
        ("t", _trade()),
        ("q", _quote()),
        ("b", _bar()),
        ("d", _bar(T="d", ts="2026-04-28T04:00:00Z")),
        ("s", _status()),
        ("l", _luld()),
        ("raw", {"T": "c", "S": "AAPL", "x": "V", "p": 190.0, "t": "2026-04-28T15:30:00Z"}),
    ]
    await worker.persist_batch(batch)

    since_us = _to_epoch_us("2026-04-28T00:00:00Z")
    assert len(await market_store.trades_window("AAPL", since_us=since_us, limit=10)) == 1
    assert len(await market_store.quotes_window("AAPL", since_us=since_us, limit=10)) == 1
    assert len(await market_store.bars_window("AAPL", timeframe="1min", limit=10)) == 1
    assert len(await market_store.bars_window("AAPL", timeframe="1day", limit=10)) == 1
    assert len(await market_store.recent_statuses(minutes=10**6)) == 1
    assert len(await market_store.recent_lulds(minutes=10**6)) == 1

    snap = market_store.snapshots.get("AAPL")
    assert snap["trade"]["p"] == 190.5
    assert snap["quote"]["bp"] == 190.4
    assert snap["minute_bar"]["c"] == 190.5
    assert snap["daily_bar"]["c"] == 190.5
    assert snap["status"]["sc"] == "H"
    assert snap["luld"]["u"] == 200.0


@pytest.mark.asyncio
async def test_halt_alert_for_watchlist_symbol_is_critical(wired):
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    await worker.persist_batch([("s", _status("AAPL"))])
    stored = await store.get_alerts(minutes=5, categories=["trading_halt"], limit=10)
    assert len(stored) == 1
    assert stored[0].severity == "critical"  # AAPL is on the watchlist
    assert stored[0].symbols == ["AAPL"]

    # Dedup: the same halt within the window doesn't alert again.
    await worker.persist_batch([("s", _status("AAPL"))])
    stored = await store.get_alerts(minutes=5, categories=["trading_halt"], limit=10)
    assert len(stored) == 1


@pytest.mark.asyncio
async def test_halt_alert_for_other_symbol_is_high_and_resume_medium(wired):
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    await worker.persist_batch([("s", _status("XYZ"))])
    halted = await store.get_alerts(minutes=5, categories=["trading_halt"], limit=10)
    assert halted[0].severity == "high"
    await worker.persist_batch([("s", _status("XYZ", sc="T", sm="Trading Resumption"))])
    resumed = await store.get_alerts(minutes=5, categories=["trading_resume"], limit=10)
    assert resumed[0].severity == "medium"


@pytest.mark.asyncio
async def test_quote_sampling_thins_storage_but_not_snapshots(wired):
    cfg, store, market_store, state, alerts = wired
    cfg2 = replace(cfg, stock_quote_sample_ms=1000)
    worker = _worker(cfg2, store, market_store, state, alerts)
    batch = [
        ("q", {**_quote(ts="2026-04-28T15:30:01.100Z"), "bp": 1.0}),
        ("q", {**_quote(ts="2026-04-28T15:30:01.900Z"), "bp": 2.0}),  # same 1s bucket
        ("q", {**_quote(ts="2026-04-28T15:30:02.500Z"), "bp": 3.0}),
    ]
    await worker.persist_batch(batch)
    since_us = _to_epoch_us("2026-04-28T00:00:00Z")
    quotes = await market_store.quotes_window("AAPL", since_us=since_us, limit=10)
    assert [q["bid_price"] for q in quotes] == [2.0, 3.0]  # bucket-last kept
    assert market_store.snapshots.get("AAPL")["quote"]["bp"] == 3.0


@pytest.mark.asyncio
async def test_update_watchlist_modes_and_persistence(wired):
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    out = await worker.update_watchlist(["nvda"], mode="add")
    assert out["watchlist"] == ["AAPL", "NVDA", "TSLA"]
    assert out["connected"] is False and out["delta_sent"] is False

    out = await worker.update_watchlist(["TSLA"], mode="remove")
    assert out["watchlist"] == ["AAPL", "NVDA"]

    out = await worker.update_watchlist(["SPY", "QQQ"], mode="replace")
    assert out["watchlist"] == ["QQQ", "SPY"]

    bad = await worker.update_watchlist(["X"], mode="bogus")
    assert bad["error"] == "invalid_mode"

    too_many = await worker.update_watchlist(
        [f"S{i}" for i in range(101)], mode="replace"
    )
    assert too_many["error"] == "watchlist_too_large"

    # Persisted → a fresh worker restores it.
    StockStreamWorker.reset_singleton()
    worker2 = _worker(cfg, store, market_store, state, alerts)
    await worker2.restore_watchlist()
    assert worker2.watchlist() == ["QQQ", "SPY"]


def _decode_control(payload) -> dict:
    """Outbound control frames are msgpack bytes in msgpack mode, JSON text otherwise."""
    if isinstance(payload, bytes):
        return msgpack.unpackb(payload, raw=False)
    return orjson.loads(payload)


@pytest.mark.asyncio
async def test_update_watchlist_sends_deltas_when_connected(wired):
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    sent: list[dict] = []

    class FakeWS:
        async def send(self, payload) -> None:
            sent.append(_decode_control(payload))

    worker._ws = FakeWS()
    out = await worker.update_watchlist(["NVDA"], mode="add")
    assert out["delta_sent"] is True
    assert len(sent) == 1
    assert sent[0]["action"] == "subscribe"
    assert sent[0]["trades"] == ["NVDA"]
    assert sent[0]["lulds"] == ["NVDA"]

    sent.clear()
    await worker.update_watchlist(["AAPL"], mode="remove")
    assert sent[0]["action"] == "unsubscribe"
    assert sent[0]["quotes"] == ["AAPL"]


@pytest.mark.asyncio
async def test_full_happy_path_over_fake_ws_json(wired):
    """End-to-end over a real websocket in JSON mode: auth → subscribe →
    ack → trade + halt persisted."""
    cfg, store, market_store, state, alerts = wired
    received_sub: list[dict] = []

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        sub = orjson.loads(await ws.recv())
        received_sub.append(sub)
        await ws.send(
            orjson.dumps([{"T": "subscription", "trades": sub.get("trades", [])}]).decode()
        )
        await ws.send(orjson.dumps([_trade(), _status("TSLA")]).decode())
        await ws.wait_closed()

    server = await websockets.serve(handler, "127.0.0.1", 0, ping_interval=None)
    port = next(iter(server.sockets)).getsockname()[1]
    cfg2 = replace(
        cfg,
        alpaca_stock_stream_url=f"ws://127.0.0.1:{port}/v2/test",
        stock_stream_codec="json",
    )
    worker = _worker(cfg2, store, market_store, state, alerts)
    try:
        await worker.start()
        deadline = asyncio.get_event_loop().time() + 8
        while asyncio.get_event_loop().time() < deadline:
            snap = market_store.snapshots.get("AAPL")
            if snap and "trade" in snap:
                break
            await asyncio.sleep(0.05)
        assert market_store.snapshots.get("AAPL")["trade"]["p"] == 190.5
        assert received_sub[0]["action"] == "subscribe"
        assert received_sub[0]["trades"] == ["AAPL", "TSLA"]
        assert state.snapshot_health("stocks").authenticated is True
        assert state.snapshot_health("stocks").acknowledged_subscription is not None
        halts = await store.get_alerts(minutes=5, categories=["trading_halt"], limit=10)
        assert len(halts) == 1 and halts[0].symbols == ["TSLA"]
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_statuses_and_lulds_keep_raw_payloads_ticks_do_not(wired):
    """Audit policy: raw payloads preserved for statuses/LULDs (rare,
    alert-driving) but not for high-volume ticks (flattened losslessly)."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    await worker.persist_batch([
        ("t", _trade()),
        ("q", _quote()),
        ("b", _bar()),
        ("s", _status()),
        ("l", _luld()),
    ])
    cur = await market_store.conn.execute(
        "SELECT message_type, raw_json FROM market_raw_events ORDER BY message_type"
    )
    rows = [(r["message_type"], orjson.loads(r["raw_json"])) for r in await cur.fetchall()]
    await cur.close()
    assert [mt for mt, _ in rows] == ["l", "s"]
    luld_raw = dict(rows)["l"]
    status_raw = dict(rows)["s"]
    assert status_raw["sc"] == "H" and status_raw["S"] == "AAPL"
    assert luld_raw["u"] == 200.0


class _FakeMarketClient:
    """Duck-typed stand-in for MarketDataClient in gap-fill tests."""

    def __init__(self, bars_by_symbol=None, error=None):
        self.bars_by_symbol = bars_by_symbol or {}
        self.error = error
        self.bars_calls = 0

    async def bars(self, symbols, *, start_iso, end_iso=None, timeframe="1Min",
                   limit_per_page=1000):
        self.bars_calls += 1
        if self.error is not None:
            raise self.error
        return self.bars_by_symbol

    async def is_market_open(self):
        return True


@pytest.mark.asyncio
async def test_bar_gap_fill_runs_on_first_session_after_restart(wired):
    """A restarted process must backfill bars on its FIRST connection — stocks
    have no startup backfill, so skipping the first session loses data."""
    cfg, store, market_store, state, alerts = wired
    fake_client = _FakeMarketClient(
        bars_by_symbol={"AAPL": [{"t": "2026-04-28T15:29:00Z", "o": 1, "h": 2,
                                  "l": 0.5, "c": 1.5, "v": 100, "n": 5, "vw": 1.1}]}
    )

    async def handler(ws):
        await ws.send(orjson.dumps([{"T": "success", "msg": "connected"}]).decode())
        await ws.send(orjson.dumps([{"T": "success", "msg": "authenticated"}]).decode())
        await ws.recv()  # subscribe message
        await ws.send(orjson.dumps([{"T": "subscription", "trades": ["AAPL"]}]).decode())
        await ws.wait_closed()

    server = await websockets.serve(handler, "127.0.0.1", 0, ping_interval=None)
    port = next(iter(server.sockets)).getsockname()[1]
    cfg2 = replace(
        cfg,
        alpaca_stock_stream_url=f"ws://127.0.0.1:{port}/v2/test",
        stock_stream_codec="json",
    )
    worker = _worker(cfg2, store, market_store, state, alerts,
                     market_client=fake_client)
    assert worker.gap_fill_on_first_session() is True
    try:
        await worker.start()
        deadline = asyncio.get_event_loop().time() + 8
        while asyncio.get_event_loop().time() < deadline:
            if fake_client.bars_calls >= 1:
                bars = await market_store.bars_window("AAPL", timeframe="1min", limit=10)
                if bars:
                    break
            await asyncio.sleep(0.05)
        assert fake_client.bars_calls >= 1
        bars = await market_store.bars_window("AAPL", timeframe="1min", limit=10)
        assert len(bars) == 1 and bars[0]["close"] == 1.5
    finally:
        await worker.stop()
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_bar_gap_fill_failure_reaches_retry_alerting(wired, monkeypatch):
    """When the bars REST call fails, _bar_gap_fill must propagate so the
    base retry/alert path records gap_fill_failure."""
    import alpaca_news_mcp.ws_base as ws_base
    from alpaca_news_mcp.market_client import MarketDataError

    monkeypatch.setattr(ws_base, "GAP_FILL_RETRY_WAIT_SECONDS", 0.01)
    cfg, store, market_store, state, alerts = wired
    fake_client = _FakeMarketClient(error=MarketDataError("REST down"))
    worker = _worker(cfg, store, market_store, state, alerts,
                     market_client=fake_client)
    await worker._gap_fill_with_retries()
    assert fake_client.bars_calls == 3  # retried
    assert state.snapshot_health("stocks").gap_fill_failures == 3
    stored = await store.get_alerts(minutes=5, categories=["gap_fill_failure"], limit=10)
    assert any(a.severity == "critical" for a in stored)


@pytest.mark.asyncio
async def test_reconnect_clears_stale_subscription_ack(wired):
    """A new session must not report the previous session's acknowledgement —
    trust acks, not requests."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    # Simulate a previous session's ack.
    worker._acknowledged = {"trades": ["AAPL", "TSLA"]}
    state.update_health("stocks", acknowledged_subscription={"trades": ["AAPL", "TSLA"]})

    sent: list = []

    class FakeWS:
        async def send(self, payload) -> None:
            sent.append(payload)

    await worker.on_authenticated(FakeWS())
    assert worker.acknowledged_subscription() is None
    assert state.snapshot_health("stocks").acknowledged_subscription is None
    assert len(sent) == 1  # fresh subscribe was replayed

    # Ack from the new session repopulates it.
    await worker.handle_item(FakeWS(), {"T": "subscription", "trades": ["AAPL", "TSLA"]})
    assert worker.acknowledged_subscription() == {"trades": ["AAPL", "TSLA"]}


@pytest.mark.asyncio
async def test_dropped_status_and_luld_are_persisted_not_lost(wired):
    """Queue-overflow drops must not lose halts/LULDs: they are persisted
    (and alerted) directly, bypassing the queue. Ticks stay expendable."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)

    await worker.on_dropped_item("s", _status("AAPL"))
    await worker.on_dropped_item("l", _luld("AAPL"))
    await worker.on_dropped_item("raw", {"T": "correction", "S": "AAPL", "x": "V"})
    # The halt survived: persisted + alerted, and not counted as dropped.
    assert len(await market_store.recent_statuses(minutes=10**6)) == 1
    assert len(await market_store.recent_lulds(minutes=10**6)) == 1
    halts = await store.get_alerts(minutes=5, categories=["trading_halt"], limit=10)
    assert len(halts) == 1 and halts[0].severity == "critical"
    assert state.snapshot_health("stocks").articles_dropped == 0
    # The dropped correction also reached the raw audit table.
    cur = await market_store.conn.execute(
        "SELECT message_type FROM market_raw_events ORDER BY message_type"
    )
    kinds = [r["message_type"] for r in await cur.fetchall()]
    await cur.close()
    assert kinds == ["correction", "l", "s"]

    # A dropped trade is just counted.
    await worker.on_dropped_item("t", _trade())
    assert state.snapshot_health("stocks").articles_dropped == 1
    since_us = _to_epoch_us("2026-04-28T00:00:00Z")
    assert await market_store.trades_window("AAPL", since_us=since_us, limit=10) == []


@pytest.mark.asyncio
async def test_watchlist_mutation_clears_stale_ack(wired):
    """Changing the watchlist invalidates the previous acknowledgement — the
    old ack must not be reported for a subscription it no longer describes."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    worker._acknowledged = {"trades": ["AAPL", "TSLA"]}
    state.update_health("stocks", acknowledged_subscription={"trades": ["AAPL", "TSLA"]})

    await worker.update_watchlist(["NVDA"], mode="add")  # disconnected mutation
    assert worker.acknowledged_subscription() is None
    assert state.snapshot_health("stocks").acknowledged_subscription is None

    # A no-op mutation (same set) keeps whatever ack exists.
    worker._acknowledged = {"trades": ["AAPL", "NVDA", "TSLA"]}
    await worker.update_watchlist([], mode="remove")
    assert worker.acknowledged_subscription() == {"trades": ["AAPL", "NVDA", "TSLA"]}


@pytest.mark.asyncio
async def test_restore_honors_persisted_empty_watchlist(wired):
    """An operator who explicitly cleared the watchlist must not get the
    config default back after a restart."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    out = await worker.update_watchlist([], mode="replace")
    assert out["watchlist"] == []

    StockStreamWorker.reset_singleton()
    worker2 = _worker(cfg, store, market_store, state, alerts)
    assert worker2.watchlist() == ["AAPL", "TSLA"]  # config default pre-restore
    await worker2.restore_watchlist()
    assert worker2.watchlist() == []


@pytest.mark.asyncio
async def test_bar_gap_fill_uses_captured_watermark(wired):
    """_bar_gap_fill must honor the pre-receive watermark instead of
    re-reading latest_bar_ts (which a post-reconnect stream bar may have
    advanced past the outage window)."""
    cfg, store, market_store, state, alerts = wired
    starts: list[str] = []

    class RecordingClient(_FakeMarketClient):
        async def bars(self, symbols, *, start_iso, end_iso=None, timeframe="1Min",
                       limit_per_page=1000):
            starts.append(start_iso)
            return {}

    client = RecordingClient()
    worker = _worker(cfg, store, market_store, state, alerts, market_client=client)

    old_ts = 1_700_000_000 // 60 * 60
    await market_store.persist_stream_batch(
        bars=[("AAPL", "1min", old_ts, 1, 2, 0.5, 1.5, 100, 5, 1.1)]
    )
    watermark = await worker.capture_gap_fill_watermark()
    # A post-reconnect stream bar advances the live latest_bar_ts.
    await market_store.persist_stream_batch(
        bars=[("AAPL", "1min", old_ts + 3600, 1, 2, 0.5, 1.5, 100, 5, 1.1)]
    )
    await worker._bar_gap_fill(watermark)
    assert len(starts) == 1
    assert str(old_ts + 3600) not in starts[0]
    from datetime import UTC as _UTC
    from datetime import datetime as _dt
    assert starts[0] == _dt.fromtimestamp(old_ts, tz=_UTC).isoformat()


@pytest.mark.asyncio
async def test_watchlist_removal_evicts_snapshot_cache(wired):
    """A removed symbol must not keep serving stale prices from the latest
    cache as source="stream" — its snapshot is evicted on removal."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    market_store.snapshots.update("AAPL", "trade", {"p": 190.5, "ts_us": 1})
    market_store.snapshots.update("TSLA", "trade", {"p": 250.0, "ts_us": 1})
    out = await worker.update_watchlist(["AAPL"], mode="remove")
    assert out["removed"] == ["AAPL"]
    assert market_store.snapshots.get("AAPL") is None
    # Symbols still on the watchlist keep their snapshots.
    assert market_store.snapshots.get("TSLA") is not None


@pytest.mark.asyncio
async def test_gap_fill_disabled_without_bar_channels(wired):
    """With STOCK_CHANNELS excluding both bar channels, no bar gap-fill
    callback is installed — reconnects must not write REST bars the
    operator opted out of."""
    cfg, store, market_store, state, alerts = wired
    client = _FakeMarketClient()
    cfg_no_bars = replace(cfg, stock_channels=["trades", "quotes"])
    worker = _worker(cfg_no_bars, store, market_store, state, alerts,
                     market_client=client)
    assert worker._gap_fill is None
    StockStreamWorker.reset_singleton()
    cfg_updated_only = replace(cfg, stock_channels=["trades", "updatedBars"])
    worker2 = _worker(cfg_updated_only, store, market_store, state, alerts,
                      market_client=client)
    assert worker2._gap_fill is not None


@pytest.mark.asyncio
async def test_malformed_timestamp_frame_does_not_clobber_snapshot(wired):
    """A frame with a missing/unparseable `t` isn't persisted as a flattened
    row — it must not overwrite the latest snapshot either (the cache can't
    order a None timestamp, so the malformed frame would win)."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    await worker.persist_batch([("t", _trade(price=190.5))])
    assert market_store.snapshots.get("AAPL")["trade"]["p"] == 190.5

    bad = _trade(price=1.0)
    bad["t"] = "garbage"
    await worker.persist_batch([("t", bad)])
    snap = market_store.snapshots.get("AAPL")
    assert snap["trade"]["p"] == 190.5  # good latest survived
    no_ts = _trade(price=2.0)
    del no_ts["t"]
    await worker.persist_batch([("t", no_ts)])
    assert market_store.snapshots.get("AAPL")["trade"]["p"] == 190.5


@pytest.mark.asyncio
async def test_oversized_startup_watchlist_is_capped(wired):
    """An env/persisted watchlist over MAX_WATCHLIST_SYMBOLS must be clamped
    before the first subscribe — an oversized request would hit Alpaca's
    symbol limit and leave the stream with no acknowledged subscription."""
    cfg, store, market_store, state, alerts = wired
    big = [f"S{i:03d}" for i in range(120)]
    cfg_big = replace(cfg, stock_watchlist_symbols=big)
    worker = _worker(cfg_big, store, market_store, state, alerts)
    assert len(worker.watchlist()) == 100
    assert worker.watchlist() == sorted(big)[:100]

    # The restored path clamps too (e.g. state written by an older build).
    await store.set_status("stock_watchlist", {"symbols": big})
    await worker.restore_watchlist()
    assert len(worker.watchlist()) == 100


@pytest.mark.asyncio
async def test_watchdog_inert_with_empty_watchlist(wired):
    """An intentionally cleared watchlist means silence is expected — the
    idle watchdog must not reconnect all day during market hours."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts,
                     market_client=_FakeMarketClient())
    assert await worker.watchdog_should_fire() is True  # symbols + open market
    await worker.update_watchlist([], mode="replace")
    assert await worker.watchdog_should_fire() is False


@pytest.mark.asyncio
async def test_watchdog_inert_for_event_only_channels(wired):
    """statuses/lulds-only subscriptions have no steady message flow — the
    watchdog must not treat healthy silence as a stall during market hours."""
    cfg, store, market_store, state, alerts = wired
    cfg_events = replace(cfg, stock_channels=["statuses", "lulds"])
    worker = _worker(cfg_events, store, market_store, state, alerts,
                     market_client=_FakeMarketClient())
    assert await worker.watchdog_should_fire() is False
    StockStreamWorker.reset_singleton()
    cfg_chatty = replace(cfg, stock_channels=["trades", "statuses"])
    worker2 = _worker(cfg_chatty, store, market_store, state, alerts,
                      market_client=_FakeMarketClient())
    assert await worker2.watchdog_should_fire() is True


@pytest.mark.asyncio
async def test_trade_without_price_routes_to_raw_not_tick_series(wired):
    """A trade frame missing `p` (or with a null price) must not persist as a
    fake 0.0 trade — it goes to the raw audit table instead."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    no_price = _trade()
    del no_price["p"]
    null_price = _trade()
    null_price["p"] = None
    await worker.persist_batch([("t", no_price), ("t", null_price)])
    assert await market_store.trades_window("AAPL", since_us=0, limit=10) == []
    cur = await market_store.conn.execute(
        "SELECT COUNT(*) FROM market_raw_events WHERE message_type = 't'"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row[0] == 2
    # A real trade still lands.
    await worker.persist_batch([("t", _trade())])
    assert len(await market_store.trades_window("AAPL", since_us=0, limit=10)) == 1


@pytest.mark.asyncio
async def test_priceless_trade_does_not_clobber_snapshot(wired):
    """A trade rejected by the tick store (no numeric price) must not become
    the cached latest trade either."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    await worker.persist_batch([("t", _trade(price=190.5))])
    bad = _trade()
    bad["p"] = None
    bad["t"] = "2026-04-28T15:35:00Z"  # newer than the good trade
    await worker.persist_batch([("t", bad)])
    assert market_store.snapshots.get("AAPL")["trade"]["p"] == 190.5


@pytest.mark.asyncio
async def test_inflight_frames_for_removed_symbol_do_not_repopulate_cache(wired):
    """Queued/in-flight frames arriving after a symbol was removed from the
    watchlist must not repopulate the evicted snapshot — get_latest_market_data
    would serve source="stream" for an unsubscribed symbol indefinitely."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    await worker.persist_batch([("t", _trade(price=190.5))])
    assert market_store.snapshots.get("AAPL") is not None
    await worker.update_watchlist(["AAPL"], mode="remove")
    assert market_store.snapshots.get("AAPL") is None
    # A frame that was already queued lands after the eviction.
    await worker.persist_batch([("t", _trade(price=191.0, ts="2026-04-28T15:36:00Z"))])
    assert market_store.snapshots.get("AAPL") is None
    # Still-subscribed symbols keep updating.
    await worker.persist_batch([("t", _trade(symbol="TSLA", price=250.0))])
    assert market_store.snapshots.get("TSLA")["trade"]["p"] == 250.0


@pytest.mark.asyncio
async def test_dropped_bar_frames_are_direct_persisted(wired):
    """Queue overflow must not lose bar frames — a mid-stream bar hole is
    invisible to get_bars_since and gap-fill only runs on reconnect."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    try:
        await worker.on_dropped_item("b", _bar("AAPL", T="b", ts="2026-04-28T15:31:00Z"))
        rows, _, _ = await market_store.bars_since(cursor=0, limit=10)
        assert [r["symbol"] for r in rows] == ["AAPL"]
        # Ticks stay counted-loss only.
        await worker.on_dropped_item("q", _quote("AAPL"))
        assert state.snapshot_health("stocks").articles_dropped == 1
    finally:
        StockStreamWorker.reset_singleton()


@pytest.mark.asyncio
async def test_stock_recovery_salvages_events_and_counts_tick_loss(wired):
    """After a double persist failure, statuses/bars get a direct retry and
    tick losses land in the dropped counter."""
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    try:
        batch = [
            ("t", _trade("AAPL")),
            ("q", _quote("AAPL")),
            ("s", _status("AAPL")),
            ("b", _bar("AAPL", T="b", ts="2026-04-28T15:32:00Z")),
        ]
        await worker.recover_failed_batch(batch)
        # Status + bar salvaged into the store.
        statuses = await market_store.recent_statuses(minutes=100_000, limit=10)
        assert len(statuses) == 1
        rows, _, _ = await market_store.bars_since(cursor=0, limit=10)
        assert len(rows) == 1
        # Trade + quote counted as dropped.
        assert state.snapshot_health("stocks").articles_dropped == 2
    finally:
        StockStreamWorker.reset_singleton()

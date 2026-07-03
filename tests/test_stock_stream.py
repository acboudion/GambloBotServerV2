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


@pytest.mark.asyncio
async def test_update_watchlist_sends_deltas_when_connected(wired):
    cfg, store, market_store, state, alerts = wired
    worker = _worker(cfg, store, market_store, state, alerts)
    sent: list[dict] = []

    class FakeWS:
        async def send(self, payload: str) -> None:
            sent.append(orjson.loads(payload))

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

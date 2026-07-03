"""MarketStore: batch writes, bar upserts, cursors, retention, snapshot cache."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from alpaca_news_mcp.market_store import MarketSnapshotCache, MarketStore


def _us(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000)


@pytest.fixture
async def market_store(tmp_path):
    store = await MarketStore.open(str(tmp_path / "market.sqlite"))
    await store.init_schema()
    yield store
    await store.close()


@pytest.mark.asyncio
async def test_persist_batch_and_windows(market_store):
    now = datetime.now(UTC)
    trades = [
        ("AAPL", _us(now - timedelta(seconds=2)), 190.0, 100, "V", "@", "C", 1),
        ("AAPL", _us(now - timedelta(seconds=1)), 190.5, 50, "V", "@", "C", 2),
        ("TSLA", _us(now), 250.0, 10, "V", "@", "C", 3),
    ]
    quotes = [
        ("AAPL", _us(now), 189.9, 2, "V", 190.1, 3, "V", "R", "C"),
    ]
    await market_store.persist_stream_batch(trades=trades, quotes=quotes)

    since = _us(now - timedelta(minutes=1))
    got = await market_store.trades_window("aapl", since_us=since, limit=10)
    assert [t["price"] for t in got] == [190.0, 190.5]  # ascending
    q = await market_store.quotes_window("AAPL", since_us=since, limit=10)
    assert q[0]["bid_price"] == 189.9 and q[0]["ask_price"] == 190.1


@pytest.mark.asyncio
async def test_bar_upsert_replaces_in_place(market_store):
    ts = int(datetime.now(UTC).timestamp()) // 60 * 60
    await market_store.persist_stream_batch(
        bars=[("AAPL", "1min", ts, 1.0, 2.0, 0.5, 1.5, 1000, 10, 1.2)]
    )
    # updatedBar correction for the same minute
    await market_store.persist_stream_batch(
        bars=[("AAPL", "1min", ts, 1.0, 2.5, 0.5, 2.0, 1500, 15, 1.4)]
    )
    bars = await market_store.bars_window("AAPL", timeframe="1min", limit=10)
    assert len(bars) == 1
    assert bars[0]["close"] == 2.0
    assert bars[0]["volume"] == 1500


@pytest.mark.asyncio
async def test_bars_since_cursor(market_store):
    ts0 = int(datetime.now(UTC).timestamp()) // 60 * 60
    for i in range(5):
        await market_store.persist_stream_batch(
            bars=[("AAPL", "1min", ts0 + 60 * i, 1, 2, 0.5, 1.5, 100, 5, 1.1)]
        )
    page1, cursor1, more1 = await market_store.bars_since(cursor=0, limit=3)
    assert len(page1) == 3 and more1
    page2, cursor2, more2 = await market_store.bars_since(cursor=cursor1, limit=3)
    assert len(page2) == 2 and not more2
    # Upserting an existing bar bumps its seq → it resurfaces.
    await market_store.persist_stream_batch(
        bars=[("AAPL", "1min", ts0, 1, 3, 0.5, 2.5, 999, 9, 1.9)]
    )
    page3, _, _ = await market_store.bars_since(cursor=cursor2, limit=10)
    assert len(page3) == 1
    assert page3[0]["ts"] == ts0 and page3[0]["volume"] == 999


@pytest.mark.asyncio
async def test_latest_bar_ts(market_store):
    ts = 1_700_000_000 // 60 * 60
    await market_store.persist_stream_batch(
        bars=[
            ("AAPL", "1min", ts, 1, 2, 0.5, 1.5, 100, 5, 1.1),
            ("AAPL", "1min", ts + 60, 1, 2, 0.5, 1.5, 100, 5, 1.1),
            ("TSLA", "1min", ts, 1, 2, 0.5, 1.5, 100, 5, 1.1),
        ]
    )
    latest = await market_store.latest_bar_ts(["AAPL", "TSLA", "MSFT"])
    assert latest == {"AAPL": ts + 60, "TSLA": ts}


@pytest.mark.asyncio
async def test_prune_ticks_and_bars(market_store):
    now = datetime.now(UTC)
    old_us = _us(now - timedelta(hours=10))
    new_us = _us(now)
    await market_store.persist_stream_batch(
        trades=[
            ("AAPL", old_us, 1.0, 1, "V", None, "C", 1),
            ("AAPL", new_us, 2.0, 1, "V", None, "C", 2),
        ],
        bars=[
            ("AAPL", "1min", int((now - timedelta(days=40)).timestamp()), 1, 2, 0.5, 1.5, 1, 1, 1),
            ("AAPL", "1min", int(now.timestamp()) // 60 * 60, 1, 2, 0.5, 1.5, 1, 1, 1),
        ],
    )
    deleted = await market_store.prune(tick_retention_minutes=240, bar_retention_days=30)
    assert deleted["trades"] == 1
    assert deleted["bars"] == 1
    remaining = await market_store.trades_window(
        "AAPL", since_us=_us(now - timedelta(days=1)), limit=10
    )
    assert len(remaining) == 1 and remaining[0]["price"] == 2.0


@pytest.mark.asyncio
async def test_recent_statuses_and_lulds(market_store):
    now_us = _us(datetime.now(UTC))
    await market_store.persist_stream_batch(
        statuses=[("AAPL", now_us, "H", "Trading Halt", "T1", "News Pending", "C")],
        lulds=[("AAPL", now_us, 200.0, 180.0, "B", "C")],
    )
    statuses = await market_store.recent_statuses(minutes=5)
    assert statuses[0]["status_code"] == "H"
    lulds = await market_store.recent_lulds(minutes=5)
    assert lulds[0]["limit_up"] == 200.0


def test_snapshot_cache_keeps_newest():
    cache = MarketSnapshotCache()
    cache.update("AAPL", "trade", {"p": 100.0, "ts_us": 2000})
    cache.update("AAPL", "trade", {"p": 99.0, "ts_us": 1000})  # older — ignored
    snap = cache.get("aapl")
    assert snap is not None
    assert snap["trade"]["p"] == 100.0
    cache.update("AAPL", "quote", {"bp": 99.5, "ts_us": 3000})
    assert set(cache.get("AAPL").keys()) == {"trade", "quote"}
    assert cache.symbols() == ["AAPL"]

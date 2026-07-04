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
async def test_unchanged_bar_redelivery_does_not_advance_cursor(market_store):
    """Gap-fill redelivers whole windows; identical bars must not consume
    seqs — a phantom tail seq advances latest_cursor (and the persisted
    high-water mark) with nothing to deliver, leaving cursor pollers
    looking permanently behind."""
    ts = 1_700_000_000 // 60 * 60
    batch = [
        ("AAPL", "1min", ts, 1.0, 2.0, 0.5, 1.5, 1000, 10, 1.2),
        ("AAPL", "1min", ts + 60, 1.5, 2.5, 1.0, 2.0, 500, 5, 1.8),
    ]
    await market_store.persist_stream_batch(bars=batch)
    latest = market_store.latest_bar_cursor

    await market_store.persist_stream_batch(bars=list(batch))  # redelivery
    assert market_store.latest_bar_cursor == latest
    rows, next_cursor, has_more = await market_store.bars_since(
        cursor=latest, limit=10
    )
    assert rows == [] and next_cursor == latest and not has_more

    # A genuinely changed bar still consumes a seq and re-surfaces.
    await market_store.persist_stream_batch(
        bars=[("AAPL", "1min", ts, 1.0, 2.0, 0.5, 1.5, 1200, 12, 1.2)]
    )
    assert market_store.latest_bar_cursor == latest + 1
    rows, _, _ = await market_store.bars_since(cursor=latest, limit=10)
    assert len(rows) == 1 and rows[0]["volume"] == 1200


@pytest.mark.asyncio
async def test_raw_events_retained_beyond_tick_window(market_store):
    """Raw statuses/corrections are the low-volume audit trail — they keep
    the day-scale status retention, not the 4-hour tick window that would
    erase them long before their flattened counterparts."""
    now = datetime.now(UTC)
    await market_store.persist_stream_batch(
        raw_events=[("c", '{"T":"c"}'), ("s", '{"T":"s"}')]
    )
    # Backdate one row past the tick window but inside the status horizon.
    async with market_store._write_lock:
        await market_store.conn.execute(
            "UPDATE market_raw_events SET received_at = ? WHERE message_type = 'c'",
            ((now - timedelta(hours=10)).isoformat(),),
        )
        await market_store.conn.commit()
    deleted = await market_store.prune(
        tick_retention_minutes=240, bar_retention_days=30, status_retention_days=30
    )
    assert deleted["raw_events"] == 0

    # Past the status horizon it does get pruned.
    async with market_store._write_lock:
        await market_store.conn.execute(
            "UPDATE market_raw_events SET received_at = ? WHERE message_type = 'c'",
            ((now - timedelta(days=40)).isoformat(),),
        )
        await market_store.conn.commit()
    deleted = await market_store.prune(
        tick_retention_minutes=240, bar_retention_days=30, status_retention_days=30
    )
    assert deleted["raw_events"] == 1


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


@pytest.mark.asyncio
async def test_failed_batch_rolls_back(market_store):
    """A mid-batch failure must roll back everything already written in the
    transaction — a later successful write must not commit partial rows."""
    # Trades write fine, but a malformed LULD row (too few columns) fails
    # after the trades executemany succeeded.
    import sqlite3

    with pytest.raises(sqlite3.ProgrammingError):
        await market_store.persist_stream_batch(
            trades=[("AAPL", 1_700_000_000_000_000, 190.0, 100, "V", "@", "C", 1)],
            lulds=[("AAPL",)],
        )
    # A later good write commits only itself.
    await market_store.persist_stream_batch(
        quotes=[("AAPL", 1_700_000_000_000_001, 189.9, 2, "V", 190.1, 3, "V", "R", "C")],
    )
    trades = await market_store.trades_window(
        "AAPL", since_us=0, limit=10
    )
    assert trades == []  # the rolled-back trade never landed
    quotes = await market_store.quotes_window("AAPL", since_us=0, limit=10)
    assert len(quotes) == 1


@pytest.mark.asyncio
async def test_market_reads_use_isolated_connection(market_store):
    assert market_store._read_conn is not None
    assert market_store.rconn is market_store._read_conn

    # Committed data is visible through the read connection.
    now = datetime.now(UTC)
    await market_store.persist_stream_batch(
        trades=[("AAPL", _us(now), 190.0, 100, "V", "@", "C", 1)]
    )
    got = await market_store.trades_window(
        "AAPL", since_us=_us(now - timedelta(minutes=1)), limit=10
    )
    assert len(got) == 1


@pytest.mark.asyncio
async def test_memory_store_falls_back_to_writer_connection():
    store = await MarketStore.open(":memory:")
    await store.init_schema()
    try:
        assert store._read_conn is None
        assert store.rconn is store.conn
        # Reads still work on the shared connection.
        got = await store.trades_window("AAPL", since_us=0, limit=5)
        assert got == []
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_bar_seq_watermark_survives_prune_and_restart(tmp_path):
    path = str(tmp_path / "wm-market.sqlite")
    store = await MarketStore.open(path)
    await store.init_schema()
    old_ts = int((datetime.now(UTC) - timedelta(days=60)).timestamp()) // 60 * 60
    await store.persist_stream_batch(
        bars=[("AAPL", "1min", old_ts, 1.0, 2.0, 0.5, 1.5, 1000, 10, 1.2)]
    )
    _, cursor, _ = await store.bars_since(cursor=0, limit=10)
    assert cursor >= 1
    # Retention removes every bar (60 days old vs 30-day retention).
    await store.prune(tick_retention_minutes=240, bar_retention_days=30)
    rows, _, _ = await store.bars_since(cursor=0, limit=10)
    assert rows == []
    await store.close()

    store = await MarketStore.open(path)
    await store.init_schema()
    try:
        new_ts = int(datetime.now(UTC).timestamp()) // 60 * 60
        await store.persist_stream_batch(
            bars=[("AAPL", "1min", new_ts, 1.0, 2.0, 0.5, 1.5, 1000, 10, 1.2)]
        )
        rows, _, _ = await store.bars_since(cursor=cursor, limit=10)
        assert [r["ts"] for r in rows] == [new_ts]
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_active_halt_survives_tick_retention(market_store):
    """Statuses/LULDs are pruned on a day-scale retention, not the 4-hour
    tick window — an active halt must stay visible to get_trading_halts."""
    now = datetime.now(UTC)
    five_h_ago = _us(now - timedelta(hours=5))
    await market_store.persist_stream_batch(
        trades=[("AAPL", five_h_ago, 190.0, 100, "V", "@", "C", 1)],
        statuses=[("AAPL", five_h_ago, "H", "Trading Halt", "T1", "News", "C")],
        lulds=[("AAPL", five_h_ago, 200.0, 180.0, "B", "C")],
    )
    deleted = await market_store.prune(
        tick_retention_minutes=240, bar_retention_days=30, status_retention_days=30
    )
    assert deleted["trades"] == 1  # the tick is gone
    statuses = await market_store.recent_statuses(minutes=24 * 60, limit=10)
    assert len(statuses) == 1  # the halt survives
    lulds = await market_store.recent_lulds(minutes=24 * 60, limit=10)
    assert len(lulds) == 1

    # Events older than the status retention do get pruned.
    deleted = await market_store.prune(
        tick_retention_minutes=240, bar_retention_days=30, status_retention_days=0
    )
    assert deleted["statuses"] == 1 and deleted["lulds"] == 1

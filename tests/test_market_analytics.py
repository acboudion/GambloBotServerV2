"""Pure indicator math + the analytics tools built on it."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from alpaca_news_mcp import market_analytics as ma


def _bar(ts, o, h, lo, c, v=1000, vw=None):
    return {"ts": ts, "open": o, "high": h, "low": lo, "close": c,
            "volume": v, "trade_count": 10, "vwap": vw if vw is not None else c}


def test_rsi_hand_computed():
    # 15 closes with known Wilder RSI. All gains -> 100.
    rising = [_bar(i, c, c, c, c) for i, c in enumerate(range(1, 17))]
    assert ma.rsi(rising, period=14) == 100.0
    falling = [_bar(i, c, c, c, c) for i, c in enumerate(range(17, 1, -1))]
    assert ma.rsi(falling, period=14) == 0.0
    assert ma.rsi(rising[:10], period=14) is None  # insufficient data
    # Alternating +1/-1: avg gain == avg loss -> RSI ~50.
    closes = [10 + (1 if i % 2 else 0) for i in range(20)]
    alt = [_bar(i, c, c, c, c) for i, c in enumerate(closes)]
    val = ma.rsi(alt, period=14)
    assert val is not None and 40 <= val <= 60
    # Completely flat closes: zero gains AND zero losses is neutral — a
    # halted/illiquid symbol must not read as overbought RSI 100.
    flat = [_bar(i, 10, 10, 10, 10) for i in range(20)]
    assert ma.rsi(flat, period=14) == 50.0


def test_ema_and_atr():
    bars = [_bar(i, c, c + 1, c - 1, c) for i, c in enumerate(range(1, 22))]
    ema9 = ma.ema(bars, 9)
    assert ema9 is not None and 16 < ema9 < 21  # tracks the rising tail
    # Constant 2.0 true range (high-low) -> ATR == 2.
    assert ma.atr(bars, period=14) == 2.0
    assert ma.ema(bars[:5], 9) is None


def test_vwap_gap_range_relvol():
    bars = [_bar(0, 10, 11, 9, 10, v=100, vw=10.0),
            _bar(60, 10, 12, 10, 12, v=300, vw=12.0)]
    assert ma.session_vwap(bars) == pytest.approx(11.5)
    assert ma.gap_pct(102.0, 100.0) == 2.0
    assert ma.pct_change(105.0, 100.0) == 5.0
    assert ma.day_range_position(11.0, 12.0, 10.0) == 0.5
    assert ma.day_range_position(11.0, 10.0, 10.0) is None
    # Half the session elapsed, half the average volume -> rel_vol 1.0.
    assert ma.relative_volume(500.0, [1000.0, 1000.0], 0.5) == 1.0
    assert ma.relative_volume(0.0, [1000.0], 0.5) is None


@pytest.mark.asyncio
async def test_bars_window_aggregated_golden(tmp_path):
    """12 known 1min bars -> 5min buckets with hand-computed OHLCV."""
    from alpaca_news_mcp.market_store import MarketStore

    store = await MarketStore.open(str(tmp_path / "m.sqlite"))
    await store.init_schema()
    try:
        base = 1_760_000_100  # NOT bucket-aligned; buckets align to ts%300
        base -= base % 300
        rows = []
        for i in range(12):
            ts = base + i * 60
            rows.append(("AAPL", "1min", ts,
                         100.0 + i, 101.0 + i, 99.0 + i, 100.5 + i,
                         100 * (i + 1), 5, 100.25 + i))
        await store.persist_stream_batch(bars=rows)
        agg = await store.bars_window_aggregated("AAPL", bucket_seconds=300, limit=10)
        assert len(agg) == 3  # 12 minutes -> buckets of 5+5+2
        first = agg[0]
        assert first["ts"] == base
        assert first["open"] == 100.0          # first bar's open
        assert first["close"] == 100.5 + 4     # 5th bar's close
        assert first["high"] == 101.0 + 4
        assert first["low"] == 99.0
        assert first["volume"] == sum(100 * (i + 1) for i in range(5))
        # Volume-weighted vwap across the bucket.
        expected_vwap = sum((100.25 + i) * 100 * (i + 1) for i in range(5)) / sum(
            100 * (i + 1) for i in range(5)
        )
        assert first["vwap"] == pytest.approx(expected_vwap)
        last = agg[-1]
        assert last["volume"] == 100 * 11 + 100 * 12
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_bars_window_aggregated_ignores_null_vwap_volume(tmp_path):
    """A minute bar with NULL vwap contributes nothing to the numerator, so
    its volume must not inflate the denominator and bias the bucket vwap
    toward zero."""
    from alpaca_news_mcp.market_store import MarketStore

    store = await MarketStore.open(str(tmp_path / "m.sqlite"))
    await store.init_schema()
    try:
        base = 1_760_000_100
        base -= base % 300
        await store.persist_stream_batch(bars=[
            ("AAPL", "1min", base, 10.0, 10.0, 10.0, 10.0, 100, 5, 10.0),
            ("AAPL", "1min", base + 60, 10.0, 10.0, 10.0, 10.0, 300, 5, None),
        ])
        agg = await store.bars_window_aggregated("AAPL", bucket_seconds=300, limit=10)
        assert len(agg) == 1
        # Weighted only over the row that has a vwap: 10.0, not
        # 10*100/(100+300) = 2.5.
        assert agg[0]["vwap"] == pytest.approx(10.0)
        assert agg[0]["volume"] == 400  # total volume still counts both rows
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_tape_and_quote_stats(tmp_path):
    from alpaca_news_mcp.market_store import MarketStore

    store = await MarketStore.open(str(tmp_path / "m.sqlite"))
    await store.init_schema()
    try:
        now = datetime.now(UTC)
        base_us = int((now - timedelta(minutes=2)).timestamp() * 1_000_000)
        bucket_us = 60 * 1_000_000
        base_us -= base_us % bucket_us
        trades = [
            ("AAPL", base_us + 1_000_000, 100.0, 100, "V", "@", "C", 1),
            ("AAPL", base_us + 2_000_000, 101.0, 2000, "V", "@", "C", 2),  # uptick+block
            ("AAPL", base_us + 3_000_000, 100.5, 50, "V", "@", "C", 3),   # downtick
        ]
        quotes = [
            ("AAPL", base_us + 1_500_000, 100.0, 5, "V", 100.2, 5, "V", "R", "C"),
        ]
        await store.persist_stream_batch(trades=trades, quotes=quotes)
        stats = await store.tape_stats(
            "AAPL", since_us=base_us, bucket_us=bucket_us, block_size=1000
        )
        assert len(stats) == 1
        b = stats[0]
        assert b["trades"] == 3 and b["upticks"] == 1 and b["downticks"] == 1
        assert b["block_trades"] == 1
        qs = await store.quote_stats("AAPL", since_us=base_us, bucket_us=bucket_us)
        assert len(qs) == 1
        assert qs[0]["spread_bps"] == pytest.approx(
            (100.2 - 100.0) * 20000.0 / (100.2 + 100.0)
        )
        pulse = await store.watchlist_pulse(["AAPL"], since_us=base_us)
        assert pulse["AAPL"]["first_price"] == 100.0
        assert pulse["AAPL"]["last_price"] == 100.5
        assert pulse["AAPL"]["volume"] == 2150
    finally:
        await store.close()

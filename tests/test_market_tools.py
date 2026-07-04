"""Market MCP tools: bounds, snapshot-vs-DB paths, compact shapes, disabled mode."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.app_state import AppState, clear_app_state, set_app_state
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.market_store import MarketStore
from alpaca_news_mcp.rest_backfill import RestBackfillWorker
from alpaca_news_mcp.server import build_mcp
from alpaca_news_mcp.state import State
from alpaca_news_mcp.stock_stream import StockStreamWorker
from alpaca_news_mcp.store import Store
from alpaca_news_mcp.stream import NewsStreamWorker


def _us(dt: datetime) -> int:
    return int(dt.timestamp() * 1_000_000)


@pytest.fixture
async def app(tmp_path, monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "news.sqlite"))
    monkeypatch.setenv("MARKET_STORAGE_PATH", str(tmp_path / "market.sqlite"))
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "false")
    monkeypatch.setenv("STOCK_WATCHLIST_SYMBOLS", "AAPL,TSLA")
    cfg = Config.from_env()
    store = await Store.open(cfg.storage_path)
    await store.init_schema()
    market_store = await MarketStore.open(cfg.market_storage_path)
    await market_store.init_schema()
    state = State()
    alerts = AlertEngine()
    rest = RestBackfillWorker(cfg, store, state, alerts)
    NewsStreamWorker.reset_singleton()
    StockStreamWorker.reset_singleton()
    stream = NewsStreamWorker(cfg, store, state, alerts)
    stock_stream = StockStreamWorker(cfg, store, market_store, state, alerts)
    app_state = AppState(
        config=cfg, store=store, state=state, alerts=alerts,
        stream=stream, rest_backfill=rest,
        market_store=market_store, stock_stream=stock_stream,
    )
    set_app_state(app_state)
    yield app_state
    clear_app_state()
    await rest.close()
    await store.close()
    await market_store.close()
    NewsStreamWorker.reset_singleton()
    StockStreamWorker.reset_singleton()


async def _call(mcp, tool_name: str, **kwargs):
    fn = mcp._tool_manager._tools[tool_name].fn
    return await fn(**kwargs)


@pytest.mark.asyncio
async def test_stock_stream_health_tool(app):
    mcp = build_mcp()
    out = await _call(mcp, "get_stock_stream_health")
    assert out["service"] == "alpaca-stock-stream"
    assert out["watchlist"] == ["AAPL", "TSLA"]
    assert "queue_depth" in out
    assert "ingest_counts_15m" in out


@pytest.mark.asyncio
async def test_watchlist_tools(app):
    mcp = build_mcp()
    out = await _call(mcp, "set_stock_watchlist", symbols=["nvda"], mode="add")
    assert out["watchlist"] == ["AAPL", "NVDA", "TSLA"]
    got = await _call(mcp, "get_stock_watchlist")
    assert got["watchlist"] == ["AAPL", "NVDA", "TSLA"]
    assert "channels" in got


@pytest.mark.asyncio
async def test_latest_market_data_memory_path(app):
    mcp = build_mcp()
    app.market_store.snapshots.update("AAPL", "trade", {"p": 190.5, "ts_us": 1})
    app.market_store.snapshots.update("AAPL", "quote", {"bp": 190.4, "ap": 190.6, "ts_us": 2})
    out = await _call(mcp, "get_latest_market_data", symbols=["AAPL", "MSFT"])
    assert out["symbols"]["AAPL"]["trade"]["p"] == 190.5
    assert out["symbols"]["AAPL"]["quote"]["bp"] == 190.4
    assert out["missing"] == ["MSFT"]
    assert out["source"] == "stream"
    # AAPL's stream snapshot has no minute bar yet — a partial stream entry
    # must be reported, not silently returned as if complete.
    assert out["missing_fields"] == {"AAPL": ["bar"]}

    # status/luld are event-driven; their absence is not a missing field.
    ev = await _call(mcp, "get_latest_market_data", symbols=["AAPL"],
                     include=["trade", "status"])
    assert ev["missing_fields"] == {}

    bad = await _call(mcp, "get_latest_market_data", symbols=["AAPL"], include=["bogus"])
    assert bad["error"] == "invalid_include"
    over = await _call(mcp, "get_latest_market_data", symbols=[f"S{i}" for i in range(51)])
    assert over["error"] == "limit_exceeded"
    empty = await _call(mcp, "get_latest_market_data", symbols=[])
    assert empty["error"] == "no_symbols"


@pytest.mark.asyncio
async def test_windows_compact_shapes_and_bounds(app):
    mcp = build_mcp()
    now = datetime.now(UTC)
    await app.market_store.persist_stream_batch(
        trades=[("AAPL", _us(now), 190.0, 100, "V", "@", "C", 1)],
        quotes=[("AAPL", _us(now), 189.9, 2, "V", 190.1, 3, "V", "R", "C")],
    )
    trades = await _call(mcp, "get_trades_window", symbol="aapl")
    assert trades["count"] == 1
    assert trades["columns"] == ["ts_us", "price", "size", "exchange"]
    assert trades["trades"][0][1] == 190.0

    quotes = await _call(mcp, "get_quotes_window", symbol="AAPL")
    assert quotes["quotes"][0][1] == 189.9 and quotes["quotes"][0][3] == 190.1

    assert (await _call(mcp, "get_trades_window", symbol="A", minutes=999))["error"] == "limit_exceeded"
    assert (await _call(mcp, "get_quotes_window", symbol="A", limit=99999))["error"] == "limit_exceeded"


@pytest.mark.asyncio
async def test_stock_bars_tool_and_cursor(app):
    mcp = build_mcp()
    base = int(datetime.now(UTC).timestamp()) // 60 * 60
    await app.market_store.persist_stream_batch(
        bars=[
            ("AAPL", "1min", base - 120, 1, 2, 0.5, 1.5, 100, 5, 1.1),
            ("AAPL", "1min", base - 60, 1.5, 2.5, 1.0, 2.0, 200, 8, 1.9),
        ]
    )
    out = await _call(mcp, "get_stock_bars", symbol="AAPL")
    assert out["count"] == 2
    assert out["bars"][0][0] < out["bars"][1][0]  # ascending

    start_iso = datetime.fromtimestamp(base - 60, tz=UTC).isoformat()
    out2 = await _call(mcp, "get_stock_bars", symbol="AAPL", start=start_iso)
    assert out2["count"] == 1

    assert (await _call(mcp, "get_stock_bars", symbol="A", timeframe="2min"))["error"] == "invalid_timeframe"

    cur = await _call(mcp, "get_bars_since", cursor=0, limit=1)
    assert cur["count"] == 1 and cur["has_more"] is True
    cur2 = await _call(mcp, "get_bars_since", cursor=cur["next_cursor"])
    assert cur2["count"] == 1 and cur2["has_more"] is False


@pytest.mark.asyncio
async def test_trading_halts_tool(app):
    mcp = build_mcp()
    now_us = _us(datetime.now(UTC) - timedelta(minutes=1))
    await app.market_store.persist_stream_batch(
        statuses=[("AAPL", now_us, "H", "Trading Halt", "T1", "News Pending", "C")],
        lulds=[("AAPL", now_us, 200.0, 180.0, "B", "C")],
    )
    out = await _call(mcp, "get_trading_halts")
    assert out["statuses"][0]["status_code"] == "H"
    assert out["lulds"][0]["limit_up"] == 200.0
    filtered = await _call(mcp, "get_trading_halts", symbols=["TSLA"])
    assert filtered["statuses"] == []


@pytest.mark.asyncio
async def test_tools_report_disabled_without_stock_stream(app):
    mcp = build_mcp()
    app.stock_stream = None
    app.market_store = None
    for tool, kwargs in [
        ("get_stock_stream_health", {}),
        ("set_stock_watchlist", {"symbols": ["A"]}),
        ("get_stock_watchlist", {}),
        ("get_latest_market_data", {"symbols": ["A"]}),
        ("get_trades_window", {"symbol": "A"}),
        ("get_quotes_window", {"symbol": "A"}),
        ("get_stock_bars", {"symbol": "A"}),
        ("get_bars_since", {}),
        ("get_trading_halts", {}),
    ]:
        out = await _call(mcp, tool, **kwargs)
        assert out["error"] == "stock_stream_disabled", tool


# ---- REST market-context tools (respx-mocked) --------------------------------

import httpx  # noqa: E402
import respx  # noqa: E402

from alpaca_news_mcp.market_client import MarketDataClient  # noqa: E402


@pytest.fixture
async def app_with_client(app):
    client = MarketDataClient(app.config)
    app.market_client = client
    yield app
    await client.close()


@pytest.mark.asyncio
@respx.mock
async def test_market_movers_tool(app_with_client):
    mcp = build_mcp()
    respx.get("https://data.alpaca.markets/v1beta1/screener/stocks/movers").mock(
        return_value=httpx.Response(
            200,
            json={"gainers": [{"symbol": "UP", "percent_change": 15.2, "price": 12.5}],
                  "losers": [{"symbol": "DN", "percent_change": -9.9, "price": 3.2}],
                  "market_type": "stocks", "last_updated": "2026-07-02T14:00:00Z"},
        )
    )
    out = await _call(mcp, "get_market_movers", top=5)
    assert out["gainers"][0]["symbol"] == "UP"
    assert out["losers"][0]["symbol"] == "DN"
    over = await _call(mcp, "get_market_movers", top=999)
    assert over["error"] == "limit_exceeded"


@pytest.mark.asyncio
@respx.mock
async def test_most_active_stocks_tool(app_with_client):
    mcp = build_mcp()
    respx.get("https://data.alpaca.markets/v1beta1/screener/stocks/most-actives").mock(
        return_value=httpx.Response(
            200, json={"most_actives": [{"symbol": "TSLA", "volume": 99}]}
        )
    )
    out = await _call(mcp, "get_most_active_stocks", by="volume", top=5)
    assert out["most_actives"][0]["symbol"] == "TSLA"
    bad = await _call(mcp, "get_most_active_stocks", by="bogus")
    assert bad["error"] == "invalid_by"


@pytest.mark.asyncio
@respx.mock
async def test_stock_snapshots_tool(app_with_client):
    mcp = build_mcp()
    respx.get("https://data.alpaca.markets/v2/stocks/snapshots").mock(
        return_value=httpx.Response(
            200,
            json={"AAPL": {"latestTrade": {"p": 190.5}, "latestQuote": {"bp": 190.4},
                           "minuteBar": {"c": 190.5}, "dailyBar": {"c": 190.0},
                           "prevDailyBar": {"c": 188.0}}},
        )
    )
    out = await _call(mcp, "get_stock_snapshots", symbols=["aapl"])
    snap = out["snapshots"]["AAPL"]
    assert snap["latest_trade"]["p"] == 190.5
    assert snap["prev_daily_bar"]["c"] == 188.0
    assert (await _call(mcp, "get_stock_snapshots", symbols=[]))["error"] == "no_symbols"


@pytest.mark.asyncio
@respx.mock
async def test_market_clock_and_calendar_tools(app_with_client):
    mcp = build_mcp()
    respx.get("https://paper-api.alpaca.markets/v2/clock").mock(
        return_value=httpx.Response(
            200, json={"timestamp": "t", "is_open": False, "next_open": "o", "next_close": "c"}
        )
    )
    clock = await _call(mcp, "get_market_clock")
    assert clock["is_open"] is False

    respx.get("https://paper-api.alpaca.markets/v2/calendar").mock(
        return_value=httpx.Response(
            200, json=[{"date": "2026-07-06", "open": "09:30", "close": "16:00"}]
        )
    )
    cal = await _call(mcp, "get_market_calendar")
    assert cal["days"][0]["date"] == "2026-07-06"
    bad = await _call(mcp, "get_market_calendar", start="2026-01-01", end="2026-12-31")
    assert bad["error"] == "limit_exceeded"


@pytest.mark.asyncio
@respx.mock
async def test_corporate_actions_tool(app_with_client):
    mcp = build_mcp()
    respx.get("https://data.alpaca.markets/v1/corporate-actions").mock(
        return_value=httpx.Response(
            200,
            json={"corporate_actions": {"forward_splits": [{"symbol": "NVDA"}]},
                  "next_page_token": None},
        )
    )
    out = await _call(mcp, "get_corporate_actions", symbols=["NVDA"])
    assert out["corporate_actions"]["forward_splits"][0]["symbol"] == "NVDA"
    assert out["has_more"] is False
    bad = await _call(mcp, "get_corporate_actions", start="2026-01-01", end="2026-12-31")
    assert bad["error"] == "limit_exceeded"


@pytest.mark.asyncio
@respx.mock
async def test_latest_market_data_rest_fallback(app_with_client):
    mcp = build_mcp()
    app_with_client.market_store.snapshots.update("AAPL", "trade", {"p": 190.5, "ts_us": 1})
    respx.get("https://data.alpaca.markets/v2/stocks/trades/latest").mock(
        return_value=httpx.Response(200, json={"trades": {"MSFT": {"p": 400.0}}})
    )
    respx.get("https://data.alpaca.markets/v2/stocks/quotes/latest").mock(
        return_value=httpx.Response(200, json={"quotes": {"MSFT": {"bp": 399.9}}})
    )
    respx.get("https://data.alpaca.markets/v2/stocks/bars/latest").mock(
        return_value=httpx.Response(200, json={"bars": {"MSFT": {"c": 400.1}}})
    )
    out = await _call(mcp, "get_latest_market_data", symbols=["AAPL", "MSFT"])
    assert out["symbols"]["AAPL"]["trade"]["p"] == 190.5
    assert out["symbols"]["MSFT"]["trade"]["p"] == 400.0
    assert out["missing"] == []
    # MSFT was fully filled by REST; AAPL's stream snapshot only has a trade.
    assert out["missing_fields"] == {"AAPL": ["bar", "quote"]}
    assert out["source"] == "stream+rest"


@pytest.mark.asyncio
@respx.mock
async def test_latest_market_data_partial_rest_fill_reports_missing_fields(app_with_client):
    """A symbol that got only some of the requested REST fields must not look
    complete: unfilled fields are reported per symbol, and symbols with no
    data at all stay in `missing`."""
    mcp = build_mcp()
    respx.get("https://data.alpaca.markets/v2/stocks/trades/latest").mock(
        return_value=httpx.Response(200, json={"trades": {"MSFT": {"p": 400.0}}})
    )
    respx.get("https://data.alpaca.markets/v2/stocks/quotes/latest").mock(
        return_value=httpx.Response(200, json={"quotes": {}})
    )
    respx.get("https://data.alpaca.markets/v2/stocks/bars/latest").mock(
        return_value=httpx.Response(500, json={"error": "boom"})
    )
    out = await _call(mcp, "get_latest_market_data", symbols=["MSFT", "ZZZZ"])
    assert out["symbols"]["MSFT"]["trade"]["p"] == 400.0
    assert out["missing"] == ["ZZZZ"]
    assert out["missing_fields"] == {"MSFT": ["bar", "quote"]}
    assert out["source"] == "stream+rest"


@pytest.mark.asyncio
async def test_context_tools_without_client(app):
    mcp = build_mcp()
    app.market_client = None
    for tool in ("get_market_movers", "get_most_active_stocks", "get_market_clock",
                 "get_market_calendar"):
        out = await _call(mcp, tool)
        assert out["error"] == "market_client_unavailable", tool
    out = await _call(mcp, "get_stock_snapshots", symbols=["A"])
    assert out["error"] == "market_client_unavailable"
    out = await _call(mcp, "get_corporate_actions")
    assert out["error"] == "market_client_unavailable"


@pytest.mark.asyncio
async def test_invalid_dates_return_structured_errors(app_with_client):
    """A malformed caller-supplied date must be a structured error, not a
    silent fallback to the default window (valid-looking data, wrong dates)."""
    mcp = build_mcp()
    cal = await _call(mcp, "get_market_calendar", start="2026-13-01")
    assert (cal["error"], cal["field"], cal["value"]) == ("invalid_date", "start", "2026-13-01")
    assert "hint" in cal
    ca = await _call(mcp, "get_corporate_actions", end="not-a-date")
    assert (ca["error"], ca["field"], ca["value"]) == ("invalid_date", "end", "not-a-date")
    bars = await _call(mcp, "get_stock_bars", symbol="AAPL", start="garbage")
    assert (bars["error"], bars["field"], bars["value"]) == ("invalid_date", "start", "garbage")


@pytest.mark.asyncio
async def test_get_bars_since_caps_symbol_count(app):
    """symbols feeds a per-symbol SQL placeholder list — reject oversized
    lists structurally instead of hitting SQLite's variable limit."""
    mcp = build_mcp()
    out = await _call(
        mcp, "get_bars_since", symbols=[f"S{i}" for i in range(51)]
    )
    assert out["error"] == "limit_exceeded"
    assert out["max_allowed"] == 50


@pytest.mark.asyncio
async def test_get_trading_halts_caps_symbol_count(app):
    """symbols feeds per-symbol SQL placeholders in two queries — oversized
    lists must be rejected structurally."""
    mcp = build_mcp()
    out = await _call(
        mcp, "get_trading_halts", symbols=[f"S{i}" for i in range(51)]
    )
    assert out["error"] == "limit_exceeded"
    assert out["max_allowed"] == 50


@pytest.mark.asyncio
async def test_latest_market_data_staleness_signal(app):
    """Pre-disconnect cache entries must be flagged: age_seconds always,
    stale when the stream is down or the data is old."""
    mcp = build_mcp()
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    old_us = int((datetime.now(UTC) - timedelta(minutes=10)).timestamp() * 1_000_000)
    app.market_store.snapshots.update("AAPL", "trade", {"p": 190.5, "ts_us": now_us})
    app.market_store.snapshots.update("TSLA", "trade", {"p": 250.0, "ts_us": old_us})

    # Stream disconnected (default health): everything from the cache is stale.
    out = await _call(mcp, "get_latest_market_data", symbols=["AAPL", "TSLA"])
    assert out["stream_connected"] is False
    assert set(out["stale"]) == {"AAPL", "TSLA"}

    # Stream connected: only the old entry is stale.
    app.state.update_health("stocks", connected=True)
    out = await _call(mcp, "get_latest_market_data", symbols=["AAPL", "TSLA"])
    assert out["stream_connected"] is True
    assert out.get("stale") == ["TSLA"]
    assert out["age_seconds"]["AAPL"] < 60
    assert out["age_seconds"]["TSLA"] > 300


@pytest.mark.asyncio
async def test_trading_halts_window_clamps_to_retention(app):
    mcp = build_mcp()
    out = await _call(mcp, "get_trading_halts", minutes=10_000_000)
    assert out["clamped_to_minutes"] == app.config.status_retention_days * 1440
    assert out["window_minutes"] == out["clamped_to_minutes"]


@pytest.mark.asyncio
async def test_get_symbol_context_scalars(app):
    """~15 scalars from local bars + snapshot; unknown symbols get a
    per-symbol structured error without failing the call."""
    now = datetime.now(UTC)
    now_min = int(now.timestamp()) // 60 * 60
    today_midnight = now_min - (now_min % 86_400)
    bars = []
    for i in range(30):
        ts = now_min - (30 - i) * 60
        c = 100.0 + i * 0.5
        bars.append(("AAPL", "1min", ts, c - 0.2, c + 0.3, c - 0.5, c,
                     1000, 10, c))
    # Yesterday's daily bar for prev_close / rel-volume baseline.
    bars.append(("AAPL", "1day", today_midnight - 86_400,
                 95.0, 116.0, 94.0, 100.0, 390_000, 3900, 105.0))
    await app.market_store.persist_stream_batch(bars=bars)
    app.market_store.snapshots.update(
        "AAPL", "trade", {"p": 115.5, "ts_us": int(now.timestamp() * 1e6)}
    )
    mcp = build_mcp()
    out = await _call(mcp, "get_symbol_context", symbols=["AAPL", "ZZZQ"])
    ctx = out["symbols"]["AAPL"]
    assert ctx["last"] == 115.5
    assert ctx["prev_close"] == 100.0
    assert ctx["change_pct"] == pytest.approx(15.5)
    assert ctx["rsi_14"] == 100.0  # strictly rising closes
    assert ctx["session_vwap"] is not None
    assert ctx["range_position"] is not None
    assert ctx["rel_volume"] is not None
    assert ctx["halted"] is False
    assert out["symbols"]["ZZZQ"]["error"] == "no_local_data"


@pytest.mark.asyncio
async def test_symbol_context_halted_from_retained_status_rows(app):
    """Same restart-mid-halt scenario as the pulse tools: a halt that exists
    only in stock_statuses (empty snapshot cache) must still flip the
    context's halted flag — the context block must not present a
    non-tradable name as tradable."""
    now = datetime.now(UTC)
    now_min = int(now.timestamp()) // 60 * 60
    now_us = int(now.timestamp() * 1_000_000)
    await app.market_store.persist_stream_batch(
        bars=[("AAPL", "1min", now_min - 60, 100.0, 101.0, 99.5, 100.5,
               1000, 10, 100.2)],
        statuses=[("AAPL", now_us - 2_000_000, "H", "Trading Halt", "T1",
                   "News Pending", "C")],
    )
    mcp = build_mcp()
    out = await _call(mcp, "get_symbol_context", symbols=["AAPL"])
    assert out["symbols"]["AAPL"]["halted"] is True


@pytest.mark.asyncio
async def test_get_stock_bars_aggregated_timeframes(app):
    now_min = int(datetime.now(UTC).timestamp()) // 300 * 300
    rows = [("AAPL", "1min", now_min + i * 60, 1.0 + i, 2.0 + i, 0.5,
             1.5 + i, 100, 5, 1.2 + i) for i in range(10)]
    await app.market_store.persist_stream_batch(bars=rows)
    mcp = build_mcp()
    out = await _call(mcp, "get_stock_bars", symbol="AAPL", timeframe="5min")
    assert out["count"] == 2
    assert out["bars"][0][1] == 1.0  # first bucket opens at first bar's open
    bad = await _call(mcp, "get_stock_bars", symbol="AAPL", timeframe="3min")
    assert bad["error"] == "invalid_timeframe"


@pytest.mark.asyncio
@respx.mock
async def test_get_stock_bars_aggregate_rest_fallback(app_with_client):
    """Aggregate timeframes on an empty local store must fall back to REST
    like 1min/1day do: fetch the underlying minute bars, aggregate them
    server-side, and report source='rest' — not return an empty result."""
    minute_bars = [
        {"t": f"2026-07-02T13:{30 + i}:00Z", "o": 1.0 + i, "h": 2.0 + i,
         "l": 0.5, "c": 1.5 + i, "v": 100, "n": 5, "vw": 1.2 + i}
        for i in range(10)
    ]
    respx.get("https://data.alpaca.markets/v2/stocks/bars").mock(
        return_value=httpx.Response(
            200, json={"bars": {"AAPL": minute_bars}, "next_page_token": None}
        )
    )
    mcp = build_mcp()
    out = await _call(mcp, "get_stock_bars", symbol="AAPL", timeframe="5min",
                      start="2026-07-02T13:00:00Z")
    assert out["source"] == "rest"
    assert out["count"] == 2  # 10 minute bars -> two 5min buckets
    rows = [dict(zip(out["columns"], r, strict=True)) for r in out["bars"]]
    assert rows[0]["open"] == 1.0 and rows[0]["close"] == 5.5
    assert rows[0]["high"] == 6.0 and rows[0]["low"] == 0.5
    assert rows[0]["volume"] == 500 and rows[0]["trade_count"] == 25
    assert rows[0]["vwap"] == pytest.approx(3.2)  # volume-weighted
    assert rows[1]["open"] == 6.0


@pytest.mark.asyncio
async def test_get_watchlist_pulse_rows(app):
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    await app.market_store.persist_stream_batch(trades=[
        ("AAPL", now_us - 2_000_000, 100.0, 100, "V", "@", "C", 1),
        ("AAPL", now_us - 1_000_000, 101.0, 200, "V", "@", "C", 2),
    ])
    mcp = build_mcp()
    out = await _call(mcp, "get_watchlist_pulse", minutes=5)
    assert out["columns"][0] == "symbol"
    row = dict(zip(out["columns"], out["rows"][0], strict=True))
    assert row["symbol"] == "AAPL"
    assert row["last"] == 101.0
    assert row["chg_pct"] == pytest.approx(1.0)
    assert row["volume"] == 300


@pytest.mark.asyncio
async def test_watchlist_pulse_includes_inactive_symbols(app):
    """A watchlist symbol with no trades in the window (halted/illiquid)
    must appear with zero activity and its halted flag — not vanish."""
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    await app.market_store.persist_stream_batch(trades=[
        ("AAPL", now_us - 1_000_000, 100.0, 100, "V", "@", "C", 1),
    ])
    # TSLA: no trades, but a cached halt status.
    app.market_store.snapshots.update(
        "TSLA", "status", {"sc": "H", "ts_us": now_us}
    )
    mcp = build_mcp()
    out = await _call(mcp, "get_watchlist_pulse", minutes=5)
    rows = {r[0]: dict(zip(out["columns"], r, strict=True)) for r in out["rows"]}
    assert set(rows) == {"AAPL", "TSLA"}
    assert rows["AAPL"]["trades"] == 1
    assert rows["TSLA"]["trades"] == 0
    assert rows["TSLA"]["last"] is None
    assert rows["TSLA"]["halted"] is True


@pytest.mark.asyncio
async def test_watchlist_pulse_halted_from_retained_status_rows(app):
    """A halt that predates this process (restart mid-halt, or a symbol added
    after its halt event) exists only in stock_statuses — the snapshot cache
    has no status. The retained row must still set halted=true, and a newer
    retained resume must clear it."""
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    await app.market_store.persist_stream_batch(statuses=[
        ("TSLA", now_us - 3_000_000, "H", "Trading Halt", "T1", "News Pending", "C"),
        ("AAPL", now_us - 3_000_000, "H", "Trading Halt", "T1", "News Pending", "C"),
        ("AAPL", now_us - 1_000_000, "T", "Trading Resumption", "", "", "C"),
    ])
    mcp = build_mcp()
    out = await _call(mcp, "get_watchlist_pulse", minutes=5)
    rows = {r[0]: dict(zip(out["columns"], r, strict=True)) for r in out["rows"]}
    assert rows["TSLA"]["halted"] is True  # DB-only halt, empty snapshot cache
    assert rows["AAPL"]["halted"] is False  # newest retained row is a resume


@pytest.mark.asyncio
async def test_market_pulse_checks_halts_beyond_first_50_candidates(app, monkeypatch):
    """Every discovery candidate gets a real halt check, even past the
    50-symbol-per-query slice."""
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    # Halt the symbol that lands LAST in insertion order (past slot 50).
    await app.market_store.persist_stream_batch(
        statuses=[("ZZT60", now_us, "H", "Trading Halt", "T1", "News", "C")]
    )

    class _Client:
        async def movers(self, top=10):
            gainers = [{"symbol": f"GG{i:02d}", "price": 10.0,
                        "percent_change": 5.0, "volume": 1000} for i in range(30)]
            losers = [{"symbol": f"LL{i:02d}", "price": 10.0,
                       "percent_change": -5.0, "volume": 1000} for i in range(30)]
            return {"gainers": gainers, "losers": losers}

        async def most_actives(self, by="volume", top=10):
            actives = [{"symbol": f"ZZT{i:02d}", "volume": 9000} for i in range(61)]
            return {"most_actives": actives}

    monkeypatch.setattr(app, "market_client", _Client())
    mcp = build_mcp()
    out = await _call(mcp, "get_market_pulse", top=50)
    assert out["count"] > 50
    by_symbol = {c["symbol"]: c for c in out["candidates"]}
    assert by_symbol["ZZT60"]["halted"] is True


@pytest.mark.asyncio
async def test_market_pulse_sees_halts_older_than_four_hours(app, monkeypatch):
    """News-pending/regulatory halts can outlive a 4h window; the halt check
    spans the full status retention so an old, un-resumed halt still flags."""
    ten_hours_ago = int(
        (datetime.now(UTC) - timedelta(hours=10)).timestamp() * 1_000_000
    )
    await app.market_store.persist_stream_batch(
        statuses=[("OLDH", ten_hours_ago, "H", "Trading Halt", "T1", "News", "C")]
    )

    class _Client:
        async def movers(self, top=10):
            return {"gainers": [{"symbol": "OLDH", "price": 5.0,
                                 "percent_change": 12.0, "volume": 100}],
                    "losers": []}

        async def most_actives(self, by="volume", top=10):
            return {"most_actives": []}

    monkeypatch.setattr(app, "market_client", _Client())
    mcp = build_mcp()
    out = await _call(mcp, "get_market_pulse", top=10)
    assert out["candidates"][0]["symbol"] == "OLDH"
    assert out["candidates"][0]["halted"] is True

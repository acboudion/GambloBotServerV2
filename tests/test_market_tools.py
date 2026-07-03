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

    assert (await _call(mcp, "get_stock_bars", symbol="A", timeframe="5min"))["error"] == "invalid_timeframe"

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

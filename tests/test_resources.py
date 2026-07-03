"""MCP resource endpoints — news resources (previously untested) + market resources."""

from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.app_state import AppState, clear_app_state, set_app_state
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.market_store import MarketStore
from alpaca_news_mcp.normalize import normalize_news_message
from alpaca_news_mcp.rest_backfill import RestBackfillWorker
from alpaca_news_mcp.server import build_mcp
from alpaca_news_mcp.state import State
from alpaca_news_mcp.stock_stream import StockStreamWorker
from alpaca_news_mcp.store import Store
from alpaca_news_mcp.stream import NewsStreamWorker


@pytest.fixture
async def app(tmp_path, monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "news.sqlite"))
    monkeypatch.setenv("MARKET_STORAGE_PATH", str(tmp_path / "market.sqlite"))
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "false")
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


async def _read(mcp, uri: str) -> dict | list:
    contents = await mcp._resource_manager.get_resource(uri)
    assert contents is not None, f"resource {uri} not found"
    text = await contents.read()
    return json.loads(text)


@pytest.mark.asyncio
async def test_news_resources(app):
    mcp = build_mcp()
    await app.store.upsert_article(
        normalize_news_message(
            {"T": "n", "id": 7, "headline": "resource test", "symbols": ["AAPL"]}
        ),
        source_kind="ws",
    )

    health = await _read(mcp, "alpaca-news://health")
    assert health["service"] == "alpaca-news-mcp"

    sub = await _read(mcp, "alpaca-news://subscription")
    assert "requested" in sub

    recent = await _read(mcp, "alpaca-news://recent")
    assert recent[0]["id"] == 7

    alerts = await _read(mcp, "alpaca-news://alerts")
    assert isinstance(alerts, list)

    symbols = await _read(mcp, "alpaca-news://symbols")
    assert symbols["symbol_counts"] == {"AAPL": 1}

    one = await _read(mcp, "alpaca-news://symbols/AAPL")
    assert one[0]["id"] == 7

    art = await _read(mcp, "alpaca-news://article/7")
    assert art["headline"] == "resource test"

    missing = await _read(mcp, "alpaca-news://article/999")
    assert missing["error"] == "not_found"

    bad = await _read(mcp, "alpaca-news://article/xyz")
    assert bad["error"] == "invalid_id"

    latency = await _read(mcp, "alpaca-news://stats/latency")
    assert "sample_count" in latency

    ingestion = await _read(mcp, "alpaca-news://stats/ingestion")
    assert ingestion["article_count"] == 1


@pytest.mark.asyncio
async def test_market_resources(app):
    mcp = build_mcp()
    app.market_store.snapshots.update("AAPL", "trade", {"p": 190.5, "ts_us": 1})
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    await app.market_store.persist_stream_batch(
        statuses=[("AAPL", now_us, "H", "Trading Halt", None, None, "C")]
    )

    watchlist = await _read(mcp, "alpaca-market://watchlist")
    assert "watchlist" in watchlist and "channels" in watchlist

    latest = await _read(mcp, "alpaca-market://latest/aapl")
    assert latest["symbol"] == "AAPL"
    assert latest["trade"]["p"] == 190.5

    missing = await _read(mcp, "alpaca-market://latest/ZZZZ")
    assert missing["error"] == "not_in_stream_cache"

    halts = await _read(mcp, "alpaca-market://halts")
    assert halts["statuses"][0]["status_code"] == "H"


@pytest.mark.asyncio
async def test_market_resources_disabled(app):
    mcp = build_mcp()
    app.stock_stream = None
    app.market_store = None
    out = await _read(mcp, "alpaca-market://watchlist")
    assert out["error"] == "stock_stream_disabled"

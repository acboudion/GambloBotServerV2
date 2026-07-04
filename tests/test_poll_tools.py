"""poll_market / get_started: composite polling, section isolation, context."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.app_state import AppState, clear_app_state, set_app_state
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.market_store import MarketStore
from alpaca_news_mcp.models import Alert
from alpaca_news_mcp.normalize import normalize_news_message
from alpaca_news_mcp.poll_tools import market_phase
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


async def _ingest(store, article_id: int, headline: str):
    return await store.upsert_article(
        normalize_news_message(
            {"T": "n", "id": article_id, "headline": headline, "symbols": ["AAPL"]}
        ),
        source_kind="ws",
    )


def _alert(alert_id: str, severity: str = "high") -> Alert:
    return Alert(
        alert_id=alert_id, article_id=None,
        created_at="2026-07-04T00:00:00+00:00", severity=severity,
        category="mna_keyword", symbols=["AAPL"], headline=None,
        reason="r", acknowledged=False,
    )


@pytest.mark.asyncio
async def test_poll_market_full_tick(app):
    """Bootstrap with -1 cursors, then one tick sees new news + alerts +
    bars and the snapshot/context blocks."""
    mcp = build_mcp()
    boot = await _call(mcp, "poll_market")
    assert boot["news"]["note"] == "started_from_tail"
    assert boot["alerts"]["note"] == "started_from_tail"
    assert boot["bars"]["note"] == "started_from_tail"
    assert "suggested_next_poll_seconds" in boot["context"]
    cursors = {
        "news": boot["news"]["next_cursor"],
        "alerts": boot["alerts"]["next_cursor"],
        "bars": boot["bars"]["next_cursor"],
    }

    await _ingest(app.store, 1, "Apple acquires Widget Co")
    await app.store.record_alert(_alert("a1"), raw_json="{}")
    ts = int(datetime.now(UTC).timestamp()) // 60 * 60
    await app.market_store.persist_stream_batch(
        bars=[("AAPL", "1min", ts, 1.0, 2.0, 0.5, 1.5, 1000, 10, 1.2)]
    )
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    app.market_store.snapshots.update("AAPL", "trade", {"p": 190.5, "ts_us": now_us})

    tick = await _call(
        mcp, "poll_market",
        news_cursor=cursors["news"], alerts_cursor=cursors["alerts"],
        bars_cursor=cursors["bars"],
    )
    assert [a["id"] for a in tick["news"]["articles"]] == [1]
    assert [a["alert_id"] for a in tick["alerts"]["alerts"]] == ["a1"]
    assert tick["bars"]["count"] == 1
    assert tick["bars"]["columns"][0] == "symbol"
    assert tick["snapshot"]["symbols"]["AAPL"]["trade"]["p"] == 190.5
    assert tick["snapshot"]["symbols"]["AAPL"]["age_seconds"] < 60
    deg = tick["context"]["degraded"]
    assert deg["news_stream"] is True  # not connected in tests
    assert "news_disconnected" in deg["reasons"]


@pytest.mark.asyncio
async def test_poll_market_sections_fail_independently(app):
    mcp = build_mcp()
    await _ingest(app.store, 1, "headline")
    out = await _call(mcp, "poll_market", news_cursor=0, bars_cursor=99_999)
    assert out["news"]["count"] == 1
    assert out["bars"]["error"] == "cursor_out_of_range"
    assert "alerts" in out and "context" in out


@pytest.mark.asyncio
async def test_poll_market_min_severity_filter(app):
    mcp = build_mcp()
    await app.store.record_alert(_alert("crit", "critical"), raw_json="{}")
    await app.store.record_alert(_alert("med", "medium"), raw_json="{}")
    out = await _call(
        mcp, "poll_market", alerts_cursor=0, min_severity="high",
        include=["alerts"],
    )
    assert [a["alert_id"] for a in out["alerts"]["alerts"]] == ["crit"]
    bad = await _call(mcp, "poll_market", min_severity="sev")
    assert bad["error"] == "invalid_severity"


@pytest.mark.asyncio
async def test_get_started_orientation(app):
    mcp = build_mcp()
    out = await _call(mcp, "get_started")
    assert out["service"] == "alpaca-news-mcp"
    assert out["current"]["watchlist"] == ["AAPL", "TSLA"]
    assert set(out["current"]["latest_cursors"]) == {"news", "alerts", "bars"}
    assert out["retention"]["news_days"] == app.config.event_retention_days
    assert any("poll_market" in step for step in out["loop_recipe"])


def test_market_phase_classification():
    open_clock = {"is_open": True}
    assert market_phase(open_clock, datetime.now(UTC)) == "open"
    assert market_phase(None, datetime.now(UTC)) == "unknown"
    closed_clock = {"is_open": False}
    # 2026-07-08 is a Wednesday; 13:00 UTC = 09:00 ET -> premarket.
    pre = datetime(2026, 7, 8, 13, 0, tzinfo=UTC)
    assert market_phase(closed_clock, pre) == "premarket"
    # 21:00 UTC = 17:00 ET -> afterhours.
    post = datetime(2026, 7, 8, 21, 0, tzinfo=UTC)
    assert market_phase(closed_clock, post) == "afterhours"
    # 07:00 UTC = 03:00 ET -> closed.
    night = datetime(2026, 7, 8, 7, 0, tzinfo=UTC)
    assert market_phase(closed_clock, night) == "closed"
    # Saturday afternoon -> closed regardless of hour.
    weekend = datetime(2026, 7, 11, 18, 0, tzinfo=UTC)
    assert market_phase(closed_clock, weekend) == "closed"


@pytest.mark.asyncio
async def test_trading_loop_prompt_registered(app):
    mcp = build_mcp()
    prompts = mcp._prompt_manager._prompts
    assert "trading-loop" in prompts


@pytest.mark.asyncio
async def test_snapshot_section_flags_truncation(tmp_path, monkeypatch):
    """Watchlists beyond the 50-symbol snapshot cap must surface truncation
    metadata instead of silently omitting symbols."""
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "n.sqlite"))
    monkeypatch.setenv("MARKET_STORAGE_PATH", str(tmp_path / "m.sqlite"))
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "false")
    monkeypatch.setenv("MAX_WATCHLIST_SYMBOLS", "120")
    monkeypatch.setenv(
        "STOCK_WATCHLIST_SYMBOLS", ",".join(f"S{i:03d}" for i in range(60))
    )
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
    try:
        mcp = build_mcp()
        out = await _call(mcp, "poll_market", include=["snapshot"])
        snap = out["snapshot"]
        assert snap["truncated"] is True
        assert snap["omitted"] == 10  # 60-symbol watchlist, 50-symbol cap
        assert "hint" in snap

        # Explicit small symbol set: no truncation keys.
        out = await _call(mcp, "poll_market", include=["snapshot"], symbols=["S001"])
        assert "truncated" not in out["snapshot"]
    finally:
        clear_app_state()
        await rest.close()
        await store.close()
        await market_store.close()
        NewsStreamWorker.reset_singleton()
        StockStreamWorker.reset_singleton()


def test_market_phase_holiday_is_closed_not_extended_hours():
    """A weekday holiday (clock closed, next_open on a future day) must not
    read as premarket/afterhours — there is no session to trade around."""
    # 2026-07-03 was a Friday US market holiday; 13:00 UTC = 09:00 ET.
    holiday_morning = datetime(2026, 7, 3, 13, 0, tzinfo=UTC)
    clock = {"is_open": False, "next_open": "2026-07-06T13:30:00Z"}
    assert market_phase(clock, holiday_morning) == "closed"
    # Same wall-clock time with the market opening later today -> premarket.
    normal = {"is_open": False, "next_open": "2026-07-08T13:30:00Z"}
    weekday_morning = datetime(2026, 7, 8, 13, 0, tzinfo=UTC)
    assert market_phase(normal, weekday_morning) == "premarket"
    # Calendar says no session today: even the afterhours window is closed.
    holiday_evening = datetime(2026, 7, 3, 21, 0, tzinfo=UTC)
    assert market_phase({"is_open": False}, holiday_evening, trading_day=False) == "closed"
    # Ordinary evening with a session today stays afterhours.
    assert market_phase({"is_open": False}, datetime(2026, 7, 8, 21, 0, tzinfo=UTC),
                        trading_day=True) == "afterhours"

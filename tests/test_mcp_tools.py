"""Tests for MCP tools — call the registered tool functions through the FastMCP API."""

from __future__ import annotations

import pytest

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.app_state import AppState, clear_app_state, set_app_state
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.normalize import normalize_news_message
from alpaca_news_mcp.rest_backfill import RestBackfillWorker
from alpaca_news_mcp.server import build_mcp
from alpaca_news_mcp.state import State
from alpaca_news_mcp.store import Store
from alpaca_news_mcp.stream import NewsStreamWorker


@pytest.fixture
async def app(tmp_path, monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "t.sqlite"))
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "false")
    cfg = Config.from_env()
    store = await Store.open(cfg.storage_path)
    await store.init_schema()
    state = State()
    state.set_interest_symbols(cfg.news_interest_symbols, "replace")
    alerts = AlertEngine()
    rest = RestBackfillWorker(cfg, store, state, alerts)
    NewsStreamWorker.reset_singleton()
    stream = NewsStreamWorker(cfg, store, state, alerts)
    app_state = AppState(
        config=cfg, store=store, state=state, alerts=alerts,
        stream=stream, rest_backfill=rest,
    )
    set_app_state(app_state)
    yield app_state
    clear_app_state()
    await rest.close()
    await store.close()
    NewsStreamWorker.reset_singleton()


async def _call(mcp, tool_name: str, **kwargs):
    """Call a registered MCP tool by name and return its serialized output."""
    tools_list = mcp._tool_manager.list_tools()
    names = [t.name for t in tools_list]
    if tool_name not in names:
        raise AssertionError(f"tool {tool_name!r} not registered; available: {names}")
    fn = mcp._tool_manager._tools[tool_name].fn
    return await fn(**kwargs)


@pytest.mark.asyncio
async def test_get_news_stream_health(app):
    mcp = build_mcp()
    out = await _call(mcp, "get_news_stream_health")
    assert out["service"] == "alpaca-news-mcp"
    assert "connected" in out
    assert "rest_backfill_enabled" in out


@pytest.mark.asyncio
async def test_get_recent_news_limit_exceeded(app):
    mcp = build_mcp()
    out = await _call(mcp, "get_recent_news", limit=10_000)
    assert out["error"] == "limit_exceeded"
    assert out["max_allowed"] == 200
    assert out["requested"] == 10_000


@pytest.mark.asyncio
async def test_get_recent_news_returns_articles(app):
    payload = {
        "T": "n",
        "id": 4242,
        "headline": "hi",
        "summary": "s",
        "content": "<p>body</p>",
        "created_at": "2026-04-28T19:00:00Z",
        "updated_at": "2026-04-28T19:00:00Z",
        "symbols": ["AAPL"],
        "source": "benzinga",
    }
    a = normalize_news_message(payload)
    await app.store.upsert_article(a, source_kind="ws")
    mcp = build_mcp()
    out = await _call(mcp, "get_recent_news", minutes=60, limit=10)
    assert out["count"] >= 1
    assert any(art["id"] == 4242 for art in out["articles"])


@pytest.mark.asyncio
async def test_set_interest_symbols_modes(app):
    mcp = build_mcp()
    out = await _call(mcp, "set_interest_symbols", symbols=["aapl", "tsla"], mode="replace")
    assert set(out["interest_symbols"]) == {"AAPL", "TSLA"}
    out = await _call(mcp, "set_interest_symbols", symbols=["msft"], mode="add")
    assert set(out["interest_symbols"]) == {"AAPL", "TSLA", "MSFT"}
    out = await _call(mcp, "set_interest_symbols", symbols=["AAPL"], mode="remove")
    assert "AAPL" not in out["interest_symbols"]
    out = await _call(mcp, "set_interest_symbols", symbols=[], mode="clear")
    assert out["interest_symbols"] == []


@pytest.mark.asyncio
async def test_set_interest_symbols_invalid_mode(app):
    mcp = build_mcp()
    out = await _call(mcp, "set_interest_symbols", symbols=["AAPL"], mode="bogus")
    assert out["error"] == "invalid_mode"


@pytest.mark.asyncio
async def test_ack_news_alert(app):
    from alpaca_news_mcp.models import Alert

    # Use article_id=None so the alert isn't blocked by the FK constraint
    alert = Alert(
        alert_id="alpha",
        article_id=None,
        created_at="2026-04-28T19:00:00Z",
        severity="high",
        category="mna_keyword",
        symbols=["AAPL"],
        headline="h",
        reason="r",
    )
    inserted = await app.store.record_alert(alert, raw_json="{}")
    assert inserted is True
    mcp = build_mcp()
    out = await _call(mcp, "ack_news_alert", alert_id="alpha")
    assert out["acknowledged"] is True


@pytest.mark.asyncio
async def test_get_news_article_not_found(app):
    mcp = build_mcp()
    out = await _call(mcp, "get_news_article", article_id=999_999_999)
    assert out["error"] == "not_found"


@pytest.mark.asyncio
async def test_get_raw_news_events_limit_exceeded(app):
    mcp = build_mcp()
    out = await _call(mcp, "get_raw_news_events", minutes=15, limit=999_999)
    assert out["error"] == "limit_exceeded"
    assert out["max_allowed"] == 500


@pytest.mark.asyncio
async def test_search_news_empty_query(app):
    mcp = build_mcp()
    out = await _call(mcp, "search_news", query="   ")
    assert out["error"] == "empty_query"


@pytest.mark.asyncio
async def test_run_news_rest_backfill_disabled(app, monkeypatch):
    # Flip the runtime config flag and ensure tool returns disabled
    from dataclasses import replace
    new_cfg = replace(app.config, enable_manual_rest_backfill=False)
    app.config = new_cfg
    mcp = build_mcp()
    out = await _call(mcp, "run_news_rest_backfill", minutes=10)
    assert out["error"] == "disabled"


def test_build_mcp_honors_custom_path():
    """A non-default MCP_PATH must propagate to FastMCP and register a route there."""
    mcp = build_mcp(mcp_path="/custom-mcp")
    assert mcp.settings.streamable_http_path == "/custom-mcp"
    app = mcp.streamable_http_app()
    paths = {getattr(r, "path", None) for r in app.routes}
    assert "/custom-mcp" in paths
    assert "/mcp" not in paths


async def _seed_articles_for_symbol(store, symbol: str, count: int) -> None:
    for i in range(count):
        payload = {
            "T": "n",
            "id": 5000 + i,
            "headline": f"h{i}",
            "summary": "s",
            "content": "<p>body</p>",
            "created_at": "2026-04-28T19:00:00Z",
            "updated_at": "2026-04-28T19:00:00Z",
            "symbols": [symbol],
            "source": "benzinga",
        }
        a = normalize_news_message(payload)
        await store.upsert_article(a, source_kind="ws")


async def _seed_versions(store, article_id: int, count: int) -> None:
    """Upsert the same article_id `count` times with distinct headlines/updated_at
    so the store records `count` versions."""
    for i in range(count):
        payload = {
            "T": "n",
            "id": article_id,
            "headline": f"v{i}",
            "summary": "s",
            "content": f"<p>body{i}</p>",
            "created_at": "2026-04-28T19:00:00Z",
            "updated_at": f"2026-04-28T19:{i // 60:02d}:{i % 60:02d}Z",
            "symbols": ["AAPL"],
            "source": "benzinga",
        }
        a = normalize_news_message(payload)
        await store.upsert_article(a, source_kind="ws")


@pytest.mark.asyncio
async def test_get_news_versions_limit_exceeded(app):
    mcp = build_mcp()
    out = await _call(mcp, "get_news_versions", article_id=8001, limit=10_000)
    assert out["error"] == "limit_exceeded"
    assert out["max_allowed"] == 200
    assert out["requested"] == 10_000


@pytest.mark.asyncio
async def test_get_news_versions_default_limit_caps_results(app):
    await _seed_versions(app.store, article_id=8002, count=60)
    mcp = build_mcp()
    out = await _call(mcp, "get_news_versions", article_id=8002)
    assert out["article_id"] == 8002
    assert out["count"] == 50
    assert out["versions_total"] == 60
    assert len(out["versions"]) == 50


@pytest.mark.asyncio
async def test_get_news_versions_explicit_limit(app):
    await _seed_versions(app.store, article_id=8003, count=60)
    mcp = build_mcp()
    out = await _call(mcp, "get_news_versions", article_id=8003, limit=10)
    assert out["count"] == 10
    assert out["versions_total"] == 60
    assert len(out["versions"]) == 10


@pytest.mark.asyncio
async def test_get_news_article_include_versions_default_caps(app):
    await _seed_versions(app.store, article_id=8004, count=60)
    mcp = build_mcp()
    out = await _call(
        mcp, "get_news_article", article_id=8004, include_versions=True
    )
    assert "error" not in out
    assert out["versions_returned"] == 50
    assert out["versions_total"] == 60
    assert len(out["versions"]) == 50


@pytest.mark.asyncio
async def test_get_news_article_version_limit_exceeded(app):
    await _seed_versions(app.store, article_id=8005, count=2)
    mcp = build_mcp()
    out = await _call(
        mcp,
        "get_news_article",
        article_id=8005,
        include_versions=True,
        version_limit=999,
    )
    assert out["error"] == "limit_exceeded"
    assert out["max_allowed"] == 200
    assert out["requested"] == 999


@pytest.mark.asyncio
async def test_get_news_article_version_limit_ignored_when_not_included(app):
    await _seed_versions(app.store, article_id=8006, count=2)
    mcp = build_mcp()
    out = await _call(
        mcp,
        "get_news_article",
        article_id=8006,
        include_versions=False,
        version_limit=999,
    )
    assert "error" not in out
    assert "versions" not in out
    assert "versions_total" not in out


@pytest.mark.parametrize("bad_limit", [0, -1, -1000])
@pytest.mark.asyncio
async def test_get_news_for_symbols_clamps_non_positive_limit(app, bad_limit):
    """Non-positive limit_per_symbol must not bypass the bound (SQLite LIMIT <= 0 means "no limit")."""
    await _seed_articles_for_symbol(app.store, "AAPL", count=5)
    mcp = build_mcp()
    out = await _call(mcp, "get_news_for_symbols", symbols=["AAPL"], limit_per_symbol=bad_limit)
    assert "error" not in out
    assert len(out["symbols"]["AAPL"]) == 1


@pytest.mark.parametrize("bad_max", [0, -1, -1000])
@pytest.mark.asyncio
async def test_get_breaking_news_digest_clamps_non_positive_max(app, bad_max):
    """Non-positive max_articles must not bypass the bound or produce a negative-slice fallback."""
    await _seed_articles_for_symbol(app.store, "AAPL", count=5)
    mcp = build_mcp()
    out = await _call(mcp, "get_breaking_news_digest", minutes=60, max_articles=bad_max)
    assert "error" not in out
    assert len(out["articles"]) <= 1

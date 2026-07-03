"""Delta-cursor tools: get_news_since / get_alerts_since + field projection."""

from __future__ import annotations

import pytest

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.app_state import AppState, clear_app_state, set_app_state
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.models import Alert
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
    fn = mcp._tool_manager._tools[tool_name].fn
    return await fn(**kwargs)


async def _ingest(store, article_id: int, headline: str, symbols=None, **extra):
    payload = {"T": "n", "id": article_id, "headline": headline}
    if symbols:
        payload["symbols"] = symbols
    payload.update(extra)
    return await store.upsert_article(
        normalize_news_message(payload), source_kind="websocket"
    )


@pytest.mark.asyncio
async def test_news_cursor_monotonic_and_empty_poll(app):
    mcp = build_mcp()
    for i in range(1, 4):
        await _ingest(app.store, i, f"headline {i}")

    out = await _call(mcp, "get_news_since", cursor=0)
    assert out["count"] == 3
    assert [a["id"] for a in out["articles"]] == [1, 2, 3]
    assert out["has_more"] is False
    cursor = out["next_cursor"]
    assert cursor == out["articles"][-1]["seq"]

    # Nothing new: cursor unchanged, empty page.
    out2 = await _call(mcp, "get_news_since", cursor=cursor)
    assert out2["count"] == 0
    assert out2["next_cursor"] == cursor

    # New article appears after the cursor.
    await _ingest(app.store, 4, "headline 4")
    out3 = await _call(mcp, "get_news_since", cursor=cursor)
    assert [a["id"] for a in out3["articles"]] == [4]


@pytest.mark.asyncio
async def test_news_cursor_update_resurfaces_article(app):
    mcp = build_mcp()
    await _ingest(app.store, 1, "original")
    out = await _call(mcp, "get_news_since", cursor=0)
    cursor = out["next_cursor"]

    # Unchanged redelivery does NOT resurface.
    await _ingest(app.store, 1, "original")
    out2 = await _call(mcp, "get_news_since", cursor=cursor)
    assert out2["count"] == 0

    # Changed headline DOES resurface with a fresh seq.
    await _ingest(app.store, 1, "updated headline")
    out3 = await _call(mcp, "get_news_since", cursor=cursor)
    assert out3["count"] == 1
    assert out3["articles"][0]["headline"] == "updated headline"
    assert out3["next_cursor"] > cursor


@pytest.mark.asyncio
async def test_news_cursor_pagination_has_more(app):
    mcp = build_mcp()
    for i in range(1, 8):
        await _ingest(app.store, i, f"h{i}")
    out = await _call(mcp, "get_news_since", cursor=0, limit=3)
    assert out["count"] == 3
    assert out["has_more"] is True
    out2 = await _call(mcp, "get_news_since", cursor=out["next_cursor"], limit=3)
    assert [a["id"] for a in out2["articles"]] == [4, 5, 6]
    out3 = await _call(mcp, "get_news_since", cursor=out2["next_cursor"], limit=3)
    assert [a["id"] for a in out3["articles"]] == [7]
    assert out3["has_more"] is False


@pytest.mark.asyncio
async def test_news_cursor_symbol_filter(app):
    mcp = build_mcp()
    await _ingest(app.store, 1, "apple news", symbols=["AAPL"])
    await _ingest(app.store, 2, "tesla news", symbols=["TSLA"])
    await _ingest(app.store, 3, "both", symbols=["AAPL", "TSLA"])
    out = await _call(mcp, "get_news_since", cursor=0, symbols=["aapl"])
    assert [a["id"] for a in out["articles"]] == [1, 3]


@pytest.mark.asyncio
async def test_news_cursor_limit_and_fields_validation(app):
    mcp = build_mcp()
    out = await _call(mcp, "get_news_since", cursor=0, limit=10_000)
    assert out["error"] == "limit_exceeded"
    assert out["max_allowed"] == 500
    out2 = await _call(mcp, "get_news_since", cursor=0, fields="bogus")
    assert out2["error"] == "invalid_fields"


@pytest.mark.asyncio
async def test_field_projection_shapes(app):
    mcp = build_mcp()
    await _ingest(
        app.store, 1, "h", symbols=["AAPL"], content="<p>body text here</p>",
        summary="sum",
    )
    compact = await _call(mcp, "get_recent_news", fields="compact")
    a = compact["articles"][0]
    assert set(a.keys()) == {"id", "headline", "symbols", "source", "updated_at", "url"}

    standard = await _call(mcp, "get_recent_news", fields="standard")
    s = standard["articles"][0]
    assert "first_seen_at" in s
    assert "content_text" not in s

    full = await _call(mcp, "get_recent_news", fields="full")
    f = full["articles"][0]
    assert f["content_text"] == "body text here"

    bogus = await _call(mcp, "get_recent_news", fields="nope")
    assert bogus["error"] == "invalid_fields"


@pytest.mark.asyncio
async def test_alerts_cursor(app):
    mcp = build_mcp()
    for i in range(1, 4):
        # alerts.article_id has a FK to news_articles — ingest the article first
        await _ingest(app.store, i, f"h{i}")
        alert = Alert(
            alert_id=f"a{i}",
            article_id=i,
            created_at="2024-01-01T00:00:00+00:00",
            severity="high",
            category="mna_keyword",
            symbols=["AAPL"],
            reason=f"r{i}",
        )
        assert await app.store.record_alert(alert, raw_json="{}")

    out = await _call(mcp, "get_alerts_since", cursor=0, limit=2)
    assert out["count"] == 2
    assert out["has_more"] is True
    assert [a["alert_id"] for a in out["alerts"]] == ["a1", "a2"]
    cursor = out["next_cursor"]

    out2 = await _call(mcp, "get_alerts_since", cursor=cursor)
    assert [a["alert_id"] for a in out2["alerts"]] == ["a3"]
    assert out2["has_more"] is False
    # Empty poll keeps the cursor stable.
    out3 = await _call(mcp, "get_alerts_since", cursor=out2["next_cursor"])
    assert out3["count"] == 0
    assert out3["next_cursor"] == out2["next_cursor"]


@pytest.mark.asyncio
async def test_alerts_cursor_severity_filter(app):
    mcp = build_mcp()
    for i, sev in enumerate(["high", "critical", "high"], start=1):
        await _ingest(app.store, i, f"h{i}")
        alert = Alert(
            alert_id=f"a{i}",
            article_id=i,
            created_at="2024-01-01T00:00:00+00:00",
            severity=sev,
            category="mna_keyword",
            symbols=[],
            reason="r",
        )
        await app.store.record_alert(alert, raw_json="{}")
    out = await _call(mcp, "get_alerts_since", cursor=0, severity="critical")
    assert [a["alert_id"] for a in out["alerts"]] == ["a2"]

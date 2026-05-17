"""Tests for the /healthz handler. Calls the handler directly with the
AppState wired up via fixture — no need for a Starlette TestClient because
the handler does not read from the Request object."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from starlette.requests import Request

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.app_state import AppState, clear_app_state, set_app_state
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.rest_backfill import RestBackfillWorker
from alpaca_news_mcp.server import healthz
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
    state = State(max_recent_articles=cfg.max_recent_articles_memory)
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


def _body(resp) -> dict:
    return json.loads(resp.body.decode("utf-8"))


@pytest.mark.asyncio
async def test_healthz_seconds_since_last_message_populated(app):
    """When `last_message_at` is set, /healthz exposes the elapsed seconds
    so a Docker healthcheck or dashboard can detect a stalled stream
    without doing timezone math on an ISO string."""
    last = (datetime.now(UTC) - timedelta(seconds=90)).isoformat()
    app.state.update_health(last_message_at=last)

    resp = await healthz(cast(Request, None))
    body = _body(resp)
    assert body["ok"] is True
    assert body["last_message_at"] == last
    assert 89 <= body["seconds_since_last_message"] <= 95
    # last_article_at was never set — delta must be None, not 0.
    assert body["last_article_at"] is None
    assert body["seconds_since_last_article"] is None


@pytest.mark.asyncio
async def test_healthz_ok_decoupled_from_stream_connected(app):
    """`ok` must remain True even while the WS is disconnected — Docker
    healthcheck should mean `process up + HTTP responsive`. Otherwise a
    transient WS reconnect would flap the container as unhealthy."""
    app.state.update_health(connected=False, authenticated=False)
    resp = await healthz(cast(Request, None))
    body = _body(resp)
    assert body["ok"] is True
    assert body["stream_connected"] is False


@pytest.mark.asyncio
async def test_healthz_handles_unparseable_timestamp(app):
    """A corrupt timestamp must not crash the handler."""
    app.state.update_health(last_message_at="not-a-date")
    resp = await healthz(cast(Request, None))
    body = _body(resp)
    assert body["seconds_since_last_message"] is None


@pytest.mark.asyncio
async def test_healthz_503_when_app_state_unset():
    """If we somehow get queried before app_setup ran, return 503."""
    clear_app_state()
    resp = await healthz(cast(Request, None))
    assert resp.status_code == 503
    body = _body(resp)
    assert body["ok"] is False

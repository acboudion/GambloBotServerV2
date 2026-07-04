"""Runtime keyword groups + watched stories (server-side news watches)."""

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
from alpaca_news_mcp.tools import ALERT_KEYWORD_OVERRIDES_KEY, WATCHED_STORIES_KEY


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


def _payload(id: int, headline: str, **extra):
    p = {"T": "n", "id": id, "headline": headline, "symbols": ["XBIO"],
         "created_at": "2026-07-04T13:00:00Z", "updated_at": "2026-07-04T13:00:00Z"}
    p.update(extra)
    return p


@pytest.mark.asyncio
async def test_runtime_keyword_group_lifecycle(app):
    """The bot adds a biotech radar intraday: new group matches immediately,
    persists, and deletes cleanly."""
    mcp = build_mcp()
    out = await _call(
        mcp, "set_alert_keyword_group",
        group="biotech_catalysts",
        phrases=["pdufa", "phase 3 topline"],
        critical_phrases=["crl issued"],
        bullish=["phase 3 topline"],
    )
    assert out["group"] == "biotech_catalysts"
    assert out["category"] == "custom_keyword"
    assert "crl issued" in out["phrases"]  # critical phrases are matchable

    # Matching article emits a custom_keyword alert at >= medium severity.
    alerts = app.alerts.evaluate_article(
        (await app.store.upsert_article(
            normalize_news_message(_payload(1, "XBIO PDUFA date set for August")),
            source_kind="ws",
        )).article,
        interest_symbols=set(),
    )
    custom = [a for a in alerts if a.category == "custom_keyword"]
    assert len(custom) == 1 and custom[0].severity in ("medium", "high")

    # Critical phrase escalates.
    alerts = app.alerts.evaluate_article(
        (await app.store.upsert_article(
            normalize_news_message(_payload(2, "CRL issued for XBIO lead drug")),
            source_kind="ws",
        )).article,
        interest_symbols=set(),
    )
    assert any(
        a.category == "custom_keyword" and a.severity == "critical" for a in alerts
    )

    # Persisted for restart restore.
    persisted = await app.store.get_status(ALERT_KEYWORD_OVERRIDES_KEY)
    assert "biotech_catalysts" in persisted

    listed = await _call(mcp, "get_alert_keyword_groups")
    assert listed["groups"]["biotech_catalysts"]["runtime_override"] is True
    assert listed["groups"]["mna"]["builtin"] is True

    out = await _call(mcp, "delete_alert_keyword_group", group="biotech_catalysts")
    assert out["deleted"] == "biotech_catalysts"
    after = app.alerts.evaluate_article(
        (await app.store.upsert_article(
            normalize_news_message(_payload(3, "Another PDUFA mention")),
            source_kind="ws",
        )).article,
        interest_symbols=set(),
    )
    assert not any(a.category == "custom_keyword" for a in after)


@pytest.mark.asyncio
async def test_runtime_group_extends_builtin_and_reverts(app):
    mcp = build_mcp()
    await _call(
        mcp, "set_alert_keyword_group",
        group="mna", phrases=["scheme of arrangement"],
    )
    alerts = app.alerts.evaluate_article(
        (await app.store.upsert_article(
            normalize_news_message(_payload(10, "Target agrees to scheme of arrangement")),
            source_kind="ws",
        )).article,
        interest_symbols=set(),
    )
    assert any(a.category == "mna_keyword" for a in alerts)
    # Deleting the override reverts to built-in phrases (group still exists).
    out = await _call(mcp, "delete_alert_keyword_group", group="mna")
    assert out["deleted"] == "mna"
    listed = await _call(mcp, "get_alert_keyword_groups")
    assert listed["groups"]["mna"]["builtin"] is True
    assert "scheme of arrangement" not in listed["groups"]["mna"]["phrases"]


@pytest.mark.asyncio
async def test_keyword_group_validation(app):
    mcp = build_mcp()
    out = await _call(mcp, "set_alert_keyword_group", group="", phrases=["x"])
    assert out["error"] == "invalid_group"
    out = await _call(mcp, "set_alert_keyword_group", group="g", phrases=[])
    assert out["error"] == "no_phrases"
    out = await _call(mcp, "delete_alert_keyword_group", group="nope")
    assert out["error"] == "not_found"


@pytest.mark.asyncio
async def test_watch_story_emits_update_alerts(app):
    """Watching a story turns its future content updates into high-severity
    alerts in the feed the bot already polls."""
    mcp = build_mcp()
    worker = app.stream
    # Ingest the original story via the stream persister path.
    await worker._persist_batch([_payload(100, "Halt: XBIO pending news")])
    out = await _call(mcp, "watch_story", article_id=100)
    assert out["watching"] == 100

    # An unchanged redelivery must NOT alert...
    await worker._persist_batch([_payload(100, "Halt: XBIO pending news")])
    rows, _, _ = await app.store.alerts_since(cursor=0, limit=50)
    assert not any(a["category"] == "watched_story_update" for a in rows)

    # ...but a real content update must.
    await worker._persist_batch([
        _payload(100, "XBIO resumes: FDA approves lead drug",
                 updated_at="2026-07-04T13:30:00Z"),
    ])
    rows, _, _ = await app.store.alerts_since(cursor=0, limit=50)
    updates = [a for a in rows if a["category"] == "watched_story_update"]
    assert len(updates) == 1
    assert updates[0]["severity"] == "high"
    assert "article_id=100" in updates[0]["reason"]
    assert updates[0]["symbols"] == ["XBIO"]

    # A second distinct update alerts AGAIN (not deduped away).
    await worker._persist_batch([
        _payload(100, "XBIO up 40% after approval",
                 updated_at="2026-07-04T14:00:00Z"),
    ])
    rows, _, _ = await app.store.alerts_since(cursor=0, limit=50)
    updates = [a for a in rows if a["category"] == "watched_story_update"]
    assert len(updates) == 2

    # Unwatch stops the flow.
    await _call(mcp, "unwatch_story", article_id=100)
    await worker._persist_batch([
        _payload(100, "XBIO closes up 35%", updated_at="2026-07-04T15:00:00Z"),
    ])
    rows, _, _ = await app.store.alerts_since(cursor=0, limit=50)
    assert len([a for a in rows if a["category"] == "watched_story_update"]) == 2


@pytest.mark.asyncio
async def test_watch_story_tools_and_persistence(app):
    mcp = build_mcp()
    out = await _call(mcp, "watch_story", article_id=999)
    assert out["error"] == "not_found"

    await app.store.upsert_article(
        normalize_news_message(_payload(200, "story A")), source_kind="ws"
    )
    await _call(mcp, "watch_story", article_id=200)
    listed = await _call(mcp, "get_watched_stories")
    assert listed["count"] == 1
    assert listed["stories"][0]["headline"] == "story A"

    persisted = await app.store.get_status(WATCHED_STORIES_KEY)
    assert persisted["articles"].keys() == {"200"}

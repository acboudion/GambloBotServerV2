"""Migration-runner tests: old-schema upgrade, fresh-DB no-op, idempotency."""

from __future__ import annotations

import sqlite3

import aiosqlite
import pytest

from alpaca_news_mcp.migrations import (
    NEWS_MIGRATIONS,
    get_user_version,
    migrate,
    table_has_column,
)
from alpaca_news_mcp.store import Store

OLD_SCHEMA = """
CREATE TABLE news_articles (
    id INTEGER PRIMARY KEY,
    headline TEXT NOT NULL,
    summary TEXT,
    author TEXT,
    created_at TEXT,
    updated_at TEXT,
    content_html TEXT,
    content_text TEXT,
    url TEXT,
    source TEXT,
    symbols_json TEXT NOT NULL DEFAULT '[]',
    raw_json TEXT NOT NULL,
    first_seen_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    last_seen_source TEXT NOT NULL,
    update_count INTEGER NOT NULL DEFAULT 0,
    latency_ms INTEGER,
    is_content_present INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE raw_events (
    event_id TEXT PRIMARY KEY,
    received_at TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    message_type TEXT,
    raw_json TEXT NOT NULL
);
CREATE TABLE alerts (
    alert_id TEXT PRIMARY KEY,
    article_id INTEGER,
    created_at TEXT NOT NULL,
    severity TEXT NOT NULL,
    category TEXT NOT NULL,
    symbols_json TEXT NOT NULL DEFAULT '[]',
    headline TEXT,
    reason TEXT NOT NULL,
    acknowledged INTEGER NOT NULL DEFAULT 0,
    raw_json TEXT NOT NULL,
    FOREIGN KEY(article_id) REFERENCES news_articles(id)
);
"""


def _make_old_db(path: str, article_ids_and_first_seen: list[tuple[int, str]]) -> None:
    conn = sqlite3.connect(path)
    conn.executescript(OLD_SCHEMA)
    for aid, first_seen in article_ids_and_first_seen:
        conn.execute(
            "INSERT INTO news_articles "
            "(id, headline, raw_json, first_seen_at, last_seen_at, last_seen_source) "
            "VALUES (?, ?, '{}', ?, ?, 'websocket')",
            (aid, f"headline {aid}", first_seen, first_seen),
        )
    conn.commit()
    conn.close()


@pytest.mark.asyncio
async def test_migrate_old_db_backfills_seq_in_first_seen_order(tmp_path):
    path = str(tmp_path / "old.sqlite")
    # Insert out of first-seen order to prove backfill sorts by first_seen_at.
    _make_old_db(
        path,
        [
            (30, "2024-01-03T00:00:00+00:00"),
            (10, "2024-01-01T00:00:00+00:00"),
            (20, "2024-01-02T00:00:00+00:00"),
        ],
    )
    async with aiosqlite.connect(path) as conn:
        conn.row_factory = aiosqlite.Row
        assert not await table_has_column(conn, "news_articles", "seq")
        version = await migrate(conn, NEWS_MIGRATIONS)
        assert version == NEWS_MIGRATIONS[-1][0]
        cur = await conn.execute("SELECT id, seq FROM news_articles ORDER BY seq")
        rows = await cur.fetchall()
        assert [(r["id"], r["seq"]) for r in rows] == [(10, 1), (20, 2), (30, 3)]


@pytest.mark.asyncio
async def test_migrate_is_idempotent(tmp_path):
    path = str(tmp_path / "old.sqlite")
    _make_old_db(path, [(1, "2024-01-01T00:00:00+00:00")])
    async with aiosqlite.connect(path) as conn:
        conn.row_factory = aiosqlite.Row
        await migrate(conn, NEWS_MIGRATIONS)
        first = await get_user_version(conn)
        await migrate(conn, NEWS_MIGRATIONS)
        assert await get_user_version(conn) == first
        cur = await conn.execute("SELECT seq FROM news_articles WHERE id = 1")
        row = await cur.fetchone()
        assert row["seq"] == 1


@pytest.mark.asyncio
async def test_store_open_on_old_db_continues_seq_after_backfill(tmp_path):
    path = str(tmp_path / "old.sqlite")
    _make_old_db(
        path,
        [
            (1, "2024-01-01T00:00:00+00:00"),
            (2, "2024-01-02T00:00:00+00:00"),
        ],
    )
    store = await Store.open(path)
    try:
        await store.init_schema()
        from alpaca_news_mcp.normalize import normalize_news_message

        norm = normalize_news_message({"T": "n", "id": 3, "headline": "new one"})
        result = await store.upsert_article(norm, source_kind="websocket")
        assert result.was_new
        article = await store.get_article(3)
        assert article is not None
        assert article.seq == 3
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_fresh_db_gets_user_version_and_seq_index(open_store):
    version = await get_user_version(open_store.conn)
    assert version >= 1
    cur = await open_store.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_articles_seq'"
    )
    row = await cur.fetchone()
    assert row is not None


def _insert_old_alert(path: str, alert_id: str, created_at: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO alerts (alert_id, article_id, created_at, severity, category, "
        "symbols_json, headline, reason, acknowledged, raw_json) "
        "VALUES (?, NULL, ?, 'high', 'mna_keyword', '[]', 'h', 'r', 0, '{}')",
        (alert_id, created_at),
    )
    conn.commit()
    conn.close()


@pytest.mark.asyncio
async def test_m5_backfills_alert_seq_in_rowid_order(tmp_path):
    path = str(tmp_path / "old.sqlite")
    _make_old_db(path, [])
    _insert_old_alert(path, "a-1", "2024-01-01T00:00:00+00:00")
    _insert_old_alert(path, "a-2", "2024-01-02T00:00:00+00:00")
    _insert_old_alert(path, "a-3", "2024-01-03T00:00:00+00:00")
    async with aiosqlite.connect(path) as conn:
        conn.row_factory = aiosqlite.Row
        assert not await table_has_column(conn, "alerts", "seq")
        await migrate(conn, NEWS_MIGRATIONS)
        cur = await conn.execute("SELECT alert_id, seq FROM alerts ORDER BY seq")
        rows = await cur.fetchall()
        assert [(r["alert_id"], r["seq"]) for r in rows] == [
            ("a-1", 1), ("a-2", 2), ("a-3", 3),
        ]
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_alerts_seq'"
        )
        assert await cur.fetchone() is not None

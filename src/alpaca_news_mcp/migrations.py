"""Idempotent schema migrations keyed on ``PRAGMA user_version``.

``schema.sql`` always describes the *final* shape for fresh databases (all
statements are ``IF NOT EXISTS``). Migrations here upgrade databases created
by older schema versions in place. Every migration function must be safe to
run against a database that already has the target shape (fresh DBs run the
whole chain once and each step no-ops).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable

import aiosqlite

from .logging_utils import get_logger

log = get_logger(__name__)

Migration = Callable[[aiosqlite.Connection], Awaitable[None]]


async def get_user_version(conn: aiosqlite.Connection) -> int:
    cur = await conn.execute("PRAGMA user_version")
    row = await cur.fetchone()
    await cur.close()
    return int(row[0]) if row else 0


async def table_has_column(
    conn: aiosqlite.Connection, table: str, column: str
) -> bool:
    cur = await conn.execute(f"PRAGMA table_info({table})")
    rows = await cur.fetchall()
    await cur.close()
    return any(r[1] == column for r in rows)


async def ensure_column(
    conn: aiosqlite.Connection, table: str, column: str, ddl: str
) -> bool:
    """ALTER TABLE ... ADD COLUMN unless the column already exists."""
    if await table_has_column(conn, table, column):
        return False
    await conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
    return True


async def _m1_news_seq(conn: aiosqlite.Connection) -> None:
    """Monotonic ingest sequence for delta-cursor polling."""
    await ensure_column(conn, "news_articles", "seq", "INTEGER")
    # Backfill in stable first-seen order so pre-existing rows are pollable.
    await conn.execute(
        """
        UPDATE news_articles SET seq = (
            SELECT rn FROM (
                SELECT id AS aid, ROW_NUMBER() OVER (ORDER BY first_seen_at, id) AS rn
                FROM news_articles
            ) WHERE aid = news_articles.id
        )
        WHERE seq IS NULL
        """
    )
    await conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_articles_seq ON news_articles(seq)"
    )


FTS_CREATE_SQL = """
CREATE VIRTUAL TABLE IF NOT EXISTS news_fts USING fts5(
    headline, summary, content_text,
    content='news_articles', content_rowid='id',
    tokenize='porter unicode61'
)
"""


async def _m2_news_fts(conn: aiosqlite.Connection) -> None:
    """Full-text search over headline/summary/content_text.

    External-content FTS5: rows are synced explicitly by Store.upsert_article
    and prune_retention (triggers would fight the COALESCE update semantics).
    The one-time rebuild indexes rows that existed before this migration.
    """
    await conn.execute(FTS_CREATE_SQL)
    await conn.execute("INSERT INTO news_fts(news_fts) VALUES('rebuild')")


async def _m3_raw_events_replayed(conn: aiosqlite.Connection) -> None:
    """Track which queue-overflow-dropped articles have been re-ingested."""
    await ensure_column(conn, "raw_events", "replayed", "INTEGER NOT NULL DEFAULT 0")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_raw_events_replay "
        "ON raw_events(message_type, replayed, received_at)"
    )


async def _m4_alert_upgrades(conn: aiosqlite.Connection) -> None:
    """Cross-source dedup (content_hash) + direction tagging for alerts."""
    await ensure_column(conn, "alerts", "content_hash", "TEXT")
    await ensure_column(conn, "alerts", "direction", "TEXT NOT NULL DEFAULT 'neutral'")
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_alerts_content_hash "
        "ON alerts(category, content_hash, created_at)"
    )


async def _m5_alert_seq(conn: aiosqlite.Connection) -> None:
    """Monotonic alert cursor for delta polling.

    alerts_since previously paged on the implicit rowid, which SQLite reuses
    once retention prunes the max-rowid row — new alerts could then land at
    or below a poller's saved cursor and never be delivered."""
    await ensure_column(conn, "alerts", "seq", "INTEGER")
    await conn.execute(
        """
        UPDATE alerts SET seq = (
            SELECT rn FROM (
                SELECT rowid AS rid, ROW_NUMBER() OVER (ORDER BY rowid) AS rn
                FROM alerts
            ) WHERE rid = alerts.rowid
        )
        WHERE seq IS NULL
        """
    )
    await conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_alerts_seq ON alerts(seq)"
    )


NEWS_MIGRATIONS: list[tuple[int, Migration]] = [
    (1, _m1_news_seq),
    (2, _m2_news_fts),
    (3, _m3_raw_events_replayed),
    (4, _m4_alert_upgrades),
    (5, _m5_alert_seq),
]


async def _mm1_market_status(conn: aiosqlite.Connection) -> None:
    """Key/value status table (bar-seq allocation high-water mark)."""
    await conn.execute(
        """
        CREATE TABLE IF NOT EXISTS market_status (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )


MARKET_MIGRATIONS: list[tuple[int, Migration]] = [
    (1, _mm1_market_status),
]


async def migrate(
    conn: aiosqlite.Connection,
    migrations: list[tuple[int, Migration]],
) -> int:
    """Run pending migrations in order. Returns the resulting user_version."""
    version = await get_user_version(conn)
    for target, fn in migrations:
        if version < target:
            log.info("running schema migration %d (%s)", target, fn.__name__)
            await fn(conn)
            await conn.execute(f"PRAGMA user_version = {target}")
            await conn.commit()
            version = target
    return version

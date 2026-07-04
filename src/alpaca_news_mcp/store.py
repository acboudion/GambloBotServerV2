"""SQLite (aiosqlite) persistence with WAL and a single-writer lock."""

from __future__ import annotations

import asyncio
import json
import statistics
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Any

import aiosqlite
from pydantic import ValidationError

from .logging_utils import get_logger
from .migrations import NEWS_MIGRATIONS, migrate
from .models import Alert, IngestionStats, LatencyStats, NewsArticle, NewsArticleVersion
from .normalize import NormalizedArticle

log = get_logger(__name__)


@dataclass
class UpsertResult:
    article_id: int
    was_new: bool
    version_inserted: bool
    article: NewsArticle


class BatchWriter:
    """Write facade yielded by Store.batch_writer().

    Methods mirror the Store API but assume the write lock is already held and
    never commit — the surrounding batch_writer() context commits once on exit.
    """

    def __init__(self, store: Store) -> None:
        self._store = store

    async def upsert_article(
        self, normalized: NormalizedArticle, *, source_kind: str
    ) -> UpsertResult:
        return await self._store._upsert_article_locked(normalized, source_kind=source_kind)

    async def record_alert(
        self, alert: Alert, *, raw_json: str, content_hash: str | None = None
    ) -> bool:
        return await self._store._record_alert_locked(
            alert, raw_json=raw_json, content_hash=content_hash
        )


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


# stream_status row persisting the seq allocation high-water marks. Seeding
# the allocators from MAX(seq) alone is not enough: retention can prune the
# rows carrying MAX(seq), and a restart would then re-issue already-consumed
# seq values — new rows landing at or below a poller's saved cursor would
# never be delivered.
SEQ_WATERMARKS_KEY = "seq_watermarks"


async def open_read_connection(path: str) -> aiosqlite.Connection | None:
    """Open a read-only companion connection (WAL snapshot isolation).

    Tool reads must never share the writer's connection: on a shared
    connection a SELECT issued while a batch transaction is open sees
    uncommitted rows — phantom articles/alerts that vanish on rollback —
    and queues behind long prunes. A separate mode=ro connection reads
    the last committed WAL snapshot instead.

    Returns None (callers fall back to the writer connection) for
    :memory: databases — a second connection would see a different,
    empty database — or if the read-only open fails for any reason.
    """
    if not path or path == ":memory:":
        return None
    try:
        conn = await aiosqlite.connect(f"file:{path}?mode=ro", uri=True)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA busy_timeout=5000")
        return conn
    except Exception as e:
        log.warning(
            "read-only connection unavailable for %s (%s); reads share the writer",
            path,
            e,
        )
        return None


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: aiosqlite.Connection | None = None
        self._read_conn: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()
        self._next_seq: int = 1
        self._next_alert_seq: int = 1
        # Set by the allocators; cleared when the watermarks row is written.
        self._seq_dirty: bool = False

    @classmethod
    async def open(cls, path: str) -> Store:
        store = cls(path)
        # Ensure parent dir exists for file paths.
        if path and path not in (":memory:",):
            p = Path(path)
            if p.parent and str(p.parent) and not p.parent.exists():
                p.parent.mkdir(parents=True, exist_ok=True)
        store._conn = await aiosqlite.connect(path)
        store._conn.row_factory = aiosqlite.Row
        return store

    async def close(self) -> None:
        if self._read_conn is not None:
            await self._read_conn.close()
            self._read_conn = None
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Store is not open")
        return self._conn

    @property
    def rconn(self) -> aiosqlite.Connection:
        """Read connection for tool queries; the writer when unavailable."""
        return self._read_conn if self._read_conn is not None else self.conn

    async def init_schema(self) -> None:
        sql_text = resources.files("alpaca_news_mcp").joinpath("schema.sql").read_text(
            encoding="utf-8"
        )
        async with self._write_lock:
            await self.conn.executescript(sql_text)
            await self.conn.commit()
            await migrate(self.conn, NEWS_MIGRATIONS)
            cur = await self.conn.execute("SELECT COALESCE(MAX(seq), 0) FROM news_articles")
            row = await cur.fetchone()
            await cur.close()
            max_article_seq = int(row[0]) if row else 0
            cur = await self.conn.execute("SELECT COALESCE(MAX(seq), 0) FROM alerts")
            row = await cur.fetchone()
            await cur.close()
            max_alert_seq = int(row[0]) if row else 0
            persisted = await self._read_status(self.conn, SEQ_WATERMARKS_KEY) or {}
            self._next_seq = max(max_article_seq, int(persisted.get("news_seq", 0))) + 1
            self._next_alert_seq = (
                max(max_alert_seq, int(persisted.get("alert_seq", 0))) + 1
            )
        # After the schema exists and WAL mode is committed — mode=ro on a
        # nonexistent file would fail.
        self._read_conn = await open_read_connection(self.path)

    def _allocate_seq(self) -> int:
        """Next monotonic ingest sequence. Callers must hold the write lock."""
        seq = self._next_seq
        self._next_seq += 1
        self._seq_dirty = True
        return seq

    def _allocate_alert_seq(self) -> int:
        """Next monotonic alert cursor. Callers must hold the write lock."""
        seq = self._next_alert_seq
        self._next_alert_seq += 1
        self._seq_dirty = True
        return seq

    async def _flush_watermarks_locked(self) -> None:
        """Persist the allocation high-water marks inside the open transaction.

        Called at every commit point that may have allocated seqs, so any
        committed row's seq is always covered by the committed watermark row.
        A rollback discards both together, keeping them consistent — the
        in-memory allocators stay advanced, which only leaves harmless gaps.
        """
        if not self._seq_dirty:
            return
        await self.conn.execute(
            """INSERT INTO stream_status (key, value_json, updated_at)
               VALUES (?, ?, ?)
               ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
                                             updated_at = excluded.updated_at""",
            (
                SEQ_WATERMARKS_KEY,
                json.dumps(
                    {
                        "news_seq": self._next_seq - 1,
                        "alert_seq": self._next_alert_seq - 1,
                    }
                ),
                _utcnow_iso(),
            ),
        )
        self._seq_dirty = False

    async def upsert_article(
        self,
        normalized: NormalizedArticle,
        *,
        source_kind: str,
    ) -> UpsertResult:
        """Insert or update an article with its own transaction."""
        async with self._write_lock:
            result = await self._upsert_article_locked(normalized, source_kind=source_kind)
            await self._flush_watermarks_locked()
            await self.conn.commit()
        return result

    @asynccontextmanager
    async def batch_writer(self) -> AsyncIterator[BatchWriter]:
        """Hold the write lock across several writes and commit once.

        This is the hot-path primitive: the stream persister drains a queue
        batch, writes every article + alert through the yielded BatchWriter,
        and pays a single fsync instead of one per row.
        """
        async with self._write_lock:
            # The commit itself must be inside the rollback guard: a commit
            # failure (SQLITE_BUSY, I/O error) would otherwise leave the
            # transaction open, and a later unrelated write would commit this
            # "failed" batch's rows after the persister already discarded its
            # pending alert quota/state.
            try:
                yield BatchWriter(self)
                await self._flush_watermarks_locked()
                await self.conn.commit()
            except BaseException:
                await self.conn.rollback()
                raise

    async def _upsert_article_locked(
        self,
        normalized: NormalizedArticle,
        *,
        source_kind: str,
    ) -> UpsertResult:
        """Insert or update an article. Caller must hold the write lock; does not commit.

        The returned NewsArticle is assembled in Python (mirroring the SQL
        COALESCE semantics) so the hot path avoids a redundant re-SELECT.
        """
        cur = await self.conn.execute(
            "SELECT * FROM news_articles WHERE id = ?",
            (normalized.id,),
        )
        existing = await cur.fetchone()
        await cur.close()

        now = _utcnow_iso()
        symbols_json = json.dumps(normalized.symbols)
        was_new = existing is None
        version_inserted = False

        try:
            raw = json.loads(normalized.raw_json) if normalized.raw_json else None
        except json.JSONDecodeError:
            raw = None

        if existing is None:
            new_seq = self._allocate_seq()
            await self.conn.execute(
                """
                INSERT INTO news_articles (
                    id, headline, summary, author, created_at, updated_at,
                    content_html, content_text, url, source,
                    symbols_json, raw_json,
                    first_seen_at, last_seen_at, last_seen_source,
                    update_count, latency_ms, is_content_present, seq
                ) VALUES (
                    ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?,
                    ?, ?, ?,
                    0, ?, ?, ?
                )
                """,
                (
                    normalized.id,
                    normalized.headline,
                    normalized.summary,
                    normalized.author,
                    normalized.created_at,
                    normalized.updated_at,
                    normalized.content_html,
                    normalized.content_text,
                    normalized.url,
                    normalized.source,
                    symbols_json,
                    normalized.raw_json,
                    now,
                    now,
                    source_kind,
                    normalized.latency_ms,
                    1 if normalized.is_content_present else 0,
                    new_seq,
                ),
            )
            version_inserted = await self._insert_version(
                normalized, source_kind=source_kind
            )
            await self._upsert_symbol_index(normalized, source_kind=source_kind)
            await self._fts_insert(
                normalized.id,
                normalized.headline,
                normalized.summary,
                normalized.content_text,
            )
            article = NewsArticle(
                id=normalized.id,
                headline=normalized.headline,
                summary=normalized.summary,
                author=normalized.author,
                created_at=normalized.created_at,
                updated_at=normalized.updated_at,
                content_html=normalized.content_html,
                content_text=normalized.content_text,
                url=normalized.url,
                source=normalized.source,
                symbols=list(normalized.symbols),
                first_seen_at=now,
                last_seen_at=now,
                last_seen_source=source_kind,
                update_count=0,
                latency_ms=normalized.latency_ms,
                is_content_present=normalized.is_content_present,
                seq=new_seq,
                raw=raw,
            )
        else:
            # Compare against post-COALESCE values: when the incoming payload
            # omits an optional field (None), the UPDATE keeps the existing
            # value, so it should not count as a content change.
            content_changed = (existing["headline"] or "") != (normalized.headline or "")
            if normalized.summary is not None and (existing["summary"] or "") != normalized.summary:
                content_changed = True
            if (
                normalized.content_html is not None
                and (existing["content_html"] or "") != normalized.content_html
            ):
                content_changed = True
            if (
                normalized.updated_at is not None
                and (existing["updated_at"] or "") != normalized.updated_at
            ):
                content_changed = True
            existing_symbols = set(json.loads(existing["symbols_json"] or "[]"))
            new_symbols = set(normalized.symbols)
            symbols_changed = existing_symbols != new_symbols
            changed = content_changed or symbols_changed
            # update last_seen + counters
            new_update_count = (existing["update_count"] or 0) + (1 if changed else 0)
            # A changed article gets a fresh seq so delta-cursor pollers
            # (get_news_since) see the update; unchanged re-deliveries keep
            # their position and stay invisible to cursors.
            new_seq = self._allocate_seq() if changed else existing["seq"]
            await self.conn.execute(
                """
                UPDATE news_articles SET
                    headline = ?,
                    summary = COALESCE(?, summary),
                    author = COALESCE(?, author),
                    created_at = COALESCE(?, created_at),
                    updated_at = COALESCE(?, updated_at),
                    content_html = COALESCE(?, content_html),
                    content_text = COALESCE(?, content_text),
                    url = COALESCE(?, url),
                    source = COALESCE(?, source),
                    symbols_json = ?,
                    raw_json = ?,
                    last_seen_at = ?,
                    last_seen_source = ?,
                    update_count = ?,
                    latency_ms = COALESCE(latency_ms, ?),
                    is_content_present = MAX(is_content_present, ?),
                    seq = ?
                WHERE id = ?
                """,
                (
                    normalized.headline,
                    normalized.summary,
                    normalized.author,
                    normalized.created_at,
                    normalized.updated_at,
                    normalized.content_html,
                    normalized.content_text,
                    normalized.url,
                    normalized.source,
                    symbols_json,
                    normalized.raw_json,
                    now,
                    source_kind,
                    new_update_count,
                    normalized.latency_ms,
                    1 if normalized.is_content_present else 0,
                    new_seq,
                    normalized.id,
                ),
            )
            if content_changed:
                version_inserted = await self._insert_version(
                    normalized, source_kind=source_kind
                )
            if symbols_changed:
                removed_symbols = existing_symbols - new_symbols
                if removed_symbols:
                    placeholders = ",".join("?" * len(removed_symbols))
                    await self.conn.execute(
                        f"DELETE FROM news_symbol_index "
                        f"WHERE article_id = ? AND symbol IN ({placeholders})",
                        (normalized.id, *sorted(removed_symbols)),
                    )
            if changed:
                await self._upsert_symbol_index(normalized, source_kind=source_kind)
            # Keep the FTS index in step with the stored (post-COALESCE) text.
            merged_summary = (
                normalized.summary if normalized.summary is not None else existing["summary"]
            )
            merged_content_text = (
                normalized.content_text
                if normalized.content_text is not None
                else existing["content_text"]
            )
            fts_changed = (
                normalized.headline != existing["headline"]
                or merged_summary != existing["summary"]
                or merged_content_text != existing["content_text"]
            )
            if fts_changed:
                await self._fts_delete(
                    normalized.id,
                    existing["headline"],
                    existing["summary"],
                    existing["content_text"],
                )
                await self._fts_insert(
                    normalized.id,
                    normalized.headline,
                    merged_summary,
                    merged_content_text,
                )
            # Mirror the UPDATE's COALESCE semantics in Python so the caller
            # gets the post-write row without a re-SELECT.
            article = NewsArticle(
                id=normalized.id,
                headline=normalized.headline,
                summary=normalized.summary if normalized.summary is not None else existing["summary"],
                author=normalized.author if normalized.author is not None else existing["author"],
                created_at=normalized.created_at if normalized.created_at is not None else existing["created_at"],
                updated_at=normalized.updated_at if normalized.updated_at is not None else existing["updated_at"],
                content_html=normalized.content_html if normalized.content_html is not None else existing["content_html"],
                content_text=normalized.content_text if normalized.content_text is not None else existing["content_text"],
                url=normalized.url if normalized.url is not None else existing["url"],
                source=normalized.source if normalized.source is not None else existing["source"],
                symbols=list(normalized.symbols),
                first_seen_at=existing["first_seen_at"],
                last_seen_at=now,
                last_seen_source=source_kind,
                update_count=new_update_count,
                latency_ms=existing["latency_ms"] if existing["latency_ms"] is not None else normalized.latency_ms,
                is_content_present=bool(existing["is_content_present"]) or normalized.is_content_present,
                seq=new_seq,
                raw=raw,
            )

        return UpsertResult(
            article_id=normalized.id,
            was_new=was_new,
            version_inserted=version_inserted,
            article=article,
        )

    async def _fts_insert(
        self, article_id: int, headline: str | None, summary: str | None,
        content_text: str | None,
    ) -> None:
        await self.conn.execute(
            "INSERT INTO news_fts(rowid, headline, summary, content_text) "
            "VALUES (?, ?, ?, ?)",
            (article_id, headline, summary, content_text),
        )

    async def _fts_delete(
        self, article_id: int, headline: str | None, summary: str | None,
        content_text: str | None,
    ) -> None:
        # External-content FTS5 'delete' requires the OLD column values.
        await self.conn.execute(
            "INSERT INTO news_fts(news_fts, rowid, headline, summary, content_text) "
            "VALUES ('delete', ?, ?, ?, ?)",
            (article_id, headline, summary, content_text),
        )

    async def _insert_version(
        self, normalized: NormalizedArticle, *, source_kind: str
    ) -> bool:
        version_id = str(uuid.uuid4())
        await self.conn.execute(
            """
            INSERT INTO news_article_versions (
                version_id, article_id, updated_at, received_at, source,
                raw_json, headline, summary, content_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version_id,
                normalized.id,
                normalized.updated_at,
                normalized.received_at,
                source_kind,
                normalized.raw_json,
                normalized.headline,
                normalized.summary,
                normalized.content_hash,
            ),
        )
        return True

    async def _upsert_symbol_index(
        self, normalized: NormalizedArticle, *, source_kind: str
    ) -> None:
        for sym in normalized.symbols:
            await self.conn.execute(
                """
                INSERT INTO news_symbol_index (
                    symbol, article_id, created_at, updated_at, received_at, source
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, article_id) DO UPDATE SET
                    updated_at = COALESCE(excluded.updated_at, updated_at),
                    received_at = excluded.received_at,
                    source = excluded.source
                """,
                (
                    sym,
                    normalized.id,
                    normalized.created_at,
                    normalized.updated_at,
                    normalized.received_at,
                    source_kind,
                ),
            )

    async def record_raw_event(
        self, *, endpoint: str, message_type: str | None, raw_json: str
    ) -> None:
        async with self._write_lock:
            await self.conn.execute(
                "INSERT INTO raw_events (event_id, received_at, endpoint, message_type, raw_json) "
                "VALUES (?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), _utcnow_iso(), endpoint, message_type, raw_json),
            )
            await self.conn.commit()

    async def unreplayed_dropped_events(self, *, limit: int = 100) -> list[dict[str, Any]]:
        """Queue-overflow-dropped articles not yet replayed, oldest first.
        Each entry: {event_id, payload} where payload is the parsed article
        dict (None if the stored JSON is unparseable)."""
        cur = await self.rconn.execute(
            "SELECT event_id, raw_json FROM raw_events "
            "WHERE message_type = 'queue_overflow_drop' AND replayed = 0 "
            "ORDER BY received_at ASC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        await cur.close()
        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                payload = json.loads(r["raw_json"])
            except (json.JSONDecodeError, TypeError):
                payload = None
            out.append({"event_id": r["event_id"], "payload": payload})
        return out

    async def mark_events_replayed(self, event_ids: list[str]) -> None:
        if not event_ids:
            return
        placeholders = ",".join("?" * len(event_ids))
        async with self._write_lock:
            await self.conn.execute(
                f"UPDATE raw_events SET replayed = 1 WHERE event_id IN ({placeholders})",
                event_ids,
            )
            await self.conn.commit()

    async def record_alert(
        self, alert: Alert, *, raw_json: str, content_hash: str | None = None
    ) -> bool:
        """Insert an alert. Returns True if inserted, False if deduplicated."""
        async with self._write_lock:
            inserted = await self._record_alert_locked(
                alert, raw_json=raw_json, content_hash=content_hash
            )
            await self._flush_watermarks_locked()
            await self.conn.commit()
            return inserted

    async def _record_alert_locked(
        self, alert: Alert, *, raw_json: str, content_hash: str | None = None
    ) -> bool:
        # Cross-source dedup: the same story republished under a different
        # article id (news is single-sourced today, but re-syndication and
        # updates produce identical bodies) must not alert twice per category.
        if content_hash:
            window_start = (datetime.now(UTC) - timedelta(hours=24)).isoformat()
            cur = await self.conn.execute(
                "SELECT 1 FROM alerts WHERE category = ? AND content_hash = ? "
                "AND created_at >= ? AND (article_id IS NULL OR article_id != ?) "
                "LIMIT 1",
                (alert.category, content_hash, window_start, alert.article_id),
            )
            duplicate = await cur.fetchone()
            await cur.close()
            if duplicate is not None:
                return False
        try:
            # Allocated after the dedup checks; an IntegrityError below leaves
            # a gap in the sequence, which cursors tolerate (monotonic, not
            # dense).
            seq = self._allocate_alert_seq()
            await self.conn.execute(
                """
                INSERT INTO alerts (
                    alert_id, article_id, created_at, severity, category,
                    symbols_json, headline, reason, acknowledged, raw_json,
                    content_hash, direction, seq
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)
                """,
                (
                    alert.alert_id,
                    alert.article_id,
                    alert.created_at,
                    alert.severity,
                    alert.category,
                    json.dumps(alert.symbols),
                    alert.headline,
                    alert.reason,
                    raw_json,
                    content_hash,
                    alert.direction,
                    seq,
                ),
            )
            return True
        except aiosqlite.IntegrityError:
            return False

    async def ack_alert(self, alert_id: str) -> bool:
        async with self._write_lock:
            cur = await self.conn.execute(
                "UPDATE alerts SET acknowledged = 1 WHERE alert_id = ?",
                (alert_id,),
            )
            await self.conn.commit()
            return (cur.rowcount or 0) > 0

    async def set_status(self, key: str, value: dict[str, Any]) -> None:
        async with self._write_lock:
            await self.conn.execute(
                """INSERT INTO stream_status (key, value_json, updated_at)
                   VALUES (?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json,
                                                 updated_at = excluded.updated_at""",
                (key, json.dumps(value), _utcnow_iso()),
            )
            await self.conn.commit()

    async def get_status(self, key: str) -> dict[str, Any] | None:
        return await self._read_status(self.rconn, key)

    @staticmethod
    async def _read_status(
        conn: aiosqlite.Connection, key: str
    ) -> dict[str, Any] | None:
        cur = await conn.execute(
            "SELECT value_json FROM stream_status WHERE key = ?", (key,)
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        try:
            return json.loads(row["value_json"])
        except json.JSONDecodeError:
            return None

    async def get_article(self, article_id: int) -> NewsArticle | None:
        cur = await self.rconn.execute(
            "SELECT * FROM news_articles WHERE id = ?", (article_id,)
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return self._row_to_article(row)

    async def get_recent_articles(
        self,
        *,
        minutes: int,
        symbols: list[str] | None = None,
        sources: list[str] | None = None,
        limit: int = 50,
    ) -> list[NewsArticle]:
        since = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
        clauses = ["(updated_at >= ? OR created_at >= ? OR first_seen_at >= ?)"]
        params: list[Any] = [since, since, since]

        if symbols:
            placeholders = ",".join("?" * len(symbols))
            clauses.append(
                f"id IN (SELECT article_id FROM news_symbol_index WHERE symbol IN ({placeholders}))"
            )
            params.extend([s.upper() for s in symbols])

        if sources:
            placeholders = ",".join("?" * len(sources))
            clauses.append(f"source IN ({placeholders})")
            params.extend(sources)

        sql = (
            "SELECT * FROM news_articles WHERE "
            + " AND ".join(clauses)
            + " ORDER BY COALESCE(updated_at, created_at, first_seen_at) DESC LIMIT ?"
        )
        params.append(limit)
        cur = await self.rconn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [self._row_to_article(r) for r in rows]

    async def search_articles(
        self,
        *,
        query: str,
        symbols: list[str] | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int = 50,
    ) -> list[NewsArticle]:
        """Ranked FTS5 search, falling back to substring LIKE when FTS finds
        nothing (token-based FTS can't match mid-word fragments) or errors."""
        try:
            articles = await self._search_articles_fts(
                query=query, symbols=symbols, since=since, until=until, limit=limit
            )
        except aiosqlite.OperationalError as e:
            log.warning("FTS search failed (%s); falling back to LIKE", e)
            articles = []
        if articles:
            return articles
        return await self._search_articles_like(
            query=query, symbols=symbols, since=since, until=until, limit=limit
        )

    @staticmethod
    def _fts_match_expression(query: str) -> str:
        # Quote every token (doubling embedded quotes) so LLM-supplied text can
        # never be parsed as FTS5 syntax; tokens are implicitly ANDed.
        tokens = [t for t in query.split() if t]
        return " ".join('"' + t.replace('"', '""') + '"' for t in tokens)

    async def _search_articles_fts(
        self,
        *,
        query: str,
        symbols: list[str] | None,
        since: str | None,
        until: str | None,
        limit: int,
    ) -> list[NewsArticle]:
        match = self._fts_match_expression(query)
        if not match:
            return []
        clauses = ["news_fts MATCH ?"]
        params: list[Any] = [match]
        if symbols:
            placeholders = ",".join("?" * len(symbols))
            clauses.append(
                f"a.id IN (SELECT article_id FROM news_symbol_index WHERE symbol IN ({placeholders}))"
            )
            params.extend([s.upper() for s in symbols])
        if since:
            clauses.append("COALESCE(a.updated_at, a.created_at, a.first_seen_at) >= ?")
            params.append(since)
        if until:
            clauses.append("COALESCE(a.updated_at, a.created_at, a.first_seen_at) <= ?")
            params.append(until)
        sql = (
            "SELECT a.* FROM news_fts f JOIN news_articles a ON a.id = f.rowid WHERE "
            + " AND ".join(clauses)
            + " ORDER BY f.rank LIMIT ?"
        )
        params.append(limit)
        cur = await self.rconn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [self._row_to_article(r) for r in rows]

    async def _search_articles_like(
        self,
        *,
        query: str,
        symbols: list[str] | None,
        since: str | None,
        until: str | None,
        limit: int,
    ) -> list[NewsArticle]:
        # Escape LIKE metacharacters so a query containing % or _ (price
        # moves, tickers) matches literally instead of as wildcards.
        escaped = (
            query.lower()
            .replace("\\", "\\\\")
            .replace("%", "\\%")
            .replace("_", "\\_")
        )
        like = f"%{escaped}%"
        clauses = [
            "(LOWER(headline) LIKE ? ESCAPE '\\' "
            "OR LOWER(summary) LIKE ? ESCAPE '\\' "
            "OR LOWER(content_text) LIKE ? ESCAPE '\\')"
        ]
        params: list[Any] = [like, like, like]

        if symbols:
            placeholders = ",".join("?" * len(symbols))
            clauses.append(
                f"id IN (SELECT article_id FROM news_symbol_index WHERE symbol IN ({placeholders}))"
            )
            params.extend([s.upper() for s in symbols])
        # Compare both bounds against a single effective timestamp so an article
        # whose created_at is before `since` but updated_at is after `until`
        # (or vice versa) is correctly excluded from the window.
        if since:
            clauses.append("COALESCE(updated_at, created_at, first_seen_at) >= ?")
            params.append(since)
        if until:
            clauses.append("COALESCE(updated_at, created_at, first_seen_at) <= ?")
            params.append(until)

        sql = (
            "SELECT * FROM news_articles WHERE "
            + " AND ".join(clauses)
            + " ORDER BY COALESCE(updated_at, created_at, first_seen_at) DESC LIMIT ?"
        )
        params.append(limit)
        cur = await self.rconn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [self._row_to_article(r) for r in rows]

    @property
    def latest_article_cursor(self) -> int:
        """Highest allocated article seq — 'pass this to receive only new
        rows'. Uses the allocator (not table MAX) so it stays meaningful
        after retention empties the table; future rows always allocate
        above it."""
        return self._next_seq - 1

    @property
    def latest_alert_cursor(self) -> int:
        return self._next_alert_seq - 1

    async def min_article_seq(self) -> int:
        cur = await self.rconn.execute(
            "SELECT COALESCE(MIN(seq), 0) FROM news_articles"
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    async def min_alert_seq(self) -> int:
        cur = await self.rconn.execute("SELECT COALESCE(MIN(seq), 0) FROM alerts")
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    async def articles_since(
        self,
        *,
        cursor: int,
        limit: int,
        symbols: list[str] | None = None,
    ) -> tuple[list[NewsArticle], int, bool]:
        """Articles with seq > cursor in ingest order.

        Returns (articles, next_cursor, has_more). next_cursor equals the input
        cursor when nothing new arrived, so pollers can pass it back verbatim.
        """
        clauses = ["seq > ?"]
        params: list[Any] = [cursor]
        if symbols:
            placeholders = ",".join("?" * len(symbols))
            clauses.append(
                f"id IN (SELECT article_id FROM news_symbol_index WHERE symbol IN ({placeholders}))"
            )
            params.extend([s.upper() for s in symbols])
        sql = (
            "SELECT * FROM news_articles WHERE "
            + " AND ".join(clauses)
            + " ORDER BY seq ASC LIMIT ?"
        )
        params.append(limit + 1)
        cur = await self.rconn.execute(sql, params)
        rows = list(await cur.fetchall())
        await cur.close()
        has_more = len(rows) > limit
        rows = rows[:limit]
        articles = [self._row_to_article(r) for r in rows]
        # With a symbol filter, articles outside the filter still advance seq;
        # skipping ahead to MAX(seq) would lose them if the filter changes, so
        # the cursor only ever advances over rows actually returned.
        next_cursor = articles[-1].seq if articles and articles[-1].seq else cursor
        return articles, next_cursor, has_more

    async def alerts_since(
        self,
        *,
        cursor: int,
        limit: int,
        severity: str | None = None,
        severities: list[str] | None = None,
        categories: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], int, bool]:
        """Alerts with seq > cursor in insert order (alerts are insert-only).

        Pages on the explicit monotonic seq (migration m5) rather than the
        implicit rowid: SQLite reuses rowids once retention prunes the
        max-rowid row, which would make later alerts land at or below a
        poller's saved cursor and never be delivered.

        Returns (alert_dicts_with_cursor, next_cursor, has_more).
        """
        clauses = ["seq > ?"]
        params: list[Any] = [cursor]
        if severity:
            clauses.append("severity = ?")
            params.append(severity)
        if severities:
            placeholders = ",".join("?" * len(severities))
            clauses.append(f"severity IN ({placeholders})")
            params.extend(severities)
        if categories:
            placeholders = ",".join("?" * len(categories))
            clauses.append(f"category IN ({placeholders})")
            params.extend(categories)
        sql = (
            "SELECT seq AS cursor_id, * FROM alerts WHERE "
            + " AND ".join(clauses)
            + " ORDER BY seq ASC LIMIT ?"
        )
        params.append(limit + 1)
        cur = await self.rconn.execute(sql, params)
        rows = list(await cur.fetchall())
        await cur.close()
        has_more = len(rows) > limit
        rows = rows[:limit]
        out: list[dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "cursor": int(r["cursor_id"]),
                    "alert_id": r["alert_id"],
                    "article_id": r["article_id"],
                    "created_at": r["created_at"],
                    "severity": r["severity"],
                    "category": r["category"],
                    "symbols": json.loads(r["symbols_json"] or "[]"),
                    "headline": r["headline"],
                    "reason": r["reason"],
                    "acknowledged": bool(r["acknowledged"]),
                    "direction": (r["direction"] if "direction" in r.keys() else None)
                    or "neutral",
                }
            )
        # Filtered-out rows still advance rowid; same conservative-cursor rule
        # as articles_since.
        next_cursor = out[-1]["cursor"] if out else cursor
        return out, next_cursor, has_more

    async def articles_for_symbols(
        self,
        symbols: list[str],
        *,
        minutes: int,
        limit_per_symbol: int,
    ) -> dict[str, list[NewsArticle]]:
        result: dict[str, list[NewsArticle]] = {}
        if not symbols:
            return result
        since = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
        for sym in symbols:
            sym_u = sym.upper()
            cur = await self.rconn.execute(
                """
                SELECT a.* FROM news_articles a
                JOIN news_symbol_index s ON s.article_id = a.id
                WHERE s.symbol = ? AND s.received_at >= ?
                ORDER BY COALESCE(a.updated_at, a.created_at, a.first_seen_at) DESC
                LIMIT ?
                """,
                (sym_u, since, limit_per_symbol),
            )
            rows = await cur.fetchall()
            await cur.close()
            result[sym_u] = [self._row_to_article(r) for r in rows]
        return result

    async def get_versions(
        self, article_id: int, limit: int | None = None
    ) -> list[NewsArticleVersion]:
        if limit is None:
            cur = await self.rconn.execute(
                "SELECT * FROM news_article_versions WHERE article_id = ? "
                "ORDER BY received_at ASC",
                (article_id,),
            )
        else:
            # Pull the most recent `limit` rows at the SQL level so we don't
            # read or deserialize the full version history, then re-sort ASC
            # to keep the existing oldest-first response ordering.
            cur = await self.rconn.execute(
                "SELECT * FROM ("
                "  SELECT * FROM news_article_versions WHERE article_id = ? "
                "  ORDER BY received_at DESC LIMIT ?"
                ") ORDER BY received_at ASC",
                (article_id, limit),
            )
        rows = await cur.fetchall()
        await cur.close()
        return [
            NewsArticleVersion(
                version_id=r["version_id"],
                article_id=r["article_id"],
                updated_at=r["updated_at"],
                received_at=r["received_at"],
                source=r["source"],
                headline=r["headline"],
                summary=r["summary"],
                content_hash=r["content_hash"],
            )
            for r in rows
        ]

    async def count_versions(self, article_id: int) -> int:
        cur = await self.rconn.execute(
            "SELECT COUNT(*) FROM news_article_versions WHERE article_id = ?",
            (article_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    async def get_alerts(
        self,
        *,
        minutes: int,
        severity: str | None = None,
        categories: list[str] | None = None,
        symbols: list[str] | None = None,
        limit: int = 100,
    ) -> list[Alert]:
        since = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
        clauses = ["created_at >= ?"]
        params: list[Any] = [since]

        if severity:
            clauses.append("severity = ?")
            params.append(severity)
        if categories:
            placeholders = ",".join("?" * len(categories))
            clauses.append(f"category IN ({placeholders})")
            params.extend(categories)
        if symbols:
            sym_filters = " OR ".join(["symbols_json LIKE ?"] * len(symbols))
            clauses.append(f"({sym_filters})")
            params.extend([f'%"{s.upper()}"%' for s in symbols])

        sql = (
            "SELECT * FROM alerts WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at DESC LIMIT ?"
        )
        params.append(limit)
        cur = await self.rconn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        out: list[Alert] = []
        for r in rows:
            try:
                out.append(
                    Alert(
                        alert_id=r["alert_id"],
                        article_id=r["article_id"],
                        created_at=r["created_at"],
                        severity=r["severity"],
                        category=r["category"],
                        symbols=json.loads(r["symbols_json"] or "[]"),
                        headline=r["headline"],
                        reason=r["reason"],
                        acknowledged=bool(r["acknowledged"]),
                        direction=(r["direction"] if "direction" in r.keys() else None)
                        or "neutral",
                    )
                )
            except ValidationError:
                # Rows written by a newer build can carry categories this
                # build's Literal doesn't know — skip them instead of failing
                # the whole read after a downgrade.
                log.warning(
                    "skipping alert %s with unknown category %r",
                    r["alert_id"],
                    r["category"],
                )
        return out

    async def latency_stats(self, minutes: int) -> LatencyStats:
        since = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
        cur = await self.rconn.execute(
            "SELECT latency_ms FROM news_articles "
            "WHERE first_seen_at >= ? AND latency_ms IS NOT NULL AND latency_ms >= 0",
            (since,),
        )
        rows = await cur.fetchall()
        await cur.close()
        samples = [r["latency_ms"] for r in rows if r["latency_ms"] is not None]
        if not samples:
            return LatencyStats(
                window_minutes=minutes,
                sample_count=0,
                p50_ms=None,
                p90_ms=None,
                p99_ms=None,
                max_ms=None,
                avg_ms=None,
            )
        samples.sort()

        def pct(p: float) -> int:
            idx = max(0, min(len(samples) - 1, round((p / 100.0) * (len(samples) - 1))))
            return int(samples[idx])

        return LatencyStats(
            window_minutes=minutes,
            sample_count=len(samples),
            p50_ms=pct(50),
            p90_ms=pct(90),
            p99_ms=pct(99),
            max_ms=int(samples[-1]),
            avg_ms=int(statistics.fmean(samples)),
        )

    async def ingestion_stats(self, minutes: int) -> IngestionStats:
        since = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()

        async def _count(sql: str) -> int:
            cur = await self.rconn.execute(sql, (since,))
            row = await cur.fetchone()
            await cur.close()
            return int(row["c"]) if row is not None else 0

        ac = await _count(
            "SELECT COUNT(*) as c FROM news_articles WHERE first_seen_at >= ?"
        )
        vc = await _count(
            "SELECT COUNT(*) as c FROM news_article_versions WHERE received_at >= ?"
        )
        sc = await _count(
            "SELECT COUNT(DISTINCT symbol) as c FROM news_symbol_index WHERE received_at >= ?"
        )
        src = await _count(
            "SELECT COUNT(DISTINCT source) as c FROM news_articles "
            "WHERE first_seen_at >= ? AND source IS NOT NULL"
        )
        rc = await _count(
            "SELECT COUNT(*) as c FROM raw_events WHERE received_at >= ?"
        )

        return IngestionStats(
            window_minutes=minutes,
            article_count=ac,
            version_count=vc,
            distinct_symbols=sc,
            distinct_sources=src,
            raw_event_count=rc,
        )

    async def recent_raw_events(
        self, *, minutes: int, limit: int = 100
    ) -> list[dict[str, Any]]:
        since = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
        cur = await self.rconn.execute(
            "SELECT event_id, received_at, endpoint, message_type, raw_json "
            "FROM raw_events WHERE received_at >= ? ORDER BY received_at DESC LIMIT ?",
            (since, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        out: list[dict[str, Any]] = []
        for r in rows:
            try:
                raw = json.loads(r["raw_json"])
            except (json.JSONDecodeError, TypeError):
                raw = r["raw_json"]
            out.append(
                {
                    "event_id": r["event_id"],
                    "received_at": r["received_at"],
                    "endpoint": r["endpoint"],
                    "message_type": r["message_type"],
                    "raw": raw,
                }
            )
        return out

    async def symbol_map(
        self, *, minutes: int, min_articles: int, limit: int = 500
    ) -> dict[str, int]:
        since = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
        cur = await self.rconn.execute(
            """
            SELECT symbol, COUNT(DISTINCT article_id) as c
            FROM news_symbol_index
            WHERE received_at >= ?
            GROUP BY symbol
            HAVING c >= ?
            ORDER BY c DESC, symbol ASC
            LIMIT ?
            """,
            (since, min_articles, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        return {r["symbol"]: int(r["c"]) for r in rows}

    async def prune_retention(
        self, *, event_days: int, raw_event_days: int
    ) -> dict[str, int]:
        now = datetime.now(UTC)
        cutoff_articles = (now - timedelta(days=event_days)).isoformat()
        cutoff_raw = (now - timedelta(days=raw_event_days)).isoformat()
        async with self._write_lock:
            # Everything below must land (or vanish) together: the FTS purge
            # runs first because external-content FTS5 needs the old column
            # values, so a mid-sequence failure without rollback would leave
            # FTS rows deleted for articles that still exist.
            try:
                await self.conn.execute(
                    "INSERT INTO news_fts(news_fts, rowid, headline, summary, content_text) "
                    "SELECT 'delete', id, headline, summary, content_text "
                    "FROM news_articles WHERE first_seen_at < ?",
                    (cutoff_articles,),
                )
                cur = await self.conn.execute(
                    "DELETE FROM news_article_versions "
                    "WHERE article_id IN (SELECT id FROM news_articles WHERE first_seen_at < ?)",
                    (cutoff_articles,),
                )
                v = cur.rowcount or 0
                cur = await self.conn.execute(
                    "DELETE FROM news_symbol_index "
                    "WHERE article_id IN (SELECT id FROM news_articles WHERE first_seen_at < ?)",
                    (cutoff_articles,),
                )
                s = cur.rowcount or 0
                # Alerts referencing expiring articles must go BEFORE the
                # articles themselves — a recent alert on an old article would
                # otherwise trip the alerts.article_id foreign key.
                cur = await self.conn.execute(
                    "DELETE FROM alerts "
                    "WHERE article_id IN (SELECT id FROM news_articles WHERE first_seen_at < ?)",
                    (cutoff_articles,),
                )
                al = cur.rowcount or 0
                cur = await self.conn.execute(
                    "DELETE FROM news_articles WHERE first_seen_at < ?",
                    (cutoff_articles,),
                )
                a = cur.rowcount or 0
                cur = await self.conn.execute(
                    "DELETE FROM raw_events WHERE received_at < ?",
                    (cutoff_raw,),
                )
                r = cur.rowcount or 0
                cur = await self.conn.execute(
                    "DELETE FROM alerts WHERE created_at < ?",
                    (cutoff_articles,),
                )
                al += cur.rowcount or 0
                await self.conn.commit()
            except BaseException:
                await self.conn.rollback()
                raise
        return {
            "articles": a,
            "versions": v,
            "symbol_index": s,
            "raw_events": r,
            "alerts": al,
        }

    @staticmethod
    def _row_to_article(row: aiosqlite.Row) -> NewsArticle:
        symbols = json.loads(row["symbols_json"] or "[]")
        try:
            raw = json.loads(row["raw_json"]) if row["raw_json"] else None
        except json.JSONDecodeError:
            raw = None
        return NewsArticle(
            id=row["id"],
            headline=row["headline"],
            summary=row["summary"],
            author=row["author"],
            created_at=row["created_at"],
            updated_at=row["updated_at"],
            content_html=row["content_html"],
            content_text=row["content_text"],
            url=row["url"],
            source=row["source"],
            symbols=symbols,
            first_seen_at=row["first_seen_at"],
            last_seen_at=row["last_seen_at"],
            last_seen_source=row["last_seen_source"],
            update_count=row["update_count"] or 0,
            latency_ms=row["latency_ms"],
            is_content_present=bool(row["is_content_present"]),
            seq=row["seq"] if "seq" in row.keys() else None,
            raw=raw,
        )

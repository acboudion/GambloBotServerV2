"""SQLite (aiosqlite) persistence with WAL and a single-writer lock."""

from __future__ import annotations

import asyncio
import json
import statistics
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Any

import aiosqlite

from .logging_utils import get_logger
from .models import Alert, IngestionStats, LatencyStats, NewsArticle, NewsArticleVersion
from .normalize import NormalizedArticle

log = get_logger(__name__)


@dataclass
class UpsertResult:
    article_id: int
    was_new: bool
    version_inserted: bool
    article: NewsArticle


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()

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
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("Store is not open")
        return self._conn

    async def init_schema(self) -> None:
        sql_text = resources.files("alpaca_news_mcp").joinpath("schema.sql").read_text(
            encoding="utf-8"
        )
        async with self._write_lock:
            await self.conn.executescript(sql_text)
            await self.conn.commit()

    async def upsert_article(
        self,
        normalized: NormalizedArticle,
        *,
        source_kind: str,
    ) -> UpsertResult:
        """Insert or update an article. Returns whether new and whether a version was added."""
        async with self._write_lock:
            cur = await self.conn.execute(
                """SELECT id, headline, summary, content_html, updated_at, update_count, symbols_json
                   FROM news_articles WHERE id = ?""",
                (normalized.id,),
            )
            existing = await cur.fetchone()
            await cur.close()

            now = _utcnow_iso()
            symbols_json = json.dumps(normalized.symbols)
            was_new = existing is None
            version_inserted = False

            if existing is None:
                await self.conn.execute(
                    """
                    INSERT INTO news_articles (
                        id, headline, summary, author, created_at, updated_at,
                        content_html, content_text, url, source,
                        symbols_json, raw_json,
                        first_seen_at, last_seen_at, last_seen_source,
                        update_count, latency_ms, is_content_present
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?,
                        ?, ?,
                        ?, ?, ?,
                        0, ?, ?
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
                    ),
                )
                version_inserted = await self._insert_version(
                    normalized, source_kind=source_kind
                )
                await self._upsert_symbol_index(normalized, source_kind=source_kind)
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
                        is_content_present = MAX(is_content_present, ?)
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

            await self.conn.commit()

        article = await self.get_article(normalized.id)
        assert article is not None
        return UpsertResult(
            article_id=normalized.id,
            was_new=was_new,
            version_inserted=version_inserted,
            article=article,
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

    async def record_alert(self, alert: Alert, *, raw_json: str) -> bool:
        """Insert an alert. Returns True if inserted, False if (article_id, category) already exists."""
        async with self._write_lock:
            try:
                await self.conn.execute(
                    """
                    INSERT INTO alerts (
                        alert_id, article_id, created_at, severity, category,
                        symbols_json, headline, reason, acknowledged, raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
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
                    ),
                )
                await self.conn.commit()
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
        cur = await self.conn.execute(
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
        cur = await self.conn.execute(
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
        cur = await self.conn.execute(sql, params)
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
        like = f"%{query.lower()}%"
        clauses = ["(LOWER(headline) LIKE ? OR LOWER(summary) LIKE ? OR LOWER(content_text) LIKE ?)"]
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
        cur = await self.conn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [self._row_to_article(r) for r in rows]

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
            cur = await self.conn.execute(
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
            cur = await self.conn.execute(
                "SELECT * FROM news_article_versions WHERE article_id = ? "
                "ORDER BY received_at ASC",
                (article_id,),
            )
        else:
            # Pull the most recent `limit` rows at the SQL level so we don't
            # read or deserialize the full version history, then re-sort ASC
            # to keep the existing oldest-first response ordering.
            cur = await self.conn.execute(
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
        cur = await self.conn.execute(
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
        cur = await self.conn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [
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
            )
            for r in rows
        ]

    async def latency_stats(self, minutes: int) -> LatencyStats:
        since = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
        cur = await self.conn.execute(
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
            cur = await self.conn.execute(sql, (since,))
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
        cur = await self.conn.execute(
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
        self, *, minutes: int, min_articles: int
    ) -> dict[str, int]:
        since = (datetime.now(UTC) - timedelta(minutes=minutes)).isoformat()
        cur = await self.conn.execute(
            """
            SELECT symbol, COUNT(DISTINCT article_id) as c
            FROM news_symbol_index
            WHERE received_at >= ?
            GROUP BY symbol
            HAVING c >= ?
            ORDER BY c DESC, symbol ASC
            """,
            (since, min_articles),
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
            al = cur.rowcount or 0
            await self.conn.commit()
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
            raw=raw,
        )


def chunked(iterable: Iterable[Any], n: int) -> Iterable[list[Any]]:
    buf: list[Any] = []
    for item in iterable:
        buf.append(item)
        if len(buf) >= n:
            yield buf
            buf = []
    if buf:
        yield buf

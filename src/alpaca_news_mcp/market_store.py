"""SQLite persistence for stock market data (separate DB from news).

Write path is batch-only: the stock stream worker drains its queue and calls
persist_stream_batch once per drain — executemany per table, single commit.
An in-memory MarketSnapshotCache holds the latest trade/quote/bar/status/LULD
per symbol so `get_latest_*` tools never touch the database.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from threading import RLock
from typing import Any

import aiosqlite

from .logging_utils import get_logger
from .store import open_read_connection

log = get_logger(__name__)

# Chunked retention deletes: bounded work per lock acquisition so a prune pass
# during market hours can't starve the ingest batch writer.
RETENTION_DELETE_CHUNK = 50_000

TICK_TABLES = ("stock_trades", "stock_quotes")
# Halt/LULD rows are a few per day, tiny, and decision-critical (an active
# halt must stay visible to get_trading_halts) — they get a day-scale
# retention instead of the 4-hour tick window.
EVENT_TABLES = ("stock_statuses", "stock_lulds")

# market_status row persisting the bar-seq allocation high-water mark; see
# store.SEQ_WATERMARKS_KEY for why MAX(seq) alone is not restart-safe.
BAR_SEQ_KEY = "bar_seq_high_water"


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


@dataclass
class MarketSnapshotCache:
    """Latest market data per symbol, updated on every persisted batch."""

    _data: dict[str, dict[str, Any]] = field(default_factory=dict)
    _lock: RLock = field(default_factory=RLock)

    def update(self, symbol: str, kind: str, value: dict[str, Any]) -> None:
        with self._lock:
            slot = self._data.setdefault(symbol, {})
            existing = slot.get(kind)
            # Ticks can arrive out of order across batches; keep the newest.
            if existing is not None and existing.get("ts_us") and value.get("ts_us"):
                if value["ts_us"] < existing["ts_us"]:
                    return
            slot[kind] = value

    def get(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            snap = self._data.get(symbol.upper())
            return dict(snap) if snap else None

    def symbols(self) -> list[str]:
        with self._lock:
            return sorted(self._data)

    def remove(self, symbol: str) -> None:
        with self._lock:
            self._data.pop(symbol.upper(), None)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


class MarketStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self._conn: aiosqlite.Connection | None = None
        self._read_conn: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()
        self._next_bar_seq: int = 1
        self.snapshots = MarketSnapshotCache()

    @classmethod
    async def open(cls, path: str) -> MarketStore:
        store = cls(path)
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
            raise RuntimeError("MarketStore is not open")
        return self._conn

    @property
    def rconn(self) -> aiosqlite.Connection:
        """Read connection for tool queries; the writer when unavailable."""
        return self._read_conn if self._read_conn is not None else self.conn

    async def init_schema(self) -> None:
        sql_text = (
            resources.files("alpaca_news_mcp")
            .joinpath("market_schema.sql")
            .read_text(encoding="utf-8")
        )
        async with self._write_lock:
            await self.conn.executescript(sql_text)
            await self.conn.commit()
            cur = await self.conn.execute("SELECT COALESCE(MAX(seq), 0) FROM stock_bars")
            row = await cur.fetchone()
            await cur.close()
            max_bar_seq = int(row[0]) if row else 0
            cur = await self.conn.execute(
                "SELECT value_json FROM market_status WHERE key = ?", (BAR_SEQ_KEY,)
            )
            row = await cur.fetchone()
            await cur.close()
            persisted = 0
            if row is not None:
                try:
                    persisted = int(json.loads(row["value_json"]).get("seq", 0))
                except (json.JSONDecodeError, TypeError, ValueError):
                    persisted = 0
            self._next_bar_seq = max(max_bar_seq, persisted) + 1
        # After the schema exists and WAL mode is committed — mode=ro on a
        # nonexistent file would fail.
        self._read_conn = await open_read_connection(self.path)

    # ---- batch write path -----------------------------------------------------

    async def persist_stream_batch(
        self,
        *,
        trades: list[tuple[Any, ...]] | None = None,
        quotes: list[tuple[Any, ...]] | None = None,
        bars: list[tuple[Any, ...]] | None = None,
        statuses: list[tuple[Any, ...]] | None = None,
        lulds: list[tuple[Any, ...]] | None = None,
        raw_events: list[tuple[str, str]] | None = None,
    ) -> None:
        """One transaction for a whole queue drain.

        Row shapes (see stock_stream._rows_from_batch):
          trades:   (symbol, ts_us, price, size, exchange, conditions, tape, trade_id)
          quotes:   (symbol, ts_us, bp, bs, bx, ap, asz, ax, conditions, tape)
          bars:     (symbol, timeframe, ts, o, h, l, c, v, n, vw)
          statuses: (symbol, ts_us, status_code, status_msg, reason_code, reason_msg, tape)
          lulds:    (symbol, ts_us, limit_up, limit_down, indicator, tape)
          raw_events: (message_type, raw_json)
        """
        now = _utcnow_iso()
        async with self._write_lock:
            # All-or-nothing: a failure mid-batch (or at commit) must roll
            # back, or a later successful write would commit earlier partial
            # rows without their matching snapshots/alerts.
            try:
                if trades:
                    await self.conn.executemany(
                        "INSERT INTO stock_trades "
                        "(symbol, ts_us, price, size, exchange, conditions, tape, trade_id) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        trades,
                    )
                if quotes:
                    await self.conn.executemany(
                        "INSERT INTO stock_quotes "
                        "(symbol, ts_us, bid_price, bid_size, bid_exchange, "
                        " ask_price, ask_size, ask_exchange, conditions, tape) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        quotes,
                    )
                if bars:
                    rows = [(*bar, now, self._allocate_bar_seq()) for bar in bars]
                    # The WHERE clause makes redelivery idempotent: an
                    # unchanged bar keeps its existing seq and stays
                    # invisible to get_bars_since cursors — without it,
                    # every reconnect gap-fill re-delivered its whole
                    # fetched window as "new" bars. (IS NOT is SQLite's
                    # null-safe distinct comparison.)
                    await self.conn.executemany(
                        "INSERT INTO stock_bars "
                        "(symbol, timeframe, ts, open, high, low, close, volume, "
                        " trade_count, vwap, updated_at, seq) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                        "ON CONFLICT(symbol, timeframe, ts) DO UPDATE SET "
                        "  open = excluded.open, high = excluded.high, low = excluded.low, "
                        "  close = excluded.close, volume = excluded.volume, "
                        "  trade_count = excluded.trade_count, vwap = excluded.vwap, "
                        "  updated_at = excluded.updated_at, seq = excluded.seq "
                        "WHERE open IS NOT excluded.open OR high IS NOT excluded.high "
                        "  OR low IS NOT excluded.low OR close IS NOT excluded.close "
                        "  OR volume IS NOT excluded.volume "
                        "  OR trade_count IS NOT excluded.trade_count "
                        "  OR vwap IS NOT excluded.vwap",
                        rows,
                    )
                    # Same transaction as the seq-consuming write, so any
                    # committed bar's seq is covered by the committed mark.
                    await self.conn.execute(
                        "INSERT INTO market_status (key, value_json, updated_at) "
                        "VALUES (?, ?, ?) "
                        "ON CONFLICT(key) DO UPDATE SET "
                        "  value_json = excluded.value_json, "
                        "  updated_at = excluded.updated_at",
                        (
                            BAR_SEQ_KEY,
                            json.dumps({"seq": self._next_bar_seq - 1}),
                            now,
                        ),
                    )
                if statuses:
                    await self.conn.executemany(
                        "INSERT INTO stock_statuses "
                        "(symbol, ts_us, status_code, status_msg, reason_code, reason_msg, tape) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?)",
                        statuses,
                    )
                if lulds:
                    await self.conn.executemany(
                        "INSERT INTO stock_lulds "
                        "(symbol, ts_us, limit_up, limit_down, indicator, tape) "
                        "VALUES (?, ?, ?, ?, ?, ?)",
                        lulds,
                    )
                if raw_events:
                    await self.conn.executemany(
                        "INSERT INTO market_raw_events (received_at, message_type, raw_json) "
                        "VALUES (?, ?, ?)",
                        [(now, mt, rj) for mt, rj in raw_events],
                    )
                await self.conn.commit()
            except BaseException:
                await self.conn.rollback()
                raise

    def _allocate_bar_seq(self) -> int:
        seq = self._next_bar_seq
        self._next_bar_seq += 1
        return seq

    async def upsert_bars(self, bars: list[tuple[Any, ...]]) -> int:
        """REST backfill entry point: same bar row shape as persist_stream_batch."""
        if not bars:
            return 0
        await self.persist_stream_batch(bars=bars)
        return len(bars)

    # ---- reads ------------------------------------------------------------------

    async def trades_window(
        self, symbol: str, *, since_us: int, limit: int
    ) -> list[dict[str, Any]]:
        cur = await self.rconn.execute(
            "SELECT * FROM ("
            "  SELECT ts_us, price, size, exchange, conditions, tape FROM stock_trades"
            "  WHERE symbol = ? AND ts_us >= ? ORDER BY ts_us DESC LIMIT ?"
            ") ORDER BY ts_us ASC",
            (symbol.upper(), since_us, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def quotes_window(
        self, symbol: str, *, since_us: int, limit: int
    ) -> list[dict[str, Any]]:
        cur = await self.rconn.execute(
            "SELECT * FROM ("
            "  SELECT ts_us, bid_price, bid_size, ask_price, ask_size, tape FROM stock_quotes"
            "  WHERE symbol = ? AND ts_us >= ? ORDER BY ts_us DESC LIMIT ?"
            ") ORDER BY ts_us ASC",
            (symbol.upper(), since_us, limit),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def bars_window(
        self,
        symbol: str,
        *,
        timeframe: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        limit: int = 390,
    ) -> list[dict[str, Any]]:
        clauses = ["symbol = ?", "timeframe = ?"]
        params: list[Any] = [symbol.upper(), timeframe]
        if start_ts is not None:
            clauses.append("ts >= ?")
            params.append(start_ts)
        if end_ts is not None:
            clauses.append("ts <= ?")
            params.append(end_ts)
        sql = (
            "SELECT * FROM ("
            "  SELECT ts, open, high, low, close, volume, trade_count, vwap FROM stock_bars"
            f"  WHERE {' AND '.join(clauses)} ORDER BY ts DESC LIMIT ?"
            ") ORDER BY ts ASC"
        )
        params.append(limit)
        cur = await self.rconn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def bars_window_aggregated(
        self,
        symbol: str,
        *,
        bucket_seconds: int,
        start_ts: int | None = None,
        end_ts: int | None = None,
        limit: int = 390,
    ) -> list[dict[str, Any]]:
        """Server-side multi-timeframe aggregation over stored 1min bars:
        5min/15min/1hour structure at a fraction of the row count (and token
        cost) of raw minute bars. UTC-aligned buckets; vwap is re-weighted by
        volume across the bucket."""
        clauses = ["symbol = ?", "timeframe = '1min'"]
        params: list[Any] = [symbol.strip().upper()]
        if start_ts is not None:
            clauses.append("ts >= ?")
            params.append(start_ts)
        if end_ts is not None:
            clauses.append("ts <= ?")
            params.append(end_ts)
        sql = f"""
            SELECT ts, open, high, low, close, volume, trade_count, vwap FROM (
              SELECT bucket_ts AS ts,
                     FIRST_VALUE(open) OVER w AS open,
                     MAX(high) OVER w AS high,
                     MIN(low) OVER w AS low,
                     LAST_VALUE(close) OVER w AS close,
                     SUM(volume) OVER w AS volume,
                     SUM(trade_count) OVER w AS trade_count,
                     CAST(SUM(vwap * volume) OVER w AS REAL)
                         / NULLIF(SUM(volume) OVER w, 0) AS vwap,
                     ROW_NUMBER() OVER (
                         PARTITION BY bucket_ts ORDER BY bar_ts DESC
                     ) AS rn
              FROM (
                SELECT ts - (ts % ?) AS bucket_ts, ts AS bar_ts,
                       open, high, low, close, volume, trade_count, vwap
                FROM stock_bars
                WHERE {" AND ".join(clauses)}
              )
              WINDOW w AS (
                PARTITION BY bucket_ts ORDER BY bar_ts
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
              )
            ) WHERE rn = 1
            ORDER BY ts DESC LIMIT ?
        """
        cur = await self.rconn.execute(sql, (bucket_seconds, *params, limit))
        rows = await cur.fetchall()
        await cur.close()
        out = [dict(r) for r in rows]
        out.reverse()  # most-recent-limited, returned ascending like bars_window
        return out

    async def tape_stats(
        self,
        symbol: str,
        *,
        since_us: int,
        bucket_us: int,
        block_size: int = 1000,
    ) -> list[dict[str, Any]]:
        """Bucketed trade-tape digest: tape speed, uptick/downtick counts via
        the tick rule (LAG over price), block-trade count, bucket VWAP."""
        cur = await self.rconn.execute(
            """
            WITH t AS (
              SELECT ts_us, price, size,
                     LAG(price) OVER (ORDER BY ts_us) AS prev_price
              FROM stock_trades WHERE symbol = ? AND ts_us >= ?
            )
            SELECT (ts_us / ?) * ? AS bucket_us,
                   COUNT(*) AS trades,
                   SUM(size) AS volume,
                   CAST(SUM(price * size) AS REAL) / NULLIF(SUM(size), 0) AS vwap,
                   SUM(CASE WHEN prev_price IS NOT NULL AND price > prev_price
                       THEN 1 ELSE 0 END) AS upticks,
                   SUM(CASE WHEN prev_price IS NOT NULL AND price < prev_price
                       THEN 1 ELSE 0 END) AS downticks,
                   SUM(CASE WHEN size >= ? THEN 1 ELSE 0 END) AS block_trades,
                   MIN(price) AS low, MAX(price) AS high
            FROM t GROUP BY bucket_us ORDER BY bucket_us ASC
            """,
            (symbol.strip().upper(), since_us, bucket_us, bucket_us, block_size),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def quote_stats(
        self, symbol: str, *, since_us: int, bucket_us: int
    ) -> list[dict[str, Any]]:
        """Bucketed NBBO digest: average spread in bps and bid-size fraction
        (order-book imbalance proxy)."""
        cur = await self.rconn.execute(
            """
            SELECT (ts_us / ?) * ? AS bucket_us,
                   COUNT(*) AS quotes,
                   AVG((ask_price - bid_price) * 20000.0
                       / NULLIF(ask_price + bid_price, 0)) AS spread_bps,
                   AVG(CAST(bid_size AS REAL)
                       / NULLIF(bid_size + ask_size, 0)) AS bid_fraction
            FROM stock_quotes
            WHERE symbol = ? AND ts_us >= ? AND bid_price > 0 AND ask_price > 0
            GROUP BY bucket_us ORDER BY bucket_us ASC
            """,
            (bucket_us, bucket_us, symbol.strip().upper(), since_us),
        )
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def watchlist_pulse(
        self, symbols: list[str], *, since_us: int
    ) -> dict[str, dict[str, Any]]:
        """One aggregate row per symbol over the window: first/last price,
        change, volume, trade count, average spread.

        Every requested symbol gets a row — a name with no trades in the
        window (halted, illiquid) must show zero activity, not silently
        vanish from the watchlist heartbeat."""
        if not symbols:
            return {}
        upper = sorted({s.strip().upper() for s in symbols if s.strip()})
        placeholders = ",".join("?" * len(upper))
        out: dict[str, dict[str, Any]] = {
            sym: {
                "symbol": sym,
                "trades": 0,
                "volume": 0,
                "low": None,
                "high": None,
            }
            for sym in upper
        }
        cur = await self.rconn.execute(
            f"""
            SELECT symbol, COUNT(*) AS trades, SUM(size) AS volume,
                   MIN(price) AS low, MAX(price) AS high
            FROM stock_trades WHERE ts_us >= ? AND symbol IN ({placeholders})
            GROUP BY symbol
            """,
            (since_us, *upper),
        )
        rows = await cur.fetchall()
        await cur.close()
        any_trades = False
        for r in rows:
            out[r["symbol"]].update(dict(r))
            any_trades = True
        if any_trades:
            cur = await self.rconn.execute(
                f"""
                SELECT symbol, price, kind FROM (
                  SELECT symbol, price, 'first' AS kind,
                         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts_us ASC) AS rn
                  FROM stock_trades WHERE ts_us >= ? AND symbol IN ({placeholders})
                ) WHERE rn = 1
                UNION ALL
                SELECT symbol, price, kind FROM (
                  SELECT symbol, price, 'last' AS kind,
                         ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts_us DESC) AS rn
                  FROM stock_trades WHERE ts_us >= ? AND symbol IN ({placeholders})
                ) WHERE rn = 1
                """,
                (since_us, *upper, since_us, *upper),
            )
            rows = await cur.fetchall()
            await cur.close()
            for r in rows:
                entry = out.setdefault(r["symbol"], {})
                entry["first_price" if r["kind"] == "first" else "last_price"] = (
                    r["price"]
                )
        # Quotes run regardless of trades: halted/illiquid names can still
        # quote, and their spread is exactly the signal worth surfacing.
        cur = await self.rconn.execute(
            f"""
            SELECT symbol,
                   AVG((ask_price - bid_price) * 20000.0
                       / NULLIF(ask_price + bid_price, 0)) AS spread_bps
            FROM stock_quotes
            WHERE ts_us >= ? AND symbol IN ({placeholders})
              AND bid_price > 0 AND ask_price > 0
            GROUP BY symbol
            """,
            (since_us, *upper),
        )
        rows = await cur.fetchall()
        await cur.close()
        for r in rows:
            if r["symbol"] in out:
                out[r["symbol"]]["spread_bps"] = (
                    round(r["spread_bps"], 2) if r["spread_bps"] is not None else None
                )
        return out

    @property
    def latest_bar_cursor(self) -> int:
        """Highest allocated bar seq; see Store.latest_article_cursor."""
        return self._next_bar_seq - 1

    async def min_bar_seq(self) -> int:
        cur = await self.rconn.execute("SELECT COALESCE(MIN(seq), 0) FROM stock_bars")
        row = await cur.fetchone()
        await cur.close()
        return int(row[0]) if row else 0

    async def bars_since(
        self,
        *,
        cursor: int,
        limit: int,
        symbols: list[str] | None = None,
        timeframe: str | None = None,
    ) -> tuple[list[dict[str, Any]], int, bool]:
        clauses = ["seq > ?"]
        params: list[Any] = [cursor]
        if symbols:
            placeholders = ",".join("?" * len(symbols))
            clauses.append(f"symbol IN ({placeholders})")
            params.extend([s.upper() for s in symbols])
        if timeframe:
            clauses.append("timeframe = ?")
            params.append(timeframe)
        sql = (
            "SELECT symbol, timeframe, ts, open, high, low, close, volume, "
            "trade_count, vwap, seq FROM stock_bars WHERE "
            + " AND ".join(clauses)
            + " ORDER BY seq ASC LIMIT ?"
        )
        params.append(limit + 1)
        cur = await self.rconn.execute(sql, params)
        rows = list(await cur.fetchall())
        await cur.close()
        has_more = len(rows) > limit
        rows = rows[:limit]
        out = [dict(r) for r in rows]
        next_cursor = out[-1]["seq"] if out else cursor
        return out, next_cursor, has_more

    async def latest_bar_ts(self, symbols: list[str], *, timeframe: str = "1min") -> dict[str, int]:
        if not symbols:
            return {}
        placeholders = ",".join("?" * len(symbols))
        cur = await self.rconn.execute(
            f"SELECT symbol, MAX(ts) AS max_ts FROM stock_bars "
            f"WHERE timeframe = ? AND symbol IN ({placeholders}) GROUP BY symbol",
            (timeframe, *[s.upper() for s in symbols]),
        )
        rows = await cur.fetchall()
        await cur.close()
        return {r["symbol"]: int(r["max_ts"]) for r in rows if r["max_ts"] is not None}

    async def recent_statuses(
        self, *, minutes: int, symbols: list[str] | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        since_us = int(
            (datetime.now(UTC) - timedelta(minutes=minutes)).timestamp() * 1_000_000
        )
        clauses = ["ts_us >= ?"]
        params: list[Any] = [since_us]
        if symbols:
            placeholders = ",".join("?" * len(symbols))
            clauses.append(f"symbol IN ({placeholders})")
            params.extend([s.upper() for s in symbols])
        sql = (
            "SELECT symbol, ts_us, status_code, status_msg, reason_code, reason_msg, tape "
            "FROM stock_statuses WHERE "
            + " AND ".join(clauses)
            + " ORDER BY ts_us DESC LIMIT ?"
        )
        params.append(limit)
        cur = await self.rconn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def recent_lulds(
        self, *, minutes: int, symbols: list[str] | None = None, limit: int = 200
    ) -> list[dict[str, Any]]:
        since_us = int(
            (datetime.now(UTC) - timedelta(minutes=minutes)).timestamp() * 1_000_000
        )
        clauses = ["ts_us >= ?"]
        params: list[Any] = [since_us]
        if symbols:
            placeholders = ",".join("?" * len(symbols))
            clauses.append(f"symbol IN ({placeholders})")
            params.extend([s.upper() for s in symbols])
        sql = (
            "SELECT symbol, ts_us, limit_up, limit_down, indicator, tape "
            "FROM stock_lulds WHERE "
            + " AND ".join(clauses)
            + " ORDER BY ts_us DESC LIMIT ?"
        )
        params.append(limit)
        cur = await self.rconn.execute(sql, params)
        rows = await cur.fetchall()
        await cur.close()
        return [dict(r) for r in rows]

    async def ingest_counts(self, *, minutes: int) -> dict[str, int]:
        since_us = int(
            (datetime.now(UTC) - timedelta(minutes=minutes)).timestamp() * 1_000_000
        )
        out: dict[str, int] = {}
        for table in TICK_TABLES + EVENT_TABLES:
            cur = await self.rconn.execute(
                f"SELECT COUNT(*) AS c FROM {table} WHERE ts_us >= ?", (since_us,)
            )
            row = await cur.fetchone()
            await cur.close()
            out[table.removeprefix("stock_")] = int(row["c"]) if row else 0
        since_s = since_us // 1_000_000
        cur = await self.rconn.execute(
            "SELECT COUNT(*) AS c FROM stock_bars WHERE ts >= ?", (since_s,)
        )
        row = await cur.fetchone()
        await cur.close()
        out["bars"] = int(row["c"]) if row else 0
        return out

    # ---- retention -----------------------------------------------------------------

    async def prune(
        self,
        *,
        tick_retention_minutes: int,
        bar_retention_days: int,
        status_retention_days: int = 30,
    ) -> dict[str, int]:
        now = datetime.now(UTC)
        tick_cutoff_us = int(
            (now - timedelta(minutes=tick_retention_minutes)).timestamp() * 1_000_000
        )
        event_cutoff_us = int(
            (now - timedelta(days=status_retention_days)).timestamp() * 1_000_000
        )
        bar_cutoff_s = int((now - timedelta(days=bar_retention_days)).timestamp())
        raw_cutoff_iso = (now - timedelta(minutes=tick_retention_minutes)).isoformat()
        deleted: dict[str, int] = {}
        for table in TICK_TABLES:
            deleted[table.removeprefix("stock_")] = await self._chunked_delete(
                table, "ts_us", tick_cutoff_us
            )
        for table in EVENT_TABLES:
            deleted[table.removeprefix("stock_")] = await self._chunked_delete(
                table, "ts_us", event_cutoff_us
            )
        # stock_bars is WITHOUT ROWID (no rowid for chunking) but tiny relative
        # to ticks (~390 rows/symbol/day), so a direct delete is fine.
        async with self._write_lock:
            cur = await self.conn.execute(
                "DELETE FROM stock_bars WHERE ts < ?", (bar_cutoff_s,)
            )
            deleted["bars"] = cur.rowcount or 0
            await self.conn.commit()
        deleted["raw_events"] = await self._chunked_delete(
            "market_raw_events", "received_at", raw_cutoff_iso
        )
        async with self._write_lock:
            await self.conn.execute("PRAGMA incremental_vacuum(2000)")
            await self.conn.commit()
        return deleted

    async def _chunked_delete(self, table: str, ts_column: str, cutoff: Any) -> int:
        """Delete old rows in bounded chunks, releasing the write lock between
        chunks so ingest batches interleave during market hours."""
        total = 0
        while True:
            async with self._write_lock:
                cur = await self.conn.execute(
                    f"DELETE FROM {table} WHERE rowid IN ("
                    f"  SELECT rowid FROM {table} WHERE {ts_column} < ? LIMIT ?"
                    ")",
                    (cutoff, RETENTION_DELETE_CHUNK),
                )
                deleted = cur.rowcount or 0
                await self.conn.commit()
            total += deleted
            if deleted < RETENTION_DELETE_CHUNK:
                return total

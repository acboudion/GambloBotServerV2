"""MCP tools for the stock market-data stream and market store.

All read-only except set_stock_watchlist (local subscription mutation — it
never places trades). Latest-data reads are served from the in-memory
snapshot cache; windows/bars come from the market SQLite database.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from dateutil import parser as dateparser
from mcp.server.fastmcp import FastMCP

from .app_state import AppState, get_app_state
from .tool_common import (
    client_unavailable as _client_unavailable,
)
from .tool_common import (
    cursor_out_of_range as _cursor_out_of_range,
)
from .tool_common import (
    err as _err,
)
from .tool_common import (
    invalid_date as _invalid_date,
)
from .tool_common import (
    limit_exceeded as _limit_exceeded,
)
from .tool_common import (
    stream_disabled as _stream_disabled,
)
from .tool_common import (
    upstream_error as _upstream_error,
)

MAX_LATEST_SYMBOLS = 50
# A stream-cached snapshot older than this is flagged stale — a bot must
# never mistake pre-disconnect prices for live ones.
SNAPSHOT_STALE_SECONDS = 120.0
MAX_WINDOW_MINUTES = 240
MAX_WINDOW_ROWS = 1000
MAX_BARS = 1000
MAX_BARS_CURSOR = 2000
MAX_STATUSES = 200
MAX_MOVERS_TOP = 50
MAX_ACTIVES_TOP = 100
MAX_SNAPSHOT_SYMBOLS = 50
MAX_CALENDAR_DAYS = 90
MAX_CORPORATE_ACTIONS = 200
CORPORATE_ACTIONS_MAX_RANGE_DAYS = 90

VALID_INCLUDE = ("trade", "quote", "bar", "daily_bar", "status", "luld")
# Timeframes stored as rows (stream-ingested).
VALID_TIMEFRAMES = ("1min", "1day")
# Timeframes served by SQL aggregation over stored 1min bars.
AGGREGATE_TIMEFRAMES = {"5min": 300, "15min": 900, "1hour": 3600}
BAR_TIMEFRAMES = VALID_TIMEFRAMES + tuple(AGGREGATE_TIMEFRAMES)
MAX_CONTEXT_SYMBOLS = 20
MAX_TAPE_BUCKETS = 360
MAX_PULSE_TOP = 50

# Snapshot-cache slot names per include key.
_INCLUDE_TO_SLOT = {
    "trade": "trade",
    "quote": "quote",
    "bar": "minute_bar",
    "daily_bar": "daily_bar",
    "status": "status",
    "luld": "luld",
}


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = dateparser.isoparse(value)
    except (ValueError, TypeError):
        return None
    return dt.replace(tzinfo=dt.tzinfo or UTC)


def _market_parts(app: AppState) -> tuple[Any, Any] | None:
    if app.stock_stream is None or app.market_store is None:
        return None
    return app.stock_stream, app.market_store


def _iso_to_epoch_s(value: str | None) -> int | None:
    if not value:
        return None
    try:
        dt = dateparser.isoparse(value)
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp())


def _minutes_ago_us(minutes: int) -> int:
    return int((datetime.now(UTC) - timedelta(minutes=minutes)).timestamp() * 1_000_000)


def _freshest_age_seconds(entry: dict[str, Any], now_us: int) -> float | None:
    """Age of the newest data point across an entry's slots. Stream-cache
    slots carry ts_us (epoch µs); REST-filled slots carry Alpaca's 't'
    RFC3339 field."""
    freshest: int | None = None
    for value in entry.values():
        if not isinstance(value, dict):
            continue
        ts_us = value.get("ts_us")
        if not isinstance(ts_us, int):
            ts_us = _iso_ts_to_us(value.get("t"))
        if ts_us is not None and (freshest is None or ts_us > freshest):
            freshest = ts_us
    if freshest is None:
        return None
    return round(max(0.0, (now_us - freshest) / 1_000_000), 1)


def _iso_ts_to_us(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        dt = dateparser.isoparse(value)
    except (ValueError, TypeError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1_000_000)


def register(mcp: FastMCP) -> None:
    # ---- stream health / watchlist ------------------------------------------------

    @mcp.tool(description="Stock market-data stream health, watchlist, and ingest counts.")
    async def get_stock_stream_health() -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        stream, market_store = parts
        h = app.state.snapshot_health("stocks")
        d = h.model_dump()
        d["watchlist"] = stream.watchlist()
        d["channels"] = stream.channels()
        d["queue_depth"] = stream._queue.qsize()
        d["ingest_counts_15m"] = await market_store.ingest_counts(minutes=15)
        return d

    @mcp.tool(
        description=(
            "Update the stock-stream watchlist (max 100 symbols). "
            "mode: replace|add|remove. Applies live when connected and "
            "persists across restarts. This changes data subscriptions only — "
            "it never trades."
        )
    )
    async def set_stock_watchlist(
        symbols: list[str], mode: str = "replace"
    ) -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        stream, _ = parts
        return await stream.update_watchlist(symbols, mode=mode)

    @mcp.tool(description="Current stock-stream watchlist, channels, and Alpaca-acknowledged subscription.")
    async def get_stock_watchlist() -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        stream, _ = parts
        return {
            "watchlist": stream.watchlist(),
            "channels": stream.channels(),
            "acknowledged": stream.acknowledged_subscription(),
        }

    # ---- latest data (memory-first) --------------------------------------------------

    @mcp.tool(
        description=(
            "Latest market data per symbol from the live stream cache. "
            "include: any of trade, quote, bar, daily_bar, status, luld. "
            "Symbols not on the watchlist appear under 'missing' — add them "
            "with set_stock_watchlist or use get_stock_snapshots (REST). "
            "age_seconds gives per-symbol data age; symbols in 'stale' "
            "should not be trusted for live pricing (stream disconnected or "
            "data older than 120s)."
        )
    )
    async def get_latest_market_data(
        symbols: list[str],
        include: list[str] | None = None,
    ) -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        _, market_store = parts
        if not symbols:
            return _err("no_symbols", "pass at least one ticker symbol")
        if len(symbols) > MAX_LATEST_SYMBOLS:
            return _limit_exceeded(MAX_LATEST_SYMBOLS, len(symbols))
        include = include or ["trade", "quote", "bar"]
        invalid = [i for i in include if i not in VALID_INCLUDE]
        if invalid:
            return _err("invalid_include", f"use any of: {', '.join(VALID_INCLUDE)}", invalid=invalid, valid=list(VALID_INCLUDE))
        out: dict[str, Any] = {}
        missing: list[str] = []
        for sym in symbols:
            sym_u = sym.strip().upper()
            snap = market_store.snapshots.get(sym_u)
            if not snap:
                missing.append(sym_u)
                continue
            entry: dict[str, Any] = {}
            for key in include:
                value = snap.get(_INCLUDE_TO_SLOT[key])
                if value is not None:
                    entry[key] = value
            out[sym_u] = entry
        source = "stream"
        # REST fallback for symbols outside the stream cache (trade/quote/bar
        # only — statuses/LULD have no latest REST endpoint).
        rest_kinds = {"trade": "trades", "quote": "quotes", "bar": "bars"}
        wanted_rest = [k for k in include if k in rest_kinds]
        filled: set[str] = set()
        if missing and app.market_client is not None and wanted_rest:
            for key in wanted_rest:
                payload = await app.market_client.latest(rest_kinds[key], missing)
                if not payload:
                    continue
                for sym_u, value in (payload.get(rest_kinds[key]) or {}).items():
                    sym_up = sym_u.upper()
                    out.setdefault(sym_up, {})[key] = value
                    filled.add(sym_up)
            if filled:
                source = "stream+rest"
            missing = sorted(s for s in missing if s not in filled)
        # Any requested field absent from a returned entry — whether the entry
        # came from a partial stream snapshot or a partial REST fill — is
        # reported per symbol so incomplete context never looks complete.
        # status/luld are event-driven: absence means "no event", not a miss.
        reportable = [k for k in include if k in ("trade", "quote", "bar", "daily_bar")]
        missing_fields = {
            sym: absent
            for sym, entry in sorted(out.items())
            if (absent := sorted(k for k in reportable if k not in entry))
        }
        # Staleness: a fresh as_of must never dress up pre-disconnect cache
        # entries as live prices. REST-filled symbols are live by definition;
        # stream-cached ones are stale when the stream is down or the data
        # is old.
        now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
        stream_connected = bool(app.state.snapshot_health("stocks").connected)
        age_seconds: dict[str, float] = {}
        stale: list[str] = []
        for sym, entry in out.items():
            age = _freshest_age_seconds(entry, now_us)
            if age is not None:
                age_seconds[sym] = age
            from_stream = sym not in filled
            if from_stream and (
                not stream_connected
                or age is None
                or age > SNAPSHOT_STALE_SECONDS
            ):
                stale.append(sym)
        result = {
            "symbols": out,
            "missing": missing,
            "missing_fields": missing_fields,
            "source": source,
            "stream_connected": stream_connected,
            "age_seconds": age_seconds,
            "as_of": datetime.now(UTC).isoformat(),
        }
        if stale:
            result["stale"] = sorted(stale)
        return result

    # ---- windows ------------------------------------------------------------------------

    @mcp.tool(
        description=(
            "Recent trades for one symbol from the rolling tick store "
            "(retention-bounded, default 4h). Rows ascend by time: "
            "[ts_us, price, size, exchange]; ts_us = epoch MICROSECONDS (UTC). "
            "With more rows than `limit`, the MOST RECENT rows are returned."
        )
    )
    async def get_trades_window(
        symbol: str, minutes: int = 5, limit: int = 200
    ) -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        _, market_store = parts
        if minutes > MAX_WINDOW_MINUTES:
            return _limit_exceeded(MAX_WINDOW_MINUTES, minutes)
        if limit > MAX_WINDOW_ROWS:
            return _limit_exceeded(MAX_WINDOW_ROWS, limit)
        symbol = symbol.strip().upper()
        rows = await market_store.trades_window(
            symbol,
            since_us=_minutes_ago_us(max(1, minutes)),
            limit=max(1, limit),
        )
        return {
            "symbol": symbol,
            "count": len(rows),
            "columns": ["ts_us", "price", "size", "exchange"],
            "trades": [[r["ts_us"], r["price"], r["size"], r["exchange"]] for r in rows],
        }

    @mcp.tool(
        description=(
            "Recent NBBO quotes for one symbol. Rows ascend by time: "
            "[ts_us, bid_price, bid_size, ask_price, ask_size]; ts_us = epoch "
            "MICROSECONDS (UTC). With more rows than `limit`, the MOST RECENT "
            "rows are returned."
        )
    )
    async def get_quotes_window(
        symbol: str, minutes: int = 5, limit: int = 200
    ) -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        _, market_store = parts
        if minutes > MAX_WINDOW_MINUTES:
            return _limit_exceeded(MAX_WINDOW_MINUTES, minutes)
        if limit > MAX_WINDOW_ROWS:
            return _limit_exceeded(MAX_WINDOW_ROWS, limit)
        symbol = symbol.strip().upper()
        rows = await market_store.quotes_window(
            symbol,
            since_us=_minutes_ago_us(max(1, minutes)),
            limit=max(1, limit),
        )
        return {
            "symbol": symbol,
            "count": len(rows),
            "columns": ["ts_us", "bid_price", "bid_size", "ask_price", "ask_size"],
            "quotes": [
                [r["ts_us"], r["bid_price"], r["bid_size"], r["ask_price"], r["ask_size"]]
                for r in rows
            ],
        }

    # ---- bars -----------------------------------------------------------------------------

    @mcp.tool(
        description=(
            "OHLCV bars for one symbol from the local store. timeframe: "
            "1min|5min|15min|1hour|1day (5min/15min/1hour are aggregated "
            "server-side from 1min bars). "
            "start/end are ISO-8601. Rows ascend by time: "
            "[ts, open, high, low, close, volume, trade_count, vwap]; "
            "`ts` is the bar start in epoch SECONDS (UTC). With more rows "
            "than `limit`, the MOST RECENT rows are returned."
        )
    )
    async def get_stock_bars(
        symbol: str,
        timeframe: str = "1min",
        start: str | None = None,
        end: str | None = None,
        limit: int = 390,
    ) -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        _, market_store = parts
        if timeframe not in BAR_TIMEFRAMES:
            return _err(
                "invalid_timeframe",
                f"use one of: {', '.join(BAR_TIMEFRAMES)}",
                valid=list(BAR_TIMEFRAMES),
            )
        if limit > MAX_BARS:
            return _limit_exceeded(MAX_BARS, limit)
        start_ts = _iso_to_epoch_s(start)
        if start and start_ts is None:
            return _invalid_date("start", start)
        end_ts = _iso_to_epoch_s(end)
        if end and end_ts is None:
            return _invalid_date("end", end)
        symbol = symbol.strip().upper()
        if timeframe in AGGREGATE_TIMEFRAMES:
            rows = await market_store.bars_window_aggregated(
                symbol,
                bucket_seconds=AGGREGATE_TIMEFRAMES[timeframe],
                start_ts=start_ts,
                end_ts=end_ts,
                limit=max(1, limit),
            )
        else:
            rows = await market_store.bars_window(
                symbol,
                timeframe=timeframe,
                start_ts=start_ts,
                end_ts=end_ts,
                limit=max(1, limit),
            )
        return {
            "symbol": symbol,
            "timeframe": timeframe,
            "count": len(rows),
            "columns": ["ts", "open", "high", "low", "close", "volume", "trade_count", "vwap"],
            "bars": [
                [r["ts"], r["open"], r["high"], r["low"], r["close"],
                 r["volume"], r["trade_count"], r["vwap"]]
                for r in rows
            ],
        }

    @mcp.tool(
        description=(
            "Delta poll: bars newer than `cursor` (monotonic; updated bars "
            "re-surface). Compact rows: [symbol, timeframe, ts, o, h, l, c, v, n, vw, seq]; "
            "`ts` is the bar start in epoch SECONDS (UTC). Pass next_cursor "
            "back on the next call. cursor=-1 starts from the tail (only "
            "future bars); latest_cursor is always included; gap=true means "
            "bars were pruned past your cursor."
        )
    )
    async def get_bars_since(
        cursor: int = 0,
        symbols: list[str] | None = None,
        timeframe: str | None = None,
        limit: int = 500,
    ) -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        _, market_store = parts
        if limit > MAX_BARS_CURSOR:
            return _limit_exceeded(MAX_BARS_CURSOR, limit)
        # One SQL placeholder per symbol — an unbounded list could exceed
        # SQLite's variable limit and raise instead of erroring structurally.
        if symbols and len(symbols) > MAX_LATEST_SYMBOLS:
            return _limit_exceeded(MAX_LATEST_SYMBOLS, len(symbols))
        if timeframe is not None and timeframe not in VALID_TIMEFRAMES:
            return _err("invalid_timeframe", f"use one of: {', '.join(VALID_TIMEFRAMES)}", valid=list(VALID_TIMEFRAMES))
        latest = market_store.latest_bar_cursor
        if cursor == -1:
            return {
                "count": 0,
                "bars": [],
                "next_cursor": latest,
                "latest_cursor": latest,
                "has_more": False,
                "note": "started_from_tail",
            }
        cursor = max(0, cursor)
        if cursor > latest:
            return _cursor_out_of_range(cursor, latest)
        rows, next_cursor, has_more = await market_store.bars_since(
            cursor=cursor,
            limit=max(1, limit),
            symbols=symbols,
            timeframe=timeframe,
        )
        out: dict[str, Any] = {
            "count": len(rows),
            "columns": [
                "symbol", "timeframe", "ts", "open", "high", "low", "close",
                "volume", "trade_count", "vwap", "seq",
            ],
            "bars": [
                [r["symbol"], r["timeframe"], r["ts"], r["open"], r["high"], r["low"],
                 r["close"], r["volume"], r["trade_count"], r["vwap"], r["seq"]]
                for r in rows
            ],
            "next_cursor": next_cursor,
            "latest_cursor": latest,
            "has_more": has_more,
        }
        if cursor:
            min_seq = await market_store.min_bar_seq()
            if min_seq and cursor < min_seq - 1:
                out["gap"] = True
                out["oldest_available_cursor"] = min_seq - 1
        return out

    # ---- halts ------------------------------------------------------------------------------

    @mcp.tool(
        description=(
            "Trading-status changes (halts/resumes) and LULD band updates over "
            "the past `minutes` (retained for STATUS_RETENTION_DAYS, default "
            "30d), newest first. Timestamps (ts_us) are epoch microseconds UTC."
        )
    )
    async def get_trading_halts(
        minutes: int = 240,
        symbols: list[str] | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        _, market_store = parts
        if limit > MAX_STATUSES:
            return _limit_exceeded(MAX_STATUSES, limit)
        # Symbols feed per-symbol SQL placeholders in two queries — reject
        # oversized lists structurally like the other symbol-filtered tools.
        if symbols and len(symbols) > MAX_LATEST_SYMBOLS:
            return _limit_exceeded(MAX_LATEST_SYMBOLS, len(symbols))
        # Clamp to what retention actually keeps and say so — a window the
        # store cannot serve must not silently look empty-but-complete.
        retention_minutes = app.config.status_retention_days * 1440
        clamped = minutes > retention_minutes
        minutes = min(max(1, minutes), retention_minutes)
        statuses = await market_store.recent_statuses(
            minutes=minutes, symbols=symbols, limit=max(1, limit)
        )
        lulds = await market_store.recent_lulds(
            minutes=minutes, symbols=symbols, limit=max(1, limit)
        )
        out: dict[str, Any] = {
            "window_minutes": minutes,
            "statuses": statuses,
            "lulds": lulds,
        }
        if clamped:
            out["clamped_to_minutes"] = minutes
        return out

    # ---- derived analytics (local SQLite + snapshot cache) -------------------

    @mcp.tool(
        description=(
            "Server-computed trading context per symbol (~15 scalars from "
            "locally stored bars + the live snapshot cache): last price, "
            "change/gap vs previous close, session VWAP distance, RSI-14 / "
            "EMA-9 / EMA-21 / ATR-14 (1min bars), day range position, "
            "volume vs 10-day average (time-adjusted), and halt flag. "
            "Much cheaper than reading raw bar arrays. Requires the symbol "
            "to be locally ingested (watchlist)."
        )
    )
    async def get_symbol_context(symbols: list[str]) -> dict[str, Any]:
        from . import market_analytics as ma

        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        _, market_store = parts
        if not symbols:
            return _err("no_symbols", "pass at least one ticker symbol")
        if len(symbols) > MAX_CONTEXT_SYMBOLS:
            return _limit_exceeded(MAX_CONTEXT_SYMBOLS, len(symbols))
        now = datetime.now(UTC)
        now_ts = int(now.timestamp())
        today_midnight = now_ts - (now_ts % 86_400)
        out: dict[str, Any] = {}
        for raw_sym in symbols:
            sym = raw_sym.strip().upper()
            minute_bars = await market_store.bars_window(
                sym, timeframe="1min", start_ts=now_ts - 8 * 3600, limit=MAX_BARS
            )
            daily_bars = await market_store.bars_window(
                sym, timeframe="1day", limit=15
            )
            snap = market_store.snapshots.get(sym) or {}
            trade = snap.get("trade") or {}
            last = trade.get("p") if isinstance(trade.get("p"), (int, float)) else None
            if last is None and minute_bars:
                last = minute_bars[-1]["close"]
            if not minute_bars and last is None:
                out[sym] = _err(
                    "no_local_data",
                    "no locally stored bars — add the symbol with "
                    "set_stock_watchlist or use get_stock_snapshots (REST)",
                )
                continue

            # Previous close + prior daily volumes (excluding today's daily).
            prev_close = None
            prior_volumes: list[float] = []
            if daily_bars:
                if daily_bars[-1]["ts"] >= today_midnight:
                    prior = daily_bars[:-1]
                else:
                    prior = daily_bars
                if prior:
                    prev_close = prior[-1]["close"]
                prior_volumes = [
                    float(b["volume"]) for b in prior if b.get("volume")
                ]

            highs = [b["high"] for b in minute_bars if b.get("high") is not None]
            lows = [b["low"] for b in minute_bars if b.get("low") is not None]
            day_high = max(highs) if highs else None
            day_low = min(lows) if lows else None
            volume_today = float(
                sum(b["volume"] or 0 for b in minute_bars)
            )
            today_open = minute_bars[0]["open"] if minute_bars else None
            vwap = ma.session_vwap(minute_bars)
            status = snap.get("status") or {}
            entry: dict[str, Any] = {
                "last": last,
                "prev_close": prev_close,
                "change_pct": ma.pct_change(last, prev_close),
                "gap_pct": ma.gap_pct(today_open, prev_close),
                "session_vwap": vwap,
                "vwap_distance_pct": ma.pct_change(last, vwap),
                "rsi_14": ma.rsi(minute_bars),
                "ema_9": ma.ema(minute_bars, 9),
                "ema_21": ma.ema(minute_bars, 21),
                "atr_14": ma.atr(minute_bars),
                "day_high": day_high,
                "day_low": day_low,
                "range_position": ma.day_range_position(last, day_high, day_low),
                "volume_today": volume_today or None,
                "rel_volume": ma.relative_volume(
                    volume_today, prior_volumes, len(minute_bars) / 390.0
                ),
                "minute_bars_used": len(minute_bars),
                "halted": status.get("sc") == "H" if status else False,
            }
            out[sym] = entry
        return {"symbols": out, "as_of": now.isoformat()}

    @mcp.tool(
        description=(
            "Bucketed tape/microstructure digest for one symbol from the "
            "rolling tick store: per-bucket trade count (tape speed), volume, "
            "VWAP, uptick/downtick counts, block-trade count, price range, "
            "average spread (bps), and bid-size fraction (imbalance proxy). "
            "Rows: [ts_us, trades, volume, vwap, upticks, downticks, "
            "block_trades, low, high, quotes, spread_bps, bid_fraction]; "
            "ts_us = bucket start, epoch MICROSECONDS (UTC)."
        )
    )
    async def get_tape_stats(
        symbol: str,
        minutes: int = 30,
        bucket_seconds: int = 60,
        block_size: int = 1000,
    ) -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        _, market_store = parts
        if minutes > MAX_WINDOW_MINUTES:
            return _limit_exceeded(MAX_WINDOW_MINUTES, minutes)
        minutes = max(1, minutes)
        bucket_seconds = max(10, min(bucket_seconds, 3600))
        if minutes * 60 // bucket_seconds > MAX_TAPE_BUCKETS:
            return _err(
                "too_many_buckets",
                f"minutes*60/bucket_seconds must be <= {MAX_TAPE_BUCKETS}; "
                "use a larger bucket_seconds or a smaller window",
                max_buckets=MAX_TAPE_BUCKETS,
            )
        symbol = symbol.strip().upper()
        since_us = _minutes_ago_us(minutes)
        bucket_us = bucket_seconds * 1_000_000
        trade_rows = await market_store.tape_stats(
            symbol, since_us=since_us, bucket_us=bucket_us,
            block_size=max(1, block_size),
        )
        quote_rows = await market_store.quote_stats(
            symbol, since_us=since_us, bucket_us=bucket_us
        )
        quotes_by_bucket = {r["bucket_us"]: r for r in quote_rows}
        buckets = sorted(
            set(quotes_by_bucket) | {r["bucket_us"] for r in trade_rows}
        )
        trades_by_bucket = {r["bucket_us"]: r for r in trade_rows}
        rows = []
        for b in buckets:
            t = trades_by_bucket.get(b, {})
            q = quotes_by_bucket.get(b, {})
            rows.append([
                b,
                t.get("trades", 0),
                t.get("volume", 0),
                round(t["vwap"], 4) if t.get("vwap") is not None else None,
                t.get("upticks", 0),
                t.get("downticks", 0),
                t.get("block_trades", 0),
                t.get("low"),
                t.get("high"),
                q.get("quotes", 0),
                round(q["spread_bps"], 2) if q.get("spread_bps") is not None else None,
                round(q["bid_fraction"], 3) if q.get("bid_fraction") is not None else None,
            ])
        return {
            "symbol": symbol,
            "bucket_seconds": bucket_seconds,
            "block_size": block_size,
            "columns": [
                "ts_us", "trades", "volume", "vwap", "upticks", "downticks",
                "block_trades", "low", "high", "quotes", "spread_bps",
                "bid_fraction",
            ],
            "buckets": rows,
            "count": len(rows),
        }

    @mcp.tool(
        description=(
            "One aggregate row per watchlist symbol over the past `minutes`: "
            "last price, change %, volume, trade count, average spread (bps), "
            "halted flag. 'What is my watchlist doing right now' in one "
            "bounded call. Rows: [symbol, last, chg_pct, volume, trades, "
            "spread_bps, halted]."
        )
    )
    async def get_watchlist_pulse(
        minutes: int = 5, symbols: list[str] | None = None
    ) -> dict[str, Any]:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return _stream_disabled()
        stream, market_store = parts
        if minutes > MAX_WINDOW_MINUTES:
            return _limit_exceeded(MAX_WINDOW_MINUTES, minutes)
        wanted = symbols or stream.watchlist()
        if len(wanted) > MAX_LATEST_SYMBOLS:
            return _limit_exceeded(MAX_LATEST_SYMBOLS, len(wanted))
        pulse = await market_store.watchlist_pulse(
            wanted, since_us=_minutes_ago_us(max(1, minutes))
        )
        rows = []
        for sym in sorted(pulse):
            entry = pulse[sym]
            first = entry.get("first_price")
            last = entry.get("last_price")
            chg = None
            if first and last is not None:
                chg = round((last - first) / first * 100.0, 2)
            status = (market_store.snapshots.get(sym) or {}).get("status") or {}
            rows.append([
                sym, last, chg, entry.get("volume"), entry.get("trades"),
                entry.get("spread_bps"), status.get("sc") == "H",
            ])
        return {
            "window_minutes": minutes,
            "columns": [
                "symbol", "last", "chg_pct", "volume", "trades", "spread_bps",
                "halted",
            ],
            "rows": rows,
            "count": len(rows),
        }

    @mcp.tool(
        description=(
            "Discovery: market movers + most-actives fused with what this "
            "server already knows — local news count, halt status, and "
            "watchlist membership per candidate. Use it to find symbols "
            "worth adding to the watchlist."
        )
    )
    async def get_market_pulse(
        top: int = 10, news_minutes: int = 120
    ) -> dict[str, Any]:
        app = get_app_state()
        if app.market_client is None:
            return _client_unavailable()
        if top > MAX_PULSE_TOP:
            return _limit_exceeded(MAX_PULSE_TOP, top)
        top = max(1, top)
        movers = await app.market_client.movers(top=top)
        actives = await app.market_client.most_actives(by="volume", top=top)
        if movers is None and actives is None:
            return _upstream_error()
        candidates: dict[str, dict[str, Any]] = {}

        def _add(entry: dict[str, Any], source: str) -> None:
            sym = str(entry.get("symbol") or "").upper()
            if not sym:
                return
            existing = candidates.setdefault(sym, {"symbol": sym, "sources": []})
            existing["sources"].append(source)
            if "price" in entry:
                existing["price"] = entry.get("price")
            if "percent_change" in entry:
                existing["percent_change"] = entry.get("percent_change")
            if "volume" in entry:
                existing["volume"] = entry.get("volume")

        for g in (movers or {}).get("gainers") or []:
            _add(g, "gainer")
        for loser in (movers or {}).get("losers") or []:
            _add(loser, "loser")
        for a in (actives or {}).get("most_actives") or []:
            _add(a, "most_active")

        news_counts = await app.store.symbol_map(
            minutes=max(1, news_minutes), min_articles=1, limit=500
        )
        watch = set(app.stock_stream.watchlist()) if app.stock_stream else set()
        halted: set[str] = set()
        if app.market_store is not None and candidates:
            statuses = await app.market_store.recent_statuses(
                minutes=240, symbols=list(candidates)[:50], limit=200
            )
            latest_status: dict[str, str] = {}
            for st in statuses:  # newest first
                latest_status.setdefault(st["symbol"], st.get("status_code") or "")
            halted = {s for s, code in latest_status.items() if code == "H"}

        rows = []
        for sym, entry in candidates.items():
            rows.append({
                **entry,
                "news_count": news_counts.get(sym, 0),
                "halted": sym in halted,
                "on_watchlist": sym in watch,
            })
        rows.sort(
            key=lambda r: (
                abs(r.get("percent_change") or 0.0),
                r.get("volume") or 0,
            ),
            reverse=True,
        )
        return {
            "candidates": rows,
            "count": len(rows),
            "news_window_minutes": news_minutes,
            "as_of": datetime.now(UTC).isoformat(),
        }

    # ---- REST market context (works even with the stock stream disabled) ---------------

    @mcp.tool(
        description=(
            "Top market gainers and losers vs previous close (Alpaca screener, "
            "real-time SIP). A live 'what's moving today' view."
        )
    )
    async def get_market_movers(top: int = 10) -> dict[str, Any]:
        app = get_app_state()
        if app.market_client is None:
            return _client_unavailable()
        if top > MAX_MOVERS_TOP:
            return _limit_exceeded(MAX_MOVERS_TOP, top)
        payload = await app.market_client.movers(top=max(1, top))
        if payload is None:
            return _upstream_error()
        return {
            "gainers": payload.get("gainers") or [],
            "losers": payload.get("losers") or [],
            "market_type": payload.get("market_type"),
            "last_updated": payload.get("last_updated"),
        }

    @mcp.tool(
        description=(
            "Most active stocks by volume or trade count (Alpaca screener). "
            "by: volume|trades."
        )
    )
    async def get_most_active_stocks(
        by: str = "volume", top: int = 10
    ) -> dict[str, Any]:
        app = get_app_state()
        if app.market_client is None:
            return _client_unavailable()
        if by not in ("volume", "trades"):
            return _err("invalid_by", "use volume or trades", valid=["volume", "trades"])
        if top > MAX_ACTIVES_TOP:
            return _limit_exceeded(MAX_ACTIVES_TOP, top)
        payload = await app.market_client.most_actives(by=by, top=max(1, top))
        if payload is None:
            return _upstream_error()
        return {
            "by": by,
            "most_actives": payload.get("most_actives") or [],
            "last_updated": payload.get("last_updated"),
        }

    @mcp.tool(
        description=(
            "Full snapshots (latest trade, latest quote, minute bar, daily bar, "
            "previous daily bar) per symbol via REST — works for any symbol, "
            "watchlisted or not. One call = complete per-symbol context."
        )
    )
    async def get_stock_snapshots(symbols: list[str]) -> dict[str, Any]:
        app = get_app_state()
        if app.market_client is None:
            return _client_unavailable()
        if not symbols:
            return _err("no_symbols", "pass at least one ticker symbol")
        if len(symbols) > MAX_SNAPSHOT_SYMBOLS:
            return _limit_exceeded(MAX_SNAPSHOT_SYMBOLS, len(symbols))
        payload = await app.market_client.snapshots(symbols)
        if payload is None:
            return _upstream_error()
        out: dict[str, Any] = {}
        for sym, snap in payload.items():
            if not isinstance(snap, dict):
                continue
            out[sym.upper()] = {
                "latest_trade": snap.get("latestTrade"),
                "latest_quote": snap.get("latestQuote"),
                "minute_bar": snap.get("minuteBar"),
                "daily_bar": snap.get("dailyBar"),
                "prev_daily_bar": snap.get("prevDailyBar"),
            }
        return {"snapshots": out, "as_of": datetime.now(UTC).isoformat()}

    @mcp.tool(
        description=(
            "Market clock: is the market open now, and the next open/close "
            "timestamps. Cached ~15s."
        )
    )
    async def get_market_clock() -> dict[str, Any]:
        app = get_app_state()
        if app.market_client is None:
            return _client_unavailable()
        payload = await app.market_client.clock()
        if payload is None:
            return _upstream_error()
        return {
            "timestamp": payload.get("timestamp"),
            "is_open": payload.get("is_open"),
            "next_open": payload.get("next_open"),
            "next_close": payload.get("next_close"),
        }

    @mcp.tool(
        description=(
            "Market calendar (trading days with open/close, incl. early closes). "
            "start/end: YYYY-MM-DD, range <= 90 days; defaults to the next 14 days."
        )
    )
    async def get_market_calendar(
        start: str | None = None, end: str | None = None
    ) -> dict[str, Any]:
        app = get_app_state()
        if app.market_client is None:
            return _client_unavailable()
        start_dt = _parse_date(start)
        if start and start_dt is None:
            return _invalid_date("start", start)
        end_dt = _parse_date(end)
        if end and end_dt is None:
            return _invalid_date("end", end)
        start_dt = start_dt or datetime.now(UTC)
        end_dt = end_dt or (start_dt + timedelta(days=14))
        if end_dt < start_dt:
            return _err("invalid_range", "end must not be before start", reason="end before start")
        if (end_dt - start_dt).days > MAX_CALENDAR_DAYS:
            return _limit_exceeded(MAX_CALENDAR_DAYS, (end_dt - start_dt).days)
        days = await app.market_client.calendar(
            start=start_dt.date().isoformat(), end=end_dt.date().isoformat()
        )
        if days is None:
            return _upstream_error()
        return {
            "start": start_dt.date().isoformat(),
            "end": end_dt.date().isoformat(),
            "days": days,
        }

    @mcp.tool(
        description=(
            "Corporate actions (splits, dividends, mergers, spinoffs...) by "
            "symbol/type/date. start/end: YYYY-MM-DD, range <= 90 days; "
            "defaults to -7d..+30d. Use to avoid surprises like splits on held "
            "symbols."
        )
    )
    async def get_corporate_actions(
        symbols: list[str] | None = None,
        types: list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        app = get_app_state()
        if app.market_client is None:
            return _client_unavailable()
        if limit > MAX_CORPORATE_ACTIONS:
            return _limit_exceeded(MAX_CORPORATE_ACTIONS, limit)
        if symbols and len(symbols) > MAX_SNAPSHOT_SYMBOLS:
            return _limit_exceeded(MAX_SNAPSHOT_SYMBOLS, len(symbols))
        now = datetime.now(UTC)
        start_dt = _parse_date(start)
        if start and start_dt is None:
            return _invalid_date("start", start)
        end_dt = _parse_date(end)
        if end and end_dt is None:
            return _invalid_date("end", end)
        start_dt = start_dt or (now - timedelta(days=7))
        end_dt = end_dt or (now + timedelta(days=30))
        if end_dt < start_dt:
            return _err("invalid_range", "end must not be before start", reason="end before start")
        if (end_dt - start_dt).days > CORPORATE_ACTIONS_MAX_RANGE_DAYS:
            return _limit_exceeded(
                CORPORATE_ACTIONS_MAX_RANGE_DAYS, (end_dt - start_dt).days
            )
        payload = await app.market_client.corporate_actions(
            symbols=symbols,
            types=types,
            start=start_dt.date().isoformat(),
            end=end_dt.date().isoformat(),
            limit=max(1, limit),
        )
        if payload is None:
            return _upstream_error()
        return {
            "start": start_dt.date().isoformat(),
            "end": end_dt.date().isoformat(),
            "corporate_actions": payload.get("corporate_actions") or {},
            "has_more": bool(payload.get("next_page_token")),
        }


def register_resources(mcp: FastMCP) -> None:
    import json

    @mcp.resource("alpaca-market://watchlist", mime_type="application/json")
    async def watchlist_resource() -> str:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return json.dumps(_stream_disabled())
        stream, _ = parts
        return json.dumps(
            {
                "watchlist": stream.watchlist(),
                "channels": stream.channels(),
                "acknowledged": stream.acknowledged_subscription(),
            },
            default=str,
        )

    @mcp.resource("alpaca-market://latest/{symbol}", mime_type="application/json")
    async def latest_resource(symbol: str) -> str:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return json.dumps(_stream_disabled())
        _, market_store = parts
        snap = market_store.snapshots.get(symbol)
        if snap is None:
            return json.dumps({"error": "not_in_stream_cache", "symbol": symbol.upper()})
        return json.dumps({"symbol": symbol.upper(), **snap}, default=str)

    @mcp.resource("alpaca-market://clock", mime_type="application/json")
    async def clock_resource() -> str:
        app = get_app_state()
        if app.market_client is None:
            return json.dumps(_client_unavailable())
        payload = await app.market_client.clock()
        if payload is None:
            return json.dumps(_upstream_error())
        return json.dumps(payload, default=str)

    @mcp.resource("alpaca-market://halts", mime_type="application/json")
    async def halts_resource() -> str:
        app = get_app_state()
        parts = _market_parts(app)
        if parts is None:
            return json.dumps(_stream_disabled())
        _, market_store = parts
        statuses = await market_store.recent_statuses(minutes=240, limit=100)
        return json.dumps({"window_minutes": 240, "statuses": statuses}, default=str)


__all__ = ["register", "register_resources"]

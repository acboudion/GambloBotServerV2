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

MAX_LATEST_SYMBOLS = 50
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
VALID_TIMEFRAMES = ("1min", "1day")

# Snapshot-cache slot names per include key.
_INCLUDE_TO_SLOT = {
    "trade": "trade",
    "quote": "quote",
    "bar": "minute_bar",
    "daily_bar": "daily_bar",
    "status": "status",
    "luld": "luld",
}


def _limit_exceeded(max_allowed: int, requested: int) -> dict[str, Any]:
    return {
        "error": "limit_exceeded",
        "max_allowed": max_allowed,
        "requested": requested,
    }


def _stream_disabled() -> dict[str, Any]:
    return {"error": "stock_stream_disabled"}


def _client_unavailable() -> dict[str, Any]:
    return {"error": "market_client_unavailable"}


def _upstream_error() -> dict[str, Any]:
    return {"error": "alpaca_request_failed"}


def _parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = dateparser.isoparse(value)
    except (ValueError, TypeError):
        return None
    return dt.replace(tzinfo=dt.tzinfo or UTC)


def _invalid_date(field: str, value: str) -> dict[str, Any]:
    """A caller-provided date that failed to parse must be a structured error
    — silently falling back to a default window would return valid-looking
    data for the wrong dates."""
    return {"error": "invalid_date", "field": field, "value": value}


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
            "with set_stock_watchlist or use get_stock_snapshots (REST)."
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
            return {"error": "no_symbols"}
        if len(symbols) > MAX_LATEST_SYMBOLS:
            return _limit_exceeded(MAX_LATEST_SYMBOLS, len(symbols))
        include = include or ["trade", "quote", "bar"]
        invalid = [i for i in include if i not in VALID_INCLUDE]
        if invalid:
            return {"error": "invalid_include", "invalid": invalid, "valid": list(VALID_INCLUDE)}
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
        if missing and app.market_client is not None and wanted_rest:
            filled: set[str] = set()
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
        return {
            "symbols": out,
            "missing": missing,
            "missing_fields": missing_fields,
            "source": source,
            "as_of": datetime.now(UTC).isoformat(),
        }

    # ---- windows ------------------------------------------------------------------------

    @mcp.tool(
        description=(
            "Recent trades for one symbol from the rolling tick store "
            "(retention-bounded). Rows ascend by time: [ts_us, price, size, exchange]."
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
        rows = await market_store.trades_window(
            symbol,
            since_us=_minutes_ago_us(max(1, minutes)),
            limit=max(1, limit),
        )
        return {
            "symbol": symbol.upper(),
            "count": len(rows),
            "columns": ["ts_us", "price", "size", "exchange"],
            "trades": [[r["ts_us"], r["price"], r["size"], r["exchange"]] for r in rows],
        }

    @mcp.tool(
        description=(
            "Recent NBBO quotes for one symbol. Rows ascend by time: "
            "[ts_us, bid_price, bid_size, ask_price, ask_size]."
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
        rows = await market_store.quotes_window(
            symbol,
            since_us=_minutes_ago_us(max(1, minutes)),
            limit=max(1, limit),
        )
        return {
            "symbol": symbol.upper(),
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
            "OHLCV bars for one symbol from the local store. timeframe: 1min|1day. "
            "start/end are ISO-8601. Rows ascend by time: "
            "[ts, open, high, low, close, volume, trade_count, vwap]."
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
        if timeframe not in VALID_TIMEFRAMES:
            return {"error": "invalid_timeframe", "valid": list(VALID_TIMEFRAMES)}
        if limit > MAX_BARS:
            return _limit_exceeded(MAX_BARS, limit)
        start_ts = _iso_to_epoch_s(start)
        if start and start_ts is None:
            return _invalid_date("start", start)
        end_ts = _iso_to_epoch_s(end)
        if end and end_ts is None:
            return _invalid_date("end", end)
        rows = await market_store.bars_window(
            symbol,
            timeframe=timeframe,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=max(1, limit),
        )
        return {
            "symbol": symbol.upper(),
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
            "re-surface). Compact rows: [symbol, timeframe, ts, o, h, l, c, v, n, vw, seq]. "
            "Pass next_cursor back on the next call."
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
        if timeframe is not None and timeframe not in VALID_TIMEFRAMES:
            return {"error": "invalid_timeframe", "valid": list(VALID_TIMEFRAMES)}
        rows, next_cursor, has_more = await market_store.bars_since(
            cursor=max(0, cursor),
            limit=max(1, limit),
            symbols=symbols,
            timeframe=timeframe,
        )
        return {
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
            "has_more": has_more,
        }

    # ---- halts ------------------------------------------------------------------------------

    @mcp.tool(
        description=(
            "Trading-status changes (halts/resumes) and LULD band updates over "
            "the past `minutes`, newest first."
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
        statuses = await market_store.recent_statuses(
            minutes=max(1, minutes), symbols=symbols, limit=max(1, limit)
        )
        lulds = await market_store.recent_lulds(
            minutes=max(1, minutes), symbols=symbols, limit=max(1, limit)
        )
        return {
            "window_minutes": minutes,
            "statuses": statuses,
            "lulds": lulds,
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
            return {"error": "invalid_by", "valid": ["volume", "trades"]}
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
            return {"error": "no_symbols"}
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
            return {"error": "invalid_range", "reason": "end before start"}
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
            return {"error": "invalid_range", "reason": "end before start"}
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

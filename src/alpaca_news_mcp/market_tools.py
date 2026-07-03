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
        return {
            "symbols": out,
            "missing": missing,
            "source": "stream",
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
        rows = await market_store.bars_window(
            symbol,
            timeframe=timeframe,
            start_ts=_iso_to_epoch_s(start),
            end_ts=_iso_to_epoch_s(end),
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

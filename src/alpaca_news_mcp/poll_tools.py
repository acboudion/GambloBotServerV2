"""Composite polling surface for the trading-loop harness.

One `poll_market` call replaces the 4-6 separate tool calls a bot loop tick
previously cost (news delta + alert delta + bar delta + latest data + clock):
every section is bounded, sections fail independently (a cursor error in one
never fails the whole call), and a loop-context block tells the bot what
phase the market is in, when to poll next, and whether the data can be
trusted right now.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from dateutil import parser as dateparser
from mcp.server.fastmcp import FastMCP

from .app_state import AppState, get_app_state
from .tool_common import (
    SEVERITIES,
    apply_gap_info,
    cursor_out_of_range,
    err,
    filter_over_cap,
    invalid_severity,
    limit_exceeded,
    severities_at_or_above,
    stream_disabled,
    tail_response,
)
from .tools import MAX_SYMBOLS_PER_LOOKUP, _serialize_article

MAX_POLL_NEWS = 200
MAX_POLL_ALERTS = 200
MAX_POLL_BARS = 500
MAX_SNAPSHOT_SYMBOLS = 50
VALID_SECTIONS = ("news", "alerts", "bars", "snapshot", "context")

BAR_COLUMNS = [
    "symbol", "timeframe", "ts", "open", "high", "low", "close",
    "volume", "trade_count", "vwap", "seq",
]

try:
    _EASTERN: ZoneInfo | None = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover - tzdata missing
    _EASTERN = None


def _parse_iso(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        dt = dateparser.isoparse(value)
    except (ValueError, TypeError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


async def _news_section(
    app: AppState, cursor: int, limit: int, symbols: list[str] | None
) -> dict[str, Any]:
    latest = app.store.latest_article_cursor
    if cursor == -1:
        return tail_response("articles", latest)
    cursor = max(0, cursor)
    if cursor > latest:
        return cursor_out_of_range(cursor, latest)
    articles, next_cursor, has_more = await app.store.articles_since(
        cursor=cursor, limit=limit, symbols=symbols
    )
    serialized = []
    for a in articles:
        d = _serialize_article(a, fields="compact")
        d["seq"] = a.seq
        serialized.append(d)
    out: dict[str, Any] = {
        "count": len(serialized),
        "articles": serialized,
        "next_cursor": next_cursor,
        "latest_cursor": latest,
        "has_more": has_more,
    }
    if cursor:
        apply_gap_info(
            out, cursor=cursor, latest=latest,
            min_seq=await app.store.min_article_seq(),
        )
    return out


async def _alerts_section(
    app: AppState, cursor: int, limit: int, min_severity: str | None
) -> dict[str, Any]:
    latest = app.store.latest_alert_cursor
    if cursor == -1:
        return tail_response("alerts", latest)
    cursor = max(0, cursor)
    if cursor > latest:
        return cursor_out_of_range(cursor, latest)
    severities = (
        severities_at_or_above(min_severity) if min_severity is not None else None
    )
    alerts, next_cursor, has_more = await app.store.alerts_since(
        cursor=cursor, limit=limit, severities=severities
    )
    out: dict[str, Any] = {
        "count": len(alerts),
        "alerts": alerts,
        "next_cursor": next_cursor,
        "latest_cursor": latest,
        "has_more": has_more,
    }
    if cursor:
        apply_gap_info(
            out, cursor=cursor, latest=latest,
            min_seq=await app.store.min_alert_seq(),
        )
    return out


async def _bars_section(
    app: AppState, cursor: int, limit: int, symbols: list[str] | None
) -> dict[str, Any]:
    if app.market_store is None or app.stock_stream is None:
        return stream_disabled()
    latest = app.market_store.latest_bar_cursor
    if cursor == -1:
        return tail_response("bars", latest)
    cursor = max(0, cursor)
    if cursor > latest:
        return cursor_out_of_range(cursor, latest)
    rows, next_cursor, has_more = await app.market_store.bars_since(
        cursor=cursor, limit=limit, symbols=symbols
    )
    out: dict[str, Any] = {
        "count": len(rows),
        "columns": BAR_COLUMNS,
        "bars": [[r[c] for c in BAR_COLUMNS] for r in rows],
        "next_cursor": next_cursor,
        "latest_cursor": latest,
        "has_more": has_more,
    }
    if cursor:
        apply_gap_info(
            out, cursor=cursor, latest=latest,
            min_seq=await app.market_store.min_bar_seq(),
        )
    return out


def _snapshot_section(app: AppState, symbols: list[str] | None) -> dict[str, Any]:
    if app.market_store is None or app.stock_stream is None:
        return stream_disabled()
    requested = [s.strip().upper() for s in (symbols or app.stock_stream.watchlist())]
    wanted = requested[:MAX_SNAPSHOT_SYMBOLS]
    now_us = int(datetime.now(UTC).timestamp() * 1_000_000)
    out: dict[str, Any] = {}
    for sym in wanted:
        snap = app.market_store.snapshots.get(sym)
        if not snap:
            continue
        entry: dict[str, Any] = {}
        for slot in ("trade", "quote", "minute_bar"):
            value = snap.get(slot)
            if value is not None:
                entry[slot] = value
        ages = [
            v["ts_us"]
            for v in entry.values()
            if isinstance(v, dict) and isinstance(v.get("ts_us"), int)
        ]
        if ages:
            entry["age_seconds"] = round(max(0.0, (now_us - max(ages)) / 1_000_000), 1)
        out[sym] = entry
    section: dict[str, Any] = {"symbols": out, "count": len(out)}
    if len(requested) > len(wanted):
        # Watchlists can exceed the snapshot cap (MAX_WATCHLIST_SYMBOLS is
        # configurable up to 500) — silent truncation would let a bot skip
        # staleness checks for the omitted names.
        section["truncated"] = True
        section["omitted"] = len(requested) - len(wanted)
        section["hint"] = (
            "snapshot covers the first "
            f"{MAX_SNAPSHOT_SYMBOLS} symbols; pass explicit `symbols` "
            "batches to cover the rest"
        )
    return section


def market_phase(
    clock: dict[str, Any] | None,
    now: datetime,
    trading_day: bool | None = None,
) -> str:
    """open | premarket | afterhours | closed | unknown. Extended-hours
    classification uses Eastern wall-clock time (4:00-9:30 pre, 16:00-20:00
    post) and only applies on days the market actually opens/opened —
    weekday holidays must not read as premarket/afterhours.

    trading_day: whether today's calendar has a session (None = unknown;
    the wall-clock heuristic then applies for afterhours, and premarket is
    still guarded by the clock's next_open date)."""
    if not clock:
        return "unknown"
    if clock.get("is_open"):
        return "open"
    if _EASTERN is None:
        return "closed"
    et = now.astimezone(_EASTERN)
    minutes = et.hour * 60 + et.minute
    if et.weekday() >= 5 or trading_day is False:
        return "closed"
    if 4 * 60 <= minutes < 9 * 60 + 30:
        # Premarket only exists if the market opens later TODAY — on a
        # holiday morning the clock's next_open points at a future day.
        next_open = _parse_iso(clock.get("next_open"))
        if next_open is not None and next_open.astimezone(_EASTERN).date() != et.date():
            return "closed"
        return "premarket"
    if 16 * 60 <= minutes < 20 * 60:
        return "afterhours"
    return "closed"


async def loop_context(app: AppState) -> dict[str, Any]:
    now = datetime.now(UTC)
    clock: dict[str, Any] | None = None
    if app.market_client is not None:
        clock = await app.market_client.clock()  # 15s TTL cache — no REST spam
    trading_day: bool | None = None
    if (
        clock is not None
        and not clock.get("is_open")
        and app.market_client is not None
        and _EASTERN is not None
    ):
        # Only consulted while closed, and the calendar is TTL-cached for
        # hours — distinguishes a weekday holiday (no extended-hours
        # sessions at all) from an ordinary evening/morning.
        et_today = now.astimezone(_EASTERN).date().isoformat()
        cal = await app.market_client.calendar(start=et_today, end=et_today)
        if cal is not None:
            trading_day = len(cal) > 0
    phase = market_phase(clock, now, trading_day)

    seconds_to_open: int | None = None
    seconds_to_close: int | None = None
    if clock:
        nxt_open = _parse_iso(clock.get("next_open"))
        nxt_close = _parse_iso(clock.get("next_close"))
        if nxt_open is not None:
            seconds_to_open = max(0, int((nxt_open - now).total_seconds()))
        if nxt_close is not None:
            seconds_to_close = max(0, int((nxt_close - now).total_seconds()))

    reasons: list[str] = []
    news_health = app.state.snapshot_health("news")
    if news_health.auth_failed:
        reasons.append("news_auth_failed")
    elif not news_health.connected:
        reasons.append("news_disconnected")
    if news_health.entitlement_error:
        reasons.append("news_entitlement_error")
    stocks_degraded = False
    if app.stock_stream is not None:
        stock_health = app.state.snapshot_health("stocks")
        if stock_health.auth_failed:
            reasons.append("stocks_auth_failed")
        elif not stock_health.connected:
            reasons.append("stocks_disconnected")
        if stock_health.entitlement_error:
            reasons.append("stocks_entitlement_error")
        stocks_degraded = any(r.startswith("stocks_") for r in reasons)

    if phase == "open":
        suggested = 30
    elif phase in ("premarket", "afterhours"):
        suggested = 120
    elif phase == "unknown":
        suggested = 300
    else:
        suggested = 600
    if reasons:
        # Data is degraded — polling faster only re-reads stale state.
        suggested = min(suggested * 2, 900)

    return {
        "as_of": now.isoformat(),
        "market_phase": phase,
        "is_open": bool(clock.get("is_open")) if clock else None,
        "seconds_to_open": seconds_to_open,
        "seconds_to_close": seconds_to_close,
        "suggested_next_poll_seconds": suggested,
        "degraded": {
            "news_stream": any(r.startswith("news_") for r in reasons),
            "stock_stream": stocks_degraded,
            "reasons": reasons,
        },
    }


def register(mcp: FastMCP) -> None:
    @mcp.tool(
        description=(
            "ONE call per trading-loop tick: combined delta poll for news, "
            "alerts, and bars (pass each next_cursor back verbatim), plus a "
            "watchlist snapshot block and a loop-context block (market_phase, "
            "seconds_to_close, suggested_next_poll_seconds, degraded data "
            "flags). Cursors: -1 = start from the tail (clean bootstrap), "
            "0 = full retained history. Sections fail independently — a "
            "cursor error in one section never fails the others. "
            "include: subset of news, alerts, bars, snapshot, context."
        )
    )
    async def poll_market(
        news_cursor: int = -1,
        alerts_cursor: int = -1,
        bars_cursor: int = -1,
        symbols: list[str] | None = None,
        include: list[str] | None = None,
        news_limit: int = 50,
        alerts_limit: int = 50,
        bars_limit: int = 200,
        min_severity: str | None = None,
    ) -> dict[str, Any]:
        include = include or list(VALID_SECTIONS)
        invalid = [i for i in include if i not in VALID_SECTIONS]
        if invalid:
            return err(
                "invalid_include",
                f"use any of: {', '.join(VALID_SECTIONS)}",
                invalid=invalid,
                valid=list(VALID_SECTIONS),
            )
        if (over := filter_over_cap(symbols, MAX_SYMBOLS_PER_LOOKUP)) is not None:
            return over
        if news_limit > MAX_POLL_NEWS:
            return limit_exceeded(MAX_POLL_NEWS, news_limit)
        if alerts_limit > MAX_POLL_ALERTS:
            return limit_exceeded(MAX_POLL_ALERTS, alerts_limit)
        if bars_limit > MAX_POLL_BARS:
            return limit_exceeded(MAX_POLL_BARS, bars_limit)
        if min_severity is not None and min_severity not in SEVERITIES:
            return invalid_severity(min_severity)
        app = get_app_state()
        out: dict[str, Any] = {}
        if "news" in include:
            out["news"] = await _news_section(
                app, news_cursor, max(1, news_limit), symbols
            )
        if "alerts" in include:
            out["alerts"] = await _alerts_section(
                app, alerts_cursor, max(1, alerts_limit), min_severity
            )
        if "bars" in include:
            out["bars"] = await _bars_section(
                app, bars_cursor, max(1, bars_limit), symbols
            )
        if "snapshot" in include:
            out["snapshot"] = _snapshot_section(app, symbols)
        if "context" in include:
            out["context"] = await loop_context(app)
        return out

    @mcp.tool(
        description=(
            "Call once per session: orientation for driving this server — "
            "the recommended polling loop, the cursor contract, timestamp "
            "units, error schema, current cursors/watchlist, and retention "
            "windows."
        )
    )
    async def get_started() -> dict[str, Any]:
        app = get_app_state()
        latest_bars = (
            app.market_store.latest_bar_cursor if app.market_store is not None else None
        )
        ctx = await loop_context(app)
        return {
            "service": "alpaca-news-mcp",
            "loop_recipe": [
                "1) call get_started once and store latest_cursors",
                "2) each tick: call poll_market with your saved cursors "
                "(first tick: cursor=-1 for a clean start-from-now)",
                "3) store each section's next_cursor verbatim; if has_more, "
                "poll again immediately",
                "4) wait context.suggested_next_poll_seconds between ticks",
                "5) investigate alerts with severity high/critical first; "
                "use get_symbol_context/get_stock_bars/search_news to dig in",
                "6) after a restart: pass cursor=-1 (fresh start) or your "
                "saved cursors (gap=true means retention pruned past them)",
            ],
            "cursor_contract": {
                "type": "monotonic integers, independent per feed "
                "(news/alerts/bars)",
                "pass_back": "next_cursor verbatim; never invent cursor values",
                "bootstrap": "-1 = tail (only future items), 0 = full "
                "retained history",
                "errors": "cursor_out_of_range carries latest_cursor to "
                "recover with",
                "gaps": "gap=true + oldest_available_cursor means retention "
                "pruned items you never saw",
            },
            "timestamp_units": {
                "news": "ISO-8601 UTC strings",
                "ticks (trades/quotes)": "ts_us = epoch microseconds UTC",
                "bars": "ts = bar start, epoch seconds UTC",
            },
            "error_schema": (
                "errors are {'error': code, 'hint': recovery action, ...}; "
                "retryable=true means retry shortly"
            ),
            "current": {
                "latest_cursors": {
                    "news": app.store.latest_article_cursor,
                    "alerts": app.store.latest_alert_cursor,
                    "bars": latest_bars,
                },
                "watchlist": (
                    app.stock_stream.watchlist()
                    if app.stock_stream is not None
                    else None
                ),
                "interest_symbols": sorted(app.state.get_interest_symbols()),
                "market": ctx,
            },
            "retention": {
                "news_days": app.config.event_retention_days,
                "bars_days": app.config.stock_bar_retention_days,
                "ticks_minutes": app.config.stock_tick_retention_minutes,
                "statuses_days": app.config.status_retention_days,
            },
        }

    @mcp.prompt(
        name="trading-loop",
        description="How to drive this server from a trading-bot polling loop.",
    )
    def trading_loop_prompt() -> str:
        return (
            "You are polling the alpaca-news-mcp server for market data.\n"
            "Loop contract:\n"
            "1. Call get_started once; store current.latest_cursors.\n"
            "2. Each tick, call poll_market(news_cursor=..., alerts_cursor=..., "
            "bars_cursor=...) with your saved cursors (-1 on the very first "
            "tick for a clean start).\n"
            "3. Save each section's next_cursor verbatim. has_more=true means "
            "poll again immediately.\n"
            "4. Respect context.suggested_next_poll_seconds and check "
            "context.degraded before trusting prices.\n"
            "5. Alerts with severity critical/high deserve immediate "
            "attention; use get_symbol_context, get_stock_bars, and "
            "search_news to investigate.\n"
            "6. Timestamps: news = ISO-8601, ticks = ts_us (epoch "
            "microseconds), bars = ts (epoch seconds).\n"
            "7. On cursor_out_of_range errors, resume from the returned "
            "latest_cursor. gap=true means retention pruned items you never "
            "saw — treat missing history as unknown, not as 'nothing "
            "happened'."
        )


__all__ = ["loop_context", "market_phase", "register"]

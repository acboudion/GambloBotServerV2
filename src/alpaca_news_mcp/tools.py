"""MCP tool implementations.

All tools are read-only except set_interest_symbols and ack_news_alert (local state mutation
only). No tool ever opens an Alpaca WebSocket — they all read from the local store + state.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from dateutil import parser as dateparser
from mcp.server.fastmcp import FastMCP

from .app_state import get_app_state
from .normalize import truncate
from .tool_common import (
    ALERT_CATEGORIES,
    SEVERITIES,
    VALID_FIELD_MODES,
    filter_over_cap,
    severities_at_or_above,
)
from .tool_common import (
    cursor_out_of_range as _cursor_out_of_range,
)
from .tool_common import (
    err as _err,
)
from .tool_common import (
    invalid_categories as _invalid_categories,
)
from .tool_common import (
    invalid_date as _invalid_date,
)
from .tool_common import (
    invalid_fields as _invalid_fields,
)
from .tool_common import (
    invalid_severity as _invalid_severity,
)
from .tool_common import (
    limit_exceeded as _limit_exceeded,
)
from .tool_common import (
    tail_response as _tail_response,
)

# Bounded-output defaults from spec §20
MAX_ARTICLES_DEFAULT = 50
MAX_ARTICLES_HARD = 200
MAX_RAW_EVENTS_DEFAULT = 100
MAX_RAW_EVENTS_HARD = 500
MAX_DIGEST_ARTICLES = 25
MAX_VERSIONS_DEFAULT = 50
MAX_VERSIONS_HARD = 200
MAX_CONTENT_CHARS = 4000
MAX_CURSOR_ITEMS = 500
MAX_SYMBOLS_PER_LOOKUP = 50
MAX_INTEREST_SYMBOLS = 200
MAX_SYMBOL_MAP_DEFAULT = 100
MAX_SYMBOL_MAP_HARD = 500
MAX_SYMBOL_MAP_MINUTES = 10_080  # 7 days
MAX_KEYWORD_GROUPS = 50
MAX_PHRASES_PER_LIST = 100
MAX_PHRASE_CHARS = 200

# stream_status keys for bot-managed runtime state (restored at startup).
ALERT_KEYWORD_OVERRIDES_KEY = "alert_keyword_overrides"
WATCHED_STORIES_KEY = "watched_stories"


def _filter_over_cap(values: list[str] | None) -> dict[str, Any] | None:
    return filter_over_cap(values, MAX_SYMBOLS_PER_LOOKUP)


def _validate_alert_filters(
    severity: str | None, categories: list[str] | None
) -> dict[str, Any] | None:
    """A typo'd severity/category silently returned zero alerts — exactly
    what 'nothing happened' looks like. Validate against the real enums."""
    if severity is not None and severity not in SEVERITIES:
        return _invalid_severity(severity)
    if categories:
        bad = [c for c in categories if c not in ALERT_CATEGORIES]
        if bad:
            return _invalid_categories(bad)
    return None


def _parse_window_bound(field: str, value: str, *, end_of_day: bool) -> str | dict[str, Any]:
    """Parse a since/until filter to the canonical stored ISO form
    ('+00:00' offset). A date-only `until` is pushed to end-of-day so
    '2026-07-04' includes July 4th instead of lexicographically excluding
    the entire day."""
    try:
        dt = dateparser.isoparse(value)
    except (ValueError, TypeError):
        return _invalid_date(field, value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    dt = dt.astimezone(UTC)
    if end_of_day and len(value.strip()) == 10:
        dt = dt + timedelta(days=1) - timedelta(microseconds=1)
    return dt.isoformat()


def _serialize_article(
    article: Any,
    *,
    fields: str = "standard",
    include_content: bool = False,
    include_raw: bool = False,
) -> dict[str, Any]:
    """Serialize an article at the requested projection level.

    compact  — headline-level identification only (token-lean for polling loops)
    standard — full metadata without body text
    full     — standard plus truncated content_text
    """
    if fields == "compact":
        out: dict[str, Any] = {
            "id": article.id,
            "headline": article.headline,
            "symbols": list(article.symbols),
            "source": article.source,
            "updated_at": article.updated_at or article.created_at,
            "url": article.url,
        }
    else:
        out = {
            "id": article.id,
            "headline": article.headline,
            "summary": article.summary,
            "author": article.author,
            "created_at": article.created_at,
            "updated_at": article.updated_at,
            "url": article.url,
            "source": article.source,
            "symbols": list(article.symbols),
            "first_seen_at": article.first_seen_at,
            "last_seen_at": article.last_seen_at,
            "last_seen_source": article.last_seen_source,
            "update_count": article.update_count,
            "latency_ms": article.latency_ms,
            "is_content_present": article.is_content_present,
        }
    if fields == "full" or include_content:
        out["content_text"] = truncate(article.content_text, MAX_CONTENT_CHARS)
    if include_raw:
        out["raw"] = article.raw
    return out


def register(mcp: FastMCP) -> None:
    """Register all tools on the FastMCP instance."""

    # ---- Health and status -------------------------------------------------

    @mcp.tool(description="Return current Alpaca news stream health and counters.")
    async def get_news_stream_health() -> dict[str, Any]:
        app = get_app_state()
        h = app.state.snapshot_health()
        d = h.model_dump()
        d["service"] = "alpaca-news-mcp"
        d["rest_backfill_enabled"] = app.config.enable_rest_backfill
        d["alerts_suppressed_by_rate_limit"] = app.alerts.suppressed_alerts
        return d

    @mcp.tool(description="Return requested vs acknowledged Alpaca news subscription state.")
    async def get_news_subscription_state() -> dict[str, Any]:
        app = get_app_state()
        s = app.state.subscription_state
        return s.model_dump()

    # ---- Article retrieval -------------------------------------------------

    @mcp.tool(description="Recent news articles within the past `minutes`.")
    async def get_recent_news(
        minutes: int = 60,
        symbols: list[str] | None = None,
        sources: list[str] | None = None,
        limit: int = 50,
        include_content: bool = False,
        include_raw: bool = False,
        fields: str = "standard",
    ) -> dict[str, Any]:
        if limit > MAX_ARTICLES_HARD:
            return _limit_exceeded(MAX_ARTICLES_HARD, limit)
        if (over := _filter_over_cap(symbols)) is not None:
            return over
        if (over := _filter_over_cap(sources)) is not None:
            return over
        if fields not in VALID_FIELD_MODES:
            return _invalid_fields(fields)
        limit = MAX_ARTICLES_DEFAULT if limit <= 0 else min(limit, MAX_ARTICLES_HARD)
        app = get_app_state()
        articles = await app.store.get_recent_articles(
            minutes=minutes, symbols=symbols, sources=sources, limit=limit
        )
        return {
            "count": len(articles),
            "articles": [
                _serialize_article(
                    a,
                    fields=fields,
                    include_content=include_content,
                    include_raw=include_raw,
                )
                for a in articles
            ],
        }

    @mcp.tool(
        description=(
            "Delta poll: news articles newer than `cursor` (a monotonic ingest "
            "sequence), oldest first. Returns next_cursor to pass back on the "
            "next call; has_more=true means call again immediately. cursor=0 "
            "starts from the oldest retained article; cursor=-1 starts from "
            "the tail (only future articles). latest_cursor is always "
            "included; gap=true means articles between your cursor and "
            "oldest_available_cursor were pruned by retention. Updated "
            "articles re-surface with a fresh cursor position."
        )
    )
    async def get_news_since(
        cursor: int = 0,
        limit: int = 100,
        symbols: list[str] | None = None,
        fields: str = "compact",
    ) -> dict[str, Any]:
        if limit > MAX_CURSOR_ITEMS:
            return _limit_exceeded(MAX_CURSOR_ITEMS, limit)
        if (over := _filter_over_cap(symbols)) is not None:
            return over
        if fields not in VALID_FIELD_MODES:
            return _invalid_fields(fields)
        limit = min(max(1, limit), MAX_CURSOR_ITEMS)
        app = get_app_state()
        latest = app.store.latest_article_cursor
        if cursor == -1:
            return _tail_response("articles", latest)
        cursor = max(0, cursor)
        if cursor > latest:
            return _cursor_out_of_range(cursor, latest)
        articles, next_cursor, has_more = await app.store.articles_since(
            cursor=cursor, limit=limit, symbols=symbols
        )
        serialized = []
        for a in articles:
            d = _serialize_article(a, fields=fields)
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
            min_seq = await app.store.min_article_seq()
            if min_seq and cursor < min_seq - 1:
                out["gap"] = True
                out["oldest_available_cursor"] = min_seq - 1
        return out

    @mcp.tool(
        description=(
            "Delta poll: alerts newer than `cursor`, oldest first. Each alert "
            "carries its own `cursor`; pass next_cursor back on the next call. "
            "has_more=true means call again immediately. cursor=-1 starts "
            "from the tail (only future alerts); latest_cursor is always "
            "included; gap=true means alerts were pruned past your cursor. "
            "severity is exact-match; min_severity='high' means high AND "
            "critical."
        )
    )
    async def get_alerts_since(
        cursor: int = 0,
        limit: int = 100,
        severity: str | None = None,
        min_severity: str | None = None,
        categories: list[str] | None = None,
    ) -> dict[str, Any]:
        if limit > MAX_CURSOR_ITEMS:
            return _limit_exceeded(MAX_CURSOR_ITEMS, limit)
        if (over := _filter_over_cap(categories)) is not None:
            return over
        if (bad := _validate_alert_filters(severity, categories)) is not None:
            return bad
        if min_severity is not None and min_severity not in SEVERITIES:
            return _invalid_severity(min_severity)
        if severity is not None and min_severity is not None:
            return _err(
                "conflicting_filters",
                "pass either severity (exact match) or min_severity "
                "(that level and above), not both",
            )
        severities = (
            severities_at_or_above(min_severity) if min_severity is not None else None
        )
        limit = min(max(1, limit), MAX_CURSOR_ITEMS)
        app = get_app_state()
        latest = app.store.latest_alert_cursor
        if cursor == -1:
            return _tail_response("alerts", latest)
        cursor = max(0, cursor)
        if cursor > latest:
            return _cursor_out_of_range(cursor, latest)
        alerts, next_cursor, has_more = await app.store.alerts_since(
            cursor=cursor,
            limit=limit,
            severity=severity,
            severities=severities,
            categories=categories,
        )
        out: dict[str, Any] = {
            "count": len(alerts),
            "alerts": alerts,
            "next_cursor": next_cursor,
            "latest_cursor": latest,
            "has_more": has_more,
        }
        if cursor:
            min_seq = await app.store.min_alert_seq()
            if min_seq and cursor < min_seq - 1:
                out["gap"] = True
                out["oldest_available_cursor"] = min_seq - 1
        return out

    @mcp.tool(description="Get a single news article by Alpaca id, optionally with version history.")
    async def get_news_article(
        article_id: int,
        include_content: bool = True,
        include_raw: bool = False,
        include_versions: bool = False,
        version_limit: int = MAX_VERSIONS_DEFAULT,
    ) -> dict[str, Any]:
        if include_versions and version_limit > MAX_VERSIONS_HARD:
            return _limit_exceeded(MAX_VERSIONS_HARD, version_limit)
        app = get_app_state()
        article = await app.store.get_article(article_id)
        if article is None:
            return _err("not_found", "no article with this Alpaca id is stored locally", article_id=article_id)
        result: dict[str, Any] = _serialize_article(
            article, include_content=include_content, include_raw=include_raw
        )
        if include_versions:
            bounded = min(max(1, version_limit), MAX_VERSIONS_HARD)
            versions_total = await app.store.count_versions(article_id)
            versions = await app.store.get_versions(article_id, limit=bounded)
            result["versions"] = [v.model_dump() for v in versions]
            result["versions_returned"] = len(versions)
            result["versions_total"] = versions_total
        return result

    @mcp.tool(
        description=(
            "Search persisted news articles by headline/summary/content text "
            "(FTS ranked). since/until: ISO-8601 or YYYY-MM-DD (a date-only "
            "until includes that whole day)."
        )
    )
    async def search_news(
        query: str,
        symbols: list[str] | None = None,
        since: str | None = None,
        until: str | None = None,
        limit: int = 50,
        include_content: bool = False,
        fields: str = "standard",
    ) -> dict[str, Any]:
        if limit > MAX_ARTICLES_HARD:
            return _limit_exceeded(MAX_ARTICLES_HARD, limit)
        if (over := _filter_over_cap(symbols)) is not None:
            return over
        if fields not in VALID_FIELD_MODES:
            return _invalid_fields(fields)
        if not query or not query.strip():
            return _err("empty_query", "pass one or more search terms")
        if since is not None:
            parsed = _parse_window_bound("since", since, end_of_day=False)
            if isinstance(parsed, dict):
                return parsed
            since = parsed
        if until is not None:
            parsed = _parse_window_bound("until", until, end_of_day=True)
            if isinstance(parsed, dict):
                return parsed
            until = parsed
        app = get_app_state()
        articles = await app.store.search_articles(
            query=query.strip(),
            symbols=symbols,
            since=since,
            until=until,
            limit=min(max(1, limit), MAX_ARTICLES_HARD),
        )
        return {
            "count": len(articles),
            "articles": [
                _serialize_article(a, fields=fields, include_content=include_content)
                for a in articles
            ],
        }

    @mcp.tool(description="Recent news articles bucketed per symbol.")
    async def get_news_for_symbols(
        symbols: list[str],
        minutes: int = 1440,
        limit_per_symbol: int = 25,
        include_content: bool = False,
        fields: str = "standard",
    ) -> dict[str, Any]:
        if not symbols:
            return _err("no_symbols", "pass at least one ticker symbol")
        # One query per symbol and up to limit_per_symbol rows each — an
        # unbounded symbol list would multiply past the bounded-response
        # contract (500 symbols x 25 = 12,500 articles).
        if len(symbols) > MAX_SYMBOLS_PER_LOOKUP:
            return _limit_exceeded(MAX_SYMBOLS_PER_LOOKUP, len(symbols))
        if limit_per_symbol > MAX_ARTICLES_HARD:
            return _limit_exceeded(MAX_ARTICLES_HARD, limit_per_symbol)
        if fields not in VALID_FIELD_MODES:
            return _invalid_fields(fields)
        # Clamp to >= 1: SQLite treats LIMIT <= 0 as "no limit", which would
        # bypass the bounded-response contract.
        limit_per_symbol = max(1, min(limit_per_symbol, MAX_ARTICLES_HARD))
        app = get_app_state()
        bucketed = await app.store.articles_for_symbols(
            symbols, minutes=minutes, limit_per_symbol=limit_per_symbol
        )
        out: dict[str, Any] = {}
        for sym, articles in bucketed.items():
            out[sym] = [
                _serialize_article(a, fields=fields, include_content=include_content)
                for a in articles
            ]
        return {"symbols": out}

    @mcp.tool(
        description=(
            "Compact digest of recent breaking-keyword/critical news. Returns "
            "only articles matching breaking markers; breaking_matches=false "
            "with an empty list means nothing breaking happened (use "
            "get_recent_news for ordinary coverage)."
        )
    )
    async def get_breaking_news_digest(
        minutes: int = 15,
        symbols: list[str] | None = None,
        max_articles: int = 25,
    ) -> dict[str, Any]:
        if max_articles > MAX_DIGEST_ARTICLES:
            return _limit_exceeded(MAX_DIGEST_ARTICLES, max_articles)
        if (over := _filter_over_cap(symbols)) is not None:
            return over
        # Clamp to >= 1: SQLite treats LIMIT <= 0 as "no limit", and a negative
        # slice (filtered[:-N]) would silently drop items rather than bound them.
        max_articles = max(1, min(max_articles, MAX_DIGEST_ARTICLES))
        app = get_app_state()
        articles = await app.store.get_recent_articles(
            minutes=minutes,
            symbols=symbols,
            sources=None,
            limit=max_articles,
        )
        # Filter to breaking-style markers. No silent fallback to ordinary
        # recent news: a digest that substitutes routine articles when
        # nothing matches presents non-breaking news as breaking.
        breaking_terms = ("breaking", "halted", "delisting", "bankruptcy", "alert:", "developing")
        filtered = []
        for a in articles:
            text = " ".join(filter(None, [a.headline, a.summary, a.content_text])).lower()
            if any(term in text for term in breaking_terms):
                filtered.append(a)
        return {
            "count": len(filtered),
            "breaking_matches": bool(filtered),
            "articles": [
                {
                    "id": a.id,
                    "headline": a.headline,
                    "symbols": list(a.symbols),
                    "source": a.source,
                    "url": a.url,
                    "updated_at": a.updated_at,
                    "summary": truncate(a.summary, 240),
                }
                for a in filtered[:max_articles]
            ],
        }

    # ---- Symbol & alert tools ---------------------------------------------

    @mcp.tool(
        description=(
            "Update local interest-symbol filters (max 200 symbols). "
            "mode: replace|add|remove|clear."
        )
    )
    async def set_interest_symbols(
        symbols: list[str], mode: str = "replace"
    ) -> dict[str, Any]:
        if mode not in ("replace", "add", "remove", "clear"):
            return _err("invalid_mode", "use one of: replace, add, remove, clear", mode=mode)
        if len(symbols) > MAX_INTEREST_SYMBOLS:
            return _limit_exceeded(MAX_INTEREST_SYMBOLS, len(symbols))
        app = get_app_state()
        if mode == "clear":
            symbols = []
        new_set = app.state.set_interest_symbols(symbols, mode)
        await app.store.set_status(
            "interest_symbols", {"symbols": sorted(new_set), "mode": mode}
        )
        return {"interest_symbols": sorted(new_set), "mode": mode}

    @mcp.tool(description="Return current interest symbols set.")
    async def get_interest_symbols() -> dict[str, Any]:
        app = get_app_state()
        return {"interest_symbols": sorted(app.state.get_interest_symbols())}

    @mcp.tool(description="Recent deterministic news alerts.")
    async def get_news_alerts(
        minutes: int = 240,
        severity: str | None = None,
        categories: list[str] | None = None,
        symbols: list[str] | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        if limit > 500:
            return _limit_exceeded(500, limit)
        if (over := _filter_over_cap(symbols)) is not None:
            return over
        if (over := _filter_over_cap(categories)) is not None:
            return over
        if (bad := _validate_alert_filters(severity, categories)) is not None:
            return bad
        app = get_app_state()
        alerts = await app.store.get_alerts(
            minutes=minutes,
            severity=severity,
            categories=categories,
            symbols=symbols,
            limit=min(max(1, limit), 500),
        )
        return {"count": len(alerts), "alerts": [a.model_dump() for a in alerts]}

    @mcp.tool(description="Acknowledge a news alert by alert_id.")
    async def ack_news_alert(alert_id: str) -> dict[str, Any]:
        app = get_app_state()
        ok = await app.store.ack_alert(alert_id)
        return {"acknowledged": ok, "alert_id": alert_id}

    @mcp.tool(
        description=(
            "Recent symbol → article-count map, busiest symbols first "
            "(bounded: limit <= 500, minutes <= 10080). truncated=true means "
            "more symbols exist beyond the limit."
        )
    )
    async def get_news_symbol_map(
        minutes: int = 60, min_articles: int = 1, limit: int = MAX_SYMBOL_MAP_DEFAULT
    ) -> dict[str, Any]:
        if limit > MAX_SYMBOL_MAP_HARD:
            return _limit_exceeded(MAX_SYMBOL_MAP_HARD, limit)
        # The wildcard firehose can accumulate thousands of distinct symbols
        # over long windows — this was the one tool violating the
        # bounded-response contract.
        clamped = minutes > MAX_SYMBOL_MAP_MINUTES
        minutes = min(max(1, minutes), MAX_SYMBOL_MAP_MINUTES)
        limit = max(1, limit)
        app = get_app_state()
        m = await app.store.symbol_map(
            minutes=minutes, min_articles=max(1, min_articles), limit=limit + 1
        )
        truncated = len(m) > limit
        if truncated:
            m = dict(list(m.items())[:limit])
        out: dict[str, Any] = {
            "window_minutes": minutes,
            "symbol_counts": m,
            "truncated": truncated,
        }
        if clamped:
            out["clamped_to_minutes"] = minutes
        return out

    # ---- Runtime news watches (keyword groups + story follows) --------------

    @mcp.tool(
        description=(
            "Create or replace a runtime alert keyword group — retarget the "
            "news radar without a restart (e.g. after entering a biotech "
            "position, add FDA/PDUFA/CRL phrases). Matching articles emit "
            "alerts (category custom_keyword for new groups, the native "
            "category when extending a built-in group like mna/earnings). "
            "critical_phrases escalate severity to critical; bullish/bearish "
            "extend direction tagging. Persists across restarts."
        )
    )
    async def set_alert_keyword_group(
        group: str,
        phrases: list[str],
        critical_phrases: list[str] | None = None,
        bullish: list[str] | None = None,
        bearish: list[str] | None = None,
    ) -> dict[str, Any]:
        group = group.strip().lower()
        if not group or len(group) > 64:
            return _err("invalid_group", "group must be 1-64 characters", group=group)
        spec_lists = {
            "phrases": phrases,
            "critical_phrases": critical_phrases or [],
            "bullish": bullish or [],
            "bearish": bearish or [],
        }
        for key, values in spec_lists.items():
            if len(values) > MAX_PHRASES_PER_LIST:
                return _limit_exceeded(MAX_PHRASES_PER_LIST, len(values))
            if any(len(str(v)) > MAX_PHRASE_CHARS for v in values):
                return _err(
                    "phrase_too_long",
                    f"keep phrases under {MAX_PHRASE_CHARS} characters",
                    field=key,
                )
        cleaned = {
            key: [str(v).strip().lower() for v in values if str(v).strip()]
            for key, values in spec_lists.items()
        }
        if not cleaned["phrases"] and not cleaned["critical_phrases"]:
            return _err(
                "no_phrases", "pass at least one phrase or critical phrase"
            )
        app = get_app_state()
        existing = app.alerts.runtime_keyword_groups()
        if group not in existing and len(existing) >= MAX_KEYWORD_GROUPS:
            return _limit_exceeded(MAX_KEYWORD_GROUPS, len(existing) + 1)
        app.alerts.set_runtime_group(group, cleaned)
        await app.store.set_status(
            ALERT_KEYWORD_OVERRIDES_KEY, app.alerts.runtime_keyword_groups()
        )
        summary = app.alerts.keyword_group_summary().get(group, {})
        return {
            "group": group,
            "category": summary.get("category"),
            "phrases": summary.get("phrases", []),
            "persisted": True,
        }

    @mcp.tool(
        description=(
            "Delete a runtime alert keyword group. Deleting an override of a "
            "built-in group (mna, earnings, ...) reverts it to its built-in "
            "phrases; built-ins themselves cannot be deleted."
        )
    )
    async def delete_alert_keyword_group(group: str) -> dict[str, Any]:
        app = get_app_state()
        group = group.strip().lower()
        if not app.alerts.delete_runtime_group(group):
            return _err(
                "not_found",
                "no runtime group with this name (built-ins cannot be "
                "deleted; see get_alert_keyword_groups)",
                group=group,
            )
        await app.store.set_status(
            ALERT_KEYWORD_OVERRIDES_KEY, app.alerts.runtime_keyword_groups()
        )
        return {"deleted": group}

    @mcp.tool(
        description=(
            "All effective alert keyword groups: built-in and runtime, the "
            "alert category each maps to, and every matchable phrase."
        )
    )
    async def get_alert_keyword_groups() -> dict[str, Any]:
        app = get_app_state()
        return {"groups": app.alerts.keyword_group_summary()}

    @mcp.tool(
        description=(
            "Follow a developing story (max 50): future content updates to "
            "this article emit a high-severity watched_story_update alert "
            "into the alert feed. Persists across restarts."
        )
    )
    async def watch_story(article_id: int) -> dict[str, Any]:
        app = get_app_state()
        article = await app.store.get_article(article_id)
        if article is None:
            return _err(
                "not_found",
                "no article with this Alpaca id is stored locally",
                article_id=article_id,
            )
        if not app.state.watch_article(
            article_id, datetime.now(UTC).isoformat()
        ):
            return _limit_exceeded(50, len(app.state.get_watched_articles()) + 1)
        await app.store.set_status(
            WATCHED_STORIES_KEY,
            {
                "articles": {
                    str(k): v for k, v in app.state.get_watched_articles().items()
                }
            },
        )
        return {
            "watching": article_id,
            "headline": article.headline,
            "update_count": article.update_count,
        }

    @mcp.tool(description="Stop following a watched story.")
    async def unwatch_story(article_id: int) -> dict[str, Any]:
        app = get_app_state()
        removed = app.state.unwatch_article(article_id)
        if removed:
            await app.store.set_status(
                WATCHED_STORIES_KEY,
                {
                    "articles": {
                        str(k): v
                        for k, v in app.state.get_watched_articles().items()
                    }
                },
            )
        return {"unwatched": removed, "article_id": article_id}

    @mcp.tool(description="Currently watched stories with their latest state.")
    async def get_watched_stories() -> dict[str, Any]:
        app = get_app_state()
        watched = app.state.get_watched_articles()
        stories = []
        for article_id, watched_at in sorted(watched.items()):
            article = await app.store.get_article(article_id)
            entry: dict[str, Any] = {
                "article_id": article_id,
                "watched_at": watched_at,
            }
            if article is not None:
                entry.update(
                    headline=article.headline,
                    update_count=article.update_count,
                    updated_at=article.updated_at,
                    symbols=list(article.symbols),
                )
            else:
                entry["note"] = "article pruned by retention"
            stories.append(entry)
        return {"count": len(stories), "stories": stories}

    # ---- Diagnostics -------------------------------------------------------

    @mcp.tool(description="WebSocket-to-receipt latency stats over the past `minutes`.")
    async def get_news_latency_stats(minutes: int = 60) -> dict[str, Any]:
        app = get_app_state()
        s = await app.store.latency_stats(minutes)
        return s.model_dump()

    @mcp.tool(description="Article/version/symbol/source ingestion counts over `minutes`.")
    async def get_news_ingestion_stats(minutes: int = 60) -> dict[str, Any]:
        app = get_app_state()
        s = await app.store.ingestion_stats(minutes)
        return s.model_dump()

    @mcp.tool(description="Recent raw Alpaca WS events captured for diagnostics.")
    async def get_raw_news_events(
        minutes: int = 15, limit: int = 100
    ) -> dict[str, Any]:
        if limit > MAX_RAW_EVENTS_HARD:
            return _limit_exceeded(MAX_RAW_EVENTS_HARD, limit)
        app = get_app_state()
        events = await app.store.recent_raw_events(
            minutes=minutes, limit=min(max(1, limit), MAX_RAW_EVENTS_HARD)
        )
        return {"count": len(events), "events": events}

    @mcp.tool(description="Version history for an article, bounded by limit.")
    async def get_news_versions(
        article_id: int, limit: int = MAX_VERSIONS_DEFAULT
    ) -> dict[str, Any]:
        if limit > MAX_VERSIONS_HARD:
            return _limit_exceeded(MAX_VERSIONS_HARD, limit)
        limit = min(max(1, limit), MAX_VERSIONS_HARD)
        app = get_app_state()
        versions_total = await app.store.count_versions(article_id)
        versions = await app.store.get_versions(article_id, limit=limit)
        return {
            "article_id": article_id,
            "count": len(versions),
            "versions_total": versions_total,
            "versions": [v.model_dump() for v in versions],
        }

    @mcp.tool(description="Trigger an Alpaca News REST backfill over the past `minutes`.")
    async def run_news_rest_backfill(minutes: int = 30) -> dict[str, Any]:
        app = get_app_state()
        if not app.config.enable_manual_rest_backfill:
            return _err("disabled", "manual REST backfill is off (ENABLE_MANUAL_REST_BACKFILL=false)")
        if minutes <= 0 or minutes > 60 * 24 * 7:
            return _err("invalid_minutes", "pass minutes between 1 and 10080", min=1, max=60 * 24 * 7)
        result = await app.rest_backfill.manual(minutes)
        failure = result.pop("failure", None)
        out = {
            "window_minutes": minutes,
            **result,
            "status": "incomplete" if failure else "ok",
        }
        if failure:
            out["failure"] = failure
        return out


__all__ = ["register"]

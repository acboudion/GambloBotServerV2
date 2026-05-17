"""MCP resource registrations."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP

from .app_state import get_app_state
from .normalize import truncate

MAX_CONTENT_CHARS = 4000


def register(mcp: FastMCP) -> None:
    @mcp.resource("alpaca-news://health", mime_type="application/json")
    async def health_resource() -> str:
        app = get_app_state()
        h = app.state.snapshot_health()
        return json.dumps(h.model_dump(), default=str)

    @mcp.resource("alpaca-news://subscription", mime_type="application/json")
    async def subscription_resource() -> str:
        app = get_app_state()
        return json.dumps(app.state.subscription_state.model_dump(), default=str)

    @mcp.resource("alpaca-news://recent", mime_type="application/json")
    async def recent_resource() -> str:
        app = get_app_state()
        articles = await app.store.get_recent_articles(minutes=60, limit=50)
        return json.dumps(
            [
                {
                    "id": a.id,
                    "headline": a.headline,
                    "symbols": list(a.symbols),
                    "source": a.source,
                    "updated_at": a.updated_at,
                    "url": a.url,
                }
                for a in articles
            ],
            default=str,
        )

    @mcp.resource("alpaca-news://alerts", mime_type="application/json")
    async def alerts_resource() -> str:
        app = get_app_state()
        alerts = await app.store.get_alerts(minutes=240, limit=100)
        return json.dumps([a.model_dump() for a in alerts], default=str)

    @mcp.resource("alpaca-news://symbols", mime_type="application/json")
    async def symbols_resource() -> str:
        app = get_app_state()
        m = await app.store.symbol_map(minutes=60, min_articles=1)
        return json.dumps({"window_minutes": 60, "symbol_counts": m}, default=str)

    @mcp.resource("alpaca-news://symbols/{symbol}", mime_type="application/json")
    async def symbols_for_one(symbol: str) -> str:
        app = get_app_state()
        bucketed = await app.store.articles_for_symbols(
            [symbol], minutes=1440, limit_per_symbol=25
        )
        articles = bucketed.get(symbol.upper(), [])
        return json.dumps(
            [
                {
                    "id": a.id,
                    "headline": a.headline,
                    "source": a.source,
                    "updated_at": a.updated_at,
                    "url": a.url,
                }
                for a in articles
            ],
            default=str,
        )

    @mcp.resource("alpaca-news://article/{article_id}", mime_type="application/json")
    async def article_one(article_id: str) -> str:
        app = get_app_state()
        try:
            aid = int(article_id)
        except ValueError:
            return json.dumps({"error": "invalid_id", "article_id": article_id})
        article = await app.store.get_article(aid)
        if article is None:
            return json.dumps({"error": "not_found", "article_id": aid})
        return json.dumps(
            {
                "id": article.id,
                "headline": article.headline,
                "summary": article.summary,
                "content_text": truncate(article.content_text, MAX_CONTENT_CHARS),
                "symbols": list(article.symbols),
                "source": article.source,
                "url": article.url,
                "created_at": article.created_at,
                "updated_at": article.updated_at,
            },
            default=str,
        )

    @mcp.resource("alpaca-news://stats/latency", mime_type="application/json")
    async def stats_latency() -> str:
        app = get_app_state()
        s = await app.store.latency_stats(60)
        return json.dumps(s.model_dump(), default=str)

    @mcp.resource("alpaca-news://stats/ingestion", mime_type="application/json")
    async def stats_ingestion() -> str:
        app = get_app_state()
        s = await app.store.ingestion_stats(60)
        return json.dumps(s.model_dump(), default=str)


__all__ = ["register"]

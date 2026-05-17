"""Quick live check: counters + a wider manual backfill, to corroborate data flow."""

from __future__ import annotations

import asyncio
import json

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


URL = "http://localhost:8000/mcp"


def _decode(result):
    if not result or not result.content:
        return None
    text = getattr(result.content[0], "text", None)
    return json.loads(text) if text else None


async def main():
    async with streamablehttp_client(URL) as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()
            h = _decode(await session.call_tool("get_news_stream_health", {}))
            print("HEALTH:")
            print(json.dumps(h, indent=2))
            print()

            # Try a wider manual backfill
            for minutes in (240, 720, 1440):
                r = _decode(await session.call_tool("run_news_rest_backfill", {"minutes": minutes}))
                print(f"manual backfill[{minutes}m]: {r}")

            print()
            ing = _decode(await session.call_tool("get_news_ingestion_stats", {"minutes": 60}))
            print(f"ingestion[60m]: {ing}")
            ing = _decode(await session.call_tool("get_news_ingestion_stats", {"minutes": 1440}))
            print(f"ingestion[1440m]: {ing}")

            recent = _decode(await session.call_tool("get_recent_news", {"minutes": 1440, "limit": 200}))
            arts = recent.get("articles", []) if isinstance(recent, dict) else []
            print(f"\nrecent[1440m] count: {len(arts)}")
            sources = {}
            for a in arts:
                src = a.get("last_seen_source")
                sources[src] = sources.get(src, 0) + 1
            print(f"by last_seen_source: {sources}")
            negs = sum(1 for a in arts if isinstance(a.get("latency_ms"), int) and a["latency_ms"] < 0)
            print(f"articles with negative latency_ms: {negs}/{len(arts)}")
            top_neg = sorted([a for a in arts if isinstance(a.get("latency_ms"), int) and a["latency_ms"] < 0], key=lambda a: a["latency_ms"])[:5]
            for a in top_neg:
                print(f"  neg lat {a['latency_ms']}: id={a['id']} created={a['created_at']} first_seen={a['first_seen_at']} src={a['last_seen_source']}")

            raw = _decode(await session.call_tool("get_raw_news_events", {"minutes": 1440, "limit": 100}))
            print(f"\nraw events[1440m]: count={raw.get('count')}")
            for e in (raw.get('events') or [])[:3]:
                print(f"  raw: {json.dumps({k: e.get(k) for k in ('id','endpoint','message_type','received_at')}, default=str)}")


if __name__ == "__main__":
    asyncio.run(main())

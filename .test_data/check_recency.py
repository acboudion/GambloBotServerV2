"""Look at the freshest articles, source breakdown, and any negative-latency entries."""

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

            # Pull a wider window
            recent = _decode(await session.call_tool(
                "get_recent_news", {"minutes": 1440, "limit": 200}
            ))
            arts = recent.get("articles", []) if isinstance(recent, dict) else []
            print(f"Total articles: {len(arts)}")

            sources_by_field = {}
            ws_articles = []
            negs = []
            for a in arts:
                s = a.get("source")
                sources_by_field[s] = sources_by_field.get(s, 0) + 1
                if a.get("last_seen_source") == "ws":
                    ws_articles.append(a)
                if isinstance(a.get("latency_ms"), int) and a["latency_ms"] < 0:
                    negs.append(a)

            print(f"By article.source: {sources_by_field}")
            print(f"WS-sourced: {len(ws_articles)}")
            print(f"Negative-latency: {len(negs)}")
            for a in negs[:5]:
                print(f"  id={a['id']} lat={a['latency_ms']} created={a['created_at']} first={a['first_seen_at']} src={a['last_seen_source']}")
            for a in ws_articles[:5]:
                print(f"  WS: id={a['id']} lat={a['latency_ms']} created={a['created_at']} first={a['first_seen_at']}")

            print("\nFreshest 5 by created_at:")
            for a in sorted(arts, key=lambda x: x.get("created_at") or "", reverse=True)[:5]:
                print(f"  created={a['created_at']} src={a.get('last_seen_source')} headline={a['headline'][:80]!r}")

            print("\nFreshest 5 by first_seen_at:")
            for a in sorted(arts, key=lambda x: x.get("first_seen_at") or "", reverse=True)[:5]:
                print(f"  first_seen={a['first_seen_at']} src={a.get('last_seen_source')} created={a['created_at']} headline={a['headline'][:80]!r}")


if __name__ == "__main__":
    asyncio.run(main())

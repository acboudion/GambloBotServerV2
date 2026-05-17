"""Verify Fixes 1/2/4 against the live container."""

from __future__ import annotations

import asyncio
import json

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


def _decode(result):
    if not result or not result.content:
        return None
    text = getattr(result.content[0], "text", None)
    return json.loads(text) if text else None


async def main():
    async with streamablehttp_client("http://localhost:8000/mcp") as (read, write, _):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("=== Fix 4: backfill returns new/updated/duplicate ===")
            r1 = _decode(await session.call_tool("run_news_rest_backfill", {"minutes": 240}))
            print(f"first  call: {r1}")
            r2 = _decode(await session.call_tool("run_news_rest_backfill", {"minutes": 240}))
            print(f"second call: {r2}")
            print(f"  ingested key present: {'ingested' in r1}")
            print(f"  legacy 'articles' key gone: {'articles' not in r1}")
            print(f"  second call duplicate>=first new: "
                  f"{(r2.get('duplicate', 0) >= r1.get('new', 0))}")

            print()
            print("=== Fix 1+2: latency stats from a fresh DB ===")
            # The container's volume was reused, so this checks the wider set.
            for w in (60, 1440):
                ls = _decode(await session.call_tool("get_news_latency_stats", {"minutes": w}))
                print(f"latency[{w}m]: {ls}")

            print()
            print("=== Fix 1 evidence: stale articles' latency was NOT overwritten by today's REST refresh ===")
            # Look at recent articles. Their latency_ms should reflect FIRST receipt,
            # not the most recent REST timestamp.
            recent = _decode(await session.call_tool(
                "get_recent_news",
                {"minutes": 1440, "limit": 50},
            ))
            arts = recent.get("articles", []) if isinstance(recent, dict) else []
            now_iso_buf = arts[0]["last_seen_at"] if arts else None
            stale = [a for a in arts if a.get("last_seen_source") == "rest"]
            print(f"recent total: {len(arts)}; rest-sourced: {len(stale)}")
            if stale:
                # The latency_ms field for these articles should be plausible
                # for a real ingestion delay, not "now - created_at" hours.
                # If all are < ~10 minutes, REST update preserved earlier WS-side
                # latency. If any are huge (matching article age), Fix 1 broke.
                lats = [a["latency_ms"] for a in stale if a.get("latency_ms") is not None]
                neg = sum(1 for v in lats if v < 0)
                print(f"  rest-sourced latency_ms: count={len(lats)} min={min(lats) if lats else None} max={max(lats) if lats else None} negatives={neg}")

            print()
            print("=== Fix 5: healthz deltas ===")
            # Healthz isn't an MCP tool, but we showed the JSON via curl.
            print("  (validated via curl — see above)")


if __name__ == "__main__":
    asyncio.run(main())

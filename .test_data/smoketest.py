"""End-to-end smoketest for alpaca-news-mcp via Streamable HTTP.

Connects, lists tools/resources, invokes every tool with realistic arguments,
reads every resource, captures result/exception per call, and prints a JSON
summary at the end. Designed to be run twice (early + late) so we can see the
delta as data accumulates.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import traceback
from datetime import datetime, timezone

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client


URL = "http://localhost:8000/mcp"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _content_to_dict(result):
    """Best-effort decode of a CallToolResult.content block into a Python dict."""
    if result is None:
        return None
    if getattr(result, "isError", False):
        return {"_isError": True, "_content": [str(c) for c in (result.content or [])]}
    out = []
    for c in result.content or []:
        text = getattr(c, "text", None)
        if text is not None:
            try:
                out.append(json.loads(text))
            except Exception:
                out.append({"_text": text})
        else:
            out.append({"_repr": repr(c)})
    if len(out) == 1:
        return out[0]
    return out


async def call_tool(session: ClientSession, name: str, args: dict, results: dict, key: str | None = None):
    label = key or name
    try:
        r = await session.call_tool(name, args)
        results[label] = {"ok": not getattr(r, "isError", False), "result": _content_to_dict(r)}
    except Exception as e:
        results[label] = {"ok": False, "exception": f"{type(e).__name__}: {e}", "tb": traceback.format_exc()}


async def read_resource(session: ClientSession, uri: str, results: dict):
    try:
        r = await session.read_resource(uri)
        contents = []
        for c in r.contents or []:
            text = getattr(c, "text", None)
            blob = getattr(c, "blob", None)
            if text is not None:
                try:
                    contents.append(json.loads(text))
                except Exception:
                    contents.append({"_text": text[:500]})
            elif blob is not None:
                contents.append({"_blob_bytes": len(blob)})
            else:
                contents.append({"_repr": repr(c)})
        results[uri] = {"ok": True, "contents": contents}
    except Exception as e:
        results[uri] = {"ok": False, "exception": f"{type(e).__name__}: {e}"}


async def run(label: str, recent_id_box: dict | None = None) -> dict:
    started = now_iso()
    out: dict = {"label": label, "started": started, "url": URL, "tools": {}, "resources": {}}

    async with streamablehttp_client(URL) as (read, write, _):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            out["server_info"] = {
                "name": getattr(init.serverInfo, "name", None),
                "version": getattr(init.serverInfo, "version", None),
            }

            tools = await session.list_tools()
            out["tools_list"] = sorted([t.name for t in tools.tools])

            resources = await session.list_resources()
            out["resources_list"] = sorted([str(r.uri) for r in resources.resources])

            templates = []
            try:
                t_resp = await session.list_resource_templates()
                templates = [str(t.uriTemplate) for t in t_resp.resourceTemplates]
            except Exception as e:
                templates = [f"_error: {e}"]
            out["resource_templates_list"] = sorted(templates)

            results = out["tools"]

            # Health & status
            await call_tool(session, "get_news_stream_health", {}, results)
            await call_tool(session, "get_news_subscription_state", {}, results)

            # Article retrieval — generous windows so REST backfill data shows up
            await call_tool(session, "get_recent_news", {"minutes": 1440, "limit": 25}, results, "get_recent_news[1440m]")
            await call_tool(session, "get_recent_news", {"minutes": 1440, "limit": 5, "include_content": True}, results, "get_recent_news[content]")
            await call_tool(session, "get_recent_news", {"minutes": 1440, "limit": 1000}, results, "get_recent_news[oversize]")
            await call_tool(session, "get_recent_news", {"minutes": 1440, "symbols": ["AAPL", "MSFT", "NVDA", "TSLA", "SPY", "QQQ"], "limit": 25}, results, "get_recent_news[symbols]")

            # Capture the most recent article id so we can probe single-article tools.
            recent = results.get("get_recent_news[1440m]", {}).get("result")
            article_id = None
            if isinstance(recent, dict):
                arts = recent.get("articles") or []
                if arts:
                    article_id = arts[0].get("id")
            if recent_id_box is not None and article_id:
                recent_id_box["id"] = article_id

            await call_tool(session, "get_news_article", {"article_id": article_id or 0, "include_content": True, "include_versions": True}, results, "get_news_article")
            await call_tool(session, "get_news_article", {"article_id": -1}, results, "get_news_article[missing]")

            await call_tool(session, "search_news", {"query": "the", "limit": 10}, results, "search_news")
            await call_tool(session, "search_news", {"query": "  ", "limit": 5}, results, "search_news[empty]")
            await call_tool(session, "search_news", {"query": "the", "limit": 10000}, results, "search_news[oversize]")

            await call_tool(session, "get_news_for_symbols", {"symbols": ["AAPL", "MSFT", "NVDA", "TSLA", "SPY", "QQQ"], "minutes": 1440, "limit_per_symbol": 5}, results, "get_news_for_symbols")
            await call_tool(session, "get_news_for_symbols", {"symbols": [], "minutes": 60}, results, "get_news_for_symbols[empty]")
            await call_tool(session, "get_news_for_symbols", {"symbols": ["AAPL"], "minutes": 60, "limit_per_symbol": 9999}, results, "get_news_for_symbols[oversize]")

            await call_tool(session, "get_breaking_news_digest", {"minutes": 1440, "max_articles": 10}, results, "get_breaking_news_digest")
            await call_tool(session, "get_breaking_news_digest", {"minutes": 1440, "max_articles": 9999}, results, "get_breaking_news_digest[oversize]")

            # Symbols & alerts
            await call_tool(session, "get_interest_symbols", {}, results)
            await call_tool(session, "set_interest_symbols", {"symbols": ["GOOGL", "AMZN"], "mode": "add"}, results, "set_interest_symbols[add]")
            await call_tool(session, "set_interest_symbols", {"symbols": ["GOOGL"], "mode": "remove"}, results, "set_interest_symbols[remove]")
            await call_tool(session, "set_interest_symbols", {"symbols": ["AAPL", "MSFT", "NVDA", "TSLA", "SPY", "QQQ"], "mode": "replace"}, results, "set_interest_symbols[replace]")
            await call_tool(session, "set_interest_symbols", {"symbols": [], "mode": "clear"}, results, "set_interest_symbols[clear]")
            await call_tool(session, "set_interest_symbols", {"symbols": ["AAPL", "MSFT", "NVDA", "TSLA", "SPY", "QQQ"], "mode": "replace"}, results, "set_interest_symbols[restore]")
            await call_tool(session, "set_interest_symbols", {"symbols": ["X"], "mode": "bogus"}, results, "set_interest_symbols[bad_mode]")

            await call_tool(session, "get_news_alerts", {"minutes": 1440, "limit": 25}, results, "get_news_alerts")
            await call_tool(session, "get_news_alerts", {"minutes": 1440, "limit": 99999}, results, "get_news_alerts[oversize]")

            # Try to ack the first available alert (best-effort).
            alerts_payload = results.get("get_news_alerts", {}).get("result") or {}
            alert_id = None
            for a in (alerts_payload.get("alerts") or []):
                alert_id = a.get("id") or a.get("alert_id")
                if alert_id:
                    break
            await call_tool(session, "ack_news_alert", {"alert_id": alert_id or "no-such-alert"}, results, "ack_news_alert")

            await call_tool(session, "get_news_symbol_map", {"minutes": 1440, "min_articles": 1}, results, "get_news_symbol_map")

            # Diagnostics
            await call_tool(session, "get_news_latency_stats", {"minutes": 60}, results, "get_news_latency_stats")
            await call_tool(session, "get_news_ingestion_stats", {"minutes": 60}, results, "get_news_ingestion_stats")
            await call_tool(session, "get_raw_news_events", {"minutes": 60, "limit": 25}, results, "get_raw_news_events")
            await call_tool(session, "get_raw_news_events", {"minutes": 60, "limit": 99999}, results, "get_raw_news_events[oversize]")
            await call_tool(session, "get_news_versions", {"article_id": article_id or 0}, results, "get_news_versions")

            # Manual REST backfill (configured to be enabled).
            await call_tool(session, "run_news_rest_backfill", {"minutes": 60}, results, "run_news_rest_backfill")
            await call_tool(session, "run_news_rest_backfill", {"minutes": 0}, results, "run_news_rest_backfill[invalid]")

            # Resources
            res = out["resources"]
            for uri in ["alpaca-news://health", "alpaca-news://subscription", "alpaca-news://recent",
                        "alpaca-news://alerts", "alpaca-news://symbols", "alpaca-news://stats/latency",
                        "alpaca-news://stats/ingestion"]:
                await read_resource(session, uri, res)
            # Templated resources
            await read_resource(session, "alpaca-news://symbols/AAPL", res)
            if article_id:
                await read_resource(session, f"alpaca-news://article/{article_id}", res)
            await read_resource(session, "alpaca-news://article/notanint", res)
            await read_resource(session, "alpaca-news://article/-1", res)

    out["finished"] = now_iso()
    return out


def summarize(report: dict) -> dict:
    tools = report["tools"]
    res = report["resources"]
    fail_tools = sorted([k for k, v in tools.items() if not v.get("ok")])
    fail_res = sorted([k for k, v in res.items() if not v.get("ok")])
    return {
        "label": report["label"],
        "tool_count": len(tools),
        "resource_count": len(res),
        "tool_failures": fail_tools,
        "resource_failures": fail_res,
    }


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--label", default="run")
    p.add_argument("--out", required=True)
    args = p.parse_args()
    report = await run(args.label)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    summary = summarize(report)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)

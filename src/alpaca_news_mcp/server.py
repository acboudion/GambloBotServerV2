"""FastMCP server wiring + lifespan with all background workers.

Notes on lifespan:
  - FastMCP's `lifespan=` constructor kwarg attaches an MCP-session-scoped
    context manager. It does NOT run at HTTP startup with `streamable_http_app()`.
  - We need our Alpaca WebSocket worker, REST backfill, and SQLite store to be
    initialized exactly once when the HTTP server starts, not once per MCP session.
  - Therefore we build our own Starlette parent app with a combined lifespan that
    runs (1) our app-level startup, then (2) FastMCP's StreamableHTTP session
    manager, and tears them down in reverse on shutdown.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime

from mcp.server.fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

from . import resources as resources_mod
from . import tools as tools_mod
from .alerts import AlertEngine
from .app_state import AppState, clear_app_state, get_app_state, set_app_state
from .config import Config
from .logging_utils import configure_logging, get_logger
from .rest_backfill import RestBackfillWorker
from .state import State
from .store import Store
from .stream import NewsStreamWorker

log = get_logger(__name__)


@asynccontextmanager
async def app_setup(config: Config) -> AsyncIterator[AppState]:
    """Initialize Alpaca workers, store, and shared state. Single global instance."""
    configure_logging(config.log_level)
    log.info("alpaca-news-mcp starting (safe config: %s)", config.safe_repr())

    store = await Store.open(config.storage_path)
    await store.init_schema()

    state = State(max_recent_articles=config.max_recent_articles_memory)
    state.set_interest_symbols(config.news_interest_symbols, "replace")

    alerts = AlertEngine(high_latency_alert_ms=config.high_latency_alert_ms)
    rest_backfill = RestBackfillWorker(config, store, state, alerts)
    NewsStreamWorker.reset_singleton()
    stream = NewsStreamWorker(
        config,
        store,
        state,
        alerts,
        gap_fill_callback=rest_backfill.gap_fill,
    )
    app_state = AppState(
        config=config,
        store=store,
        state=state,
        alerts=alerts,
        stream=stream,
        rest_backfill=rest_backfill,
    )
    set_app_state(app_state)

    if config.enable_rest_backfill:
        try:
            await rest_backfill.backfill_startup()
        except Exception as e:
            log.warning("startup backfill failed (continuing): %s", e)

    await stream.start()
    state.update_health(rest_backfill_enabled=config.enable_rest_backfill)

    pruner_task = asyncio.create_task(_retention_loop(app_state), name="retention")

    try:
        yield app_state
    finally:
        log.info("alpaca-news-mcp shutting down")
        pruner_task.cancel()
        try:
            await pruner_task
        except (asyncio.CancelledError, Exception):
            pass
        await stream.stop()
        await rest_backfill.close()
        await store.close()
        clear_app_state()
        NewsStreamWorker.reset_singleton()


async def _retention_loop(app_state: AppState) -> None:
    """Periodic retention pruner. Runs once at startup, then on the configured cadence."""
    first_run = True
    while True:
        try:
            if not first_run:
                await asyncio.sleep(app_state.config.retention_interval_seconds)
            first_run = False
            await app_state.store.prune_retention(
                event_days=app_state.config.event_retention_days,
                raw_event_days=app_state.config.raw_event_retention_days,
            )
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("retention pruning failed: %s", e)


def build_mcp(*, mcp_path: str = "/mcp") -> FastMCP:
    """Construct FastMCP with tools and resources registered.

    NOTE: We do NOT pass `lifespan=` to FastMCP because that is an MCP-session
    lifespan, not an HTTP-server lifespan. App-level setup runs in `app_setup`.
    """
    mcp = FastMCP(
        name="alpaca-news-mcp",
        instructions=(
            "Real-time Alpaca news ingestion with bounded read-only MCP tools. "
            "Single Alpaca WS connection per container."
        ),
        json_response=True,
        stateless_http=True,
        streamable_http_path=mcp_path,
    )
    tools_mod.register(mcp)
    resources_mod.register(mcp)
    return mcp


def _seconds_since(iso: str | None) -> float | None:
    """Seconds elapsed since an ISO-8601 UTC timestamp, or None if absent
    or unparseable. Used by /healthz so the value is queryable without
    forcing the operator to do timezone math."""
    if not iso:
        return None
    try:
        ts = datetime.fromisoformat(iso)
    except (TypeError, ValueError):
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return (datetime.now(UTC) - ts).total_seconds()


async def healthz(request: Request) -> JSONResponse:
    """Plain JSON healthcheck for Docker — no secrets, minimal info.

    `ok` stays decoupled from `stream_connected`/`authenticated` so that
    transient WS reconnects do not flap the container as unhealthy. To
    detect a stalled stream, watch `seconds_since_last_message`.
    """
    try:
        app = get_app_state()
        h = app.state.snapshot_health()
        body = {
            "ok": True,
            "stream_connected": h.connected,
            "authenticated": h.authenticated,
            "subscription_mode": h.subscription_mode,
            "last_message_at": h.last_message_at,
            "last_article_at": h.last_article_at,
            "seconds_since_last_message": _seconds_since(h.last_message_at),
            "seconds_since_last_article": _seconds_since(h.last_article_at),
            "connection_limit_blocked": h.connection_limit_blocked,
            "entitlement_error": h.entitlement_error,
        }
        return JSONResponse(body)
    except RuntimeError:
        return JSONResponse({"ok": False, "reason": "starting"}, status_code=503)


def build_starlette_app(mcp: FastMCP, config: Config) -> Starlette:
    """Build the parent Starlette app.

    Runs both our app-level setup AND FastMCP's StreamableHTTP session manager
    inside a single combined lifespan. Adds /healthz alongside FastMCP's /mcp
    route.
    """
    # Build FastMCP's app to lazy-init the session manager + the ASGI handler.
    mcp_app = mcp.streamable_http_app()
    session_manager = mcp._session_manager
    if session_manager is None:
        raise RuntimeError("FastMCP session manager was not initialized")

    @asynccontextmanager
    async def combined_lifespan(_parent: Starlette) -> AsyncIterator[None]:
        async with app_setup(config):
            async with session_manager.run():
                yield

    # Mount the entire FastMCP app under "/" so its routing handles /mcp.
    return Starlette(
        debug=False,
        routes=[
            Route("/healthz", healthz, methods=["GET"]),
            Mount("/", app=mcp_app),
        ],
        lifespan=combined_lifespan,
    )

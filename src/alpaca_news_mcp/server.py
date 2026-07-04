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

from . import market_tools as market_tools_mod
from . import resources as resources_mod
from . import tools as tools_mod
from .alerts import AlertEngine
from .app_state import AppState, clear_app_state, get_app_state, set_app_state
from .config import Config
from .logging_utils import configure_logging, get_logger
from .market_client import MarketDataClient
from .market_store import MarketStore
from .rest_backfill import RestBackfillWorker
from .state import State
from .stock_stream import StockStreamWorker
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

    state = State()
    state.set_interest_symbols(config.news_interest_symbols, "replace")

    alerts = AlertEngine(
        high_latency_alert_ms=config.high_latency_alert_ms,
        halt_alert_dedup_seconds=config.halt_alert_dedup_seconds,
        keywords_file=config.alert_keywords_file or None,
        rate_limit_per_symbol_hour=config.alert_rate_limit_per_symbol_hour,
    )
    rest_backfill = RestBackfillWorker(config, store, state, alerts)
    NewsStreamWorker.reset_singleton()
    stream = NewsStreamWorker(
        config,
        store,
        state,
        alerts,
        gap_fill_callback=rest_backfill.gap_fill,
    )

    market_client = MarketDataClient(config)
    market_store: MarketStore | None = None
    stock_stream: StockStreamWorker | None = None
    if config.enable_stock_stream:
        market_store = await MarketStore.open(config.market_storage_path)
        await market_store.init_schema()
        StockStreamWorker.reset_singleton()
        stock_stream = StockStreamWorker(
            config,
            store,
            market_store,
            state,
            alerts,
            market_client=market_client,
        )
        await stock_stream.restore_watchlist()

    app_state = AppState(
        config=config,
        store=store,
        state=state,
        alerts=alerts,
        stream=stream,
        rest_backfill=rest_backfill,
        market_store=market_store,
        stock_stream=stock_stream,
        market_client=market_client,
    )
    set_app_state(app_state)

    backfill_task: asyncio.Task[None] | None = None
    if config.enable_rest_backfill:
        # Background task: a long backfill window must not delay stream
        # startup or keep /healthz returning 503 (docker healthchecks would
        # kill the container). Article upserts dedup, so running it
        # concurrently with the live stream is safe.
        async def _startup_backfill() -> None:
            try:
                await rest_backfill.backfill_startup()
            except Exception as e:
                log.warning("startup backfill failed (continuing): %s", e)

        backfill_task = asyncio.create_task(
            _startup_backfill(), name="startup-backfill"
        )

    await stream.start()
    if stock_stream is not None:
        await stock_stream.start()
    state.update_health(rest_backfill_enabled=config.enable_rest_backfill)

    pruner_task = asyncio.create_task(_retention_loop(app_state), name="retention")
    market_pruner_task: asyncio.Task[None] | None = None
    if market_store is not None:
        market_pruner_task = asyncio.create_task(
            _market_retention_loop(app_state), name="market-retention"
        )

    try:
        yield app_state
    finally:
        log.info("alpaca-news-mcp shutting down")
        for task in (backfill_task, pruner_task, market_pruner_task):
            if task is None:
                continue
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        if stock_stream is not None:
            await stock_stream.stop()
        await stream.stop()
        await rest_backfill.close()
        await market_client.close()
        if market_store is not None:
            await market_store.close()
        await store.close()
        clear_app_state()
        NewsStreamWorker.reset_singleton()
        StockStreamWorker.reset_singleton()


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


async def _market_retention_loop(app_state: AppState) -> None:
    """Aggressive tick retention for the market DB (defaults: every 15 min)."""
    assert app_state.market_store is not None
    while True:
        try:
            await asyncio.sleep(app_state.config.market_retention_interval_seconds)
            deleted = await app_state.market_store.prune(
                tick_retention_minutes=app_state.config.stock_tick_retention_minutes,
                bar_retention_days=app_state.config.stock_bar_retention_days,
                status_retention_days=app_state.config.status_retention_days,
            )
            if any(deleted.values()):
                log.info("market retention pruned: %s", deleted)
        except asyncio.CancelledError:
            return
        except Exception as e:
            log.warning("market retention pruning failed: %s", e)


def build_mcp(*, mcp_path: str = "/mcp") -> FastMCP:
    """Construct FastMCP with tools and resources registered.

    NOTE: We do NOT pass `lifespan=` to FastMCP because that is an MCP-session
    lifespan, not an HTTP-server lifespan. App-level setup runs in `app_setup`.
    """
    mcp = FastMCP(
        name="alpaca-news-mcp",
        instructions=(
            "Real-time Alpaca market data: news stream + v2 stock stream "
            "(trades/quotes/bars/halts) with bounded read-only MCP tools. "
            "Prefer the delta-cursor tools (get_news_since, get_alerts_since, "
            "get_bars_since) for polling loops. One Alpaca WS per endpoint per "
            "container; no trading endpoints."
        ),
        json_response=True,
        stateless_http=True,
        streamable_http_path=mcp_path,
    )
    tools_mod.register(mcp)
    resources_mod.register(mcp)
    market_tools_mod.register(mcp)
    market_tools_mod.register_resources(mcp)
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
            "auth_failed": h.auth_failed,
            "subscription_mode": h.subscription_mode,
            "last_message_at": h.last_message_at,
            "last_article_at": h.last_article_at,
            "seconds_since_last_message": _seconds_since(h.last_message_at),
            "seconds_since_last_article": _seconds_since(h.last_article_at),
            "connection_limit_blocked": h.connection_limit_blocked,
            "entitlement_error": h.entitlement_error,
        }
        if app.stock_stream is not None:
            sh = app.state.snapshot_health("stocks")
            body["stocks"] = {
                "stream_connected": sh.connected,
                "authenticated": sh.authenticated,
                "auth_failed": sh.auth_failed,
                "last_message_at": sh.last_message_at,
                "seconds_since_last_message": _seconds_since(sh.last_message_at),
                "connection_limit_blocked": sh.connection_limit_blocked,
                "entitlement_error": sh.entitlement_error,
                "articles_dropped": sh.articles_dropped,
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

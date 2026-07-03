"""Alpaca REST client for market context data.

Two hosts:
  - data:    https://data.alpaca.markets       (bars, snapshots, screener, news...)
  - trading: https://paper-api.alpaca.markets  (GET /v2/clock and /v2/calendar ONLY —
             read-only; this codebase never touches order/position endpoints)

Every read goes through a small TTL cache so a chatty LLM harness cannot
hammer Alpaca (the trading host in particular is limited to 200 req/min).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import httpx

from .config import Config
from .logging_utils import get_logger

log = get_logger(__name__)

BARS_PATH = "/v2/stocks/bars"
CLOCK_PATH = "/v2/clock"
CALENDAR_PATH = "/v2/calendar"
MOST_ACTIVES_PATH = "/v1beta1/screener/stocks/most-actives"
MOVERS_PATH = "/v1beta1/screener/stocks/movers"
SNAPSHOTS_PATH = "/v2/stocks/snapshots"
LATEST_PATHS = {
    "trades": "/v2/stocks/trades/latest",
    "quotes": "/v2/stocks/quotes/latest",
    "bars": "/v2/stocks/bars/latest",
}
CORPORATE_ACTIONS_PATH = "/v1/corporate-actions"

MAX_BARS_PAGES = 40


class MarketDataError(RuntimeError):
    """A REST request that callers must not silently treat as success."""


class TTLCache:
    def __init__(self, maxsize: int = 256) -> None:
        self._data: dict[str, tuple[float, Any]] = {}
        self._maxsize = maxsize

    def get(self, key: str) -> Any | None:
        entry = self._data.get(key)
        if entry is None:
            return None
        expires, value = entry
        if time.monotonic() >= expires:
            self._data.pop(key, None)
            return None
        return value

    def set(self, key: str, value: Any, ttl: float) -> None:
        if len(self._data) >= self._maxsize:
            # Drop the soonest-to-expire entry; cheap and good enough here.
            oldest = min(self._data, key=lambda k: self._data[k][0])
            self._data.pop(oldest, None)
        self._data[key] = (time.monotonic() + ttl, value)


class MarketDataClient:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._cache = TTLCache()
        self._lock = asyncio.Lock()
        self._data_client: httpx.AsyncClient | None = None
        self._trading_client: httpx.AsyncClient | None = None

    def _headers(self) -> dict[str, str]:
        return {
            "APCA-API-KEY-ID": self._config.alpaca_api_key,
            "APCA-API-SECRET-KEY": self._config.alpaca_secret_key,
            "Accept": "application/json",
        }

    async def _ensure_clients(self) -> tuple[httpx.AsyncClient, httpx.AsyncClient]:
        async with self._lock:
            if self._data_client is None or self._data_client.is_closed:
                self._data_client = httpx.AsyncClient(
                    base_url=self._config.alpaca_data_base_url,
                    headers=self._headers(),
                    timeout=httpx.Timeout(20.0, connect=10.0),
                )
            if self._trading_client is None or self._trading_client.is_closed:
                self._trading_client = httpx.AsyncClient(
                    base_url=self._config.alpaca_trading_base_url,
                    headers=self._headers(),
                    timeout=httpx.Timeout(20.0, connect=10.0),
                )
            return self._data_client, self._trading_client

    async def close(self) -> None:
        async with self._lock:
            for client in (self._data_client, self._trading_client):
                if client is not None and not client.is_closed:
                    await client.aclose()
            self._data_client = None
            self._trading_client = None

    async def _get_json(
        self,
        *,
        host: str,
        path: str,
        params: dict[str, Any] | None = None,
        cache_key: str | None = None,
        ttl: float = 0,
    ) -> dict[str, Any] | None:
        """GET with TTL caching and a single Retry-After honoring 429 retry.
        Returns None on failure (callers surface a structured error)."""
        if cache_key and ttl > 0:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached
        data_client, trading_client = await self._ensure_clients()
        client = trading_client if host == "trading" else data_client
        for attempt in (1, 2):
            try:
                resp = await client.get(path, params=params)
            except httpx.HTTPError as e:
                log.warning("market client request failed %s: %s", path, e)
                return None
            if resp.status_code == 429 and attempt == 1:
                retry_after = float(resp.headers.get("Retry-After", "1") or 1)
                await asyncio.sleep(min(retry_after, 10.0))
                continue
            if resp.status_code >= 400:
                log.warning(
                    "market client http %d %s body=%s",
                    resp.status_code,
                    path,
                    resp.text[:200],
                )
                return None
            payload = resp.json()
            if cache_key and ttl > 0:
                self._cache.set(cache_key, payload, ttl)
            return payload
        return None

    # ---- clock / calendar --------------------------------------------------------

    async def clock(self) -> dict[str, Any] | None:
        return await self._get_json(
            host="trading",
            path=CLOCK_PATH,
            cache_key="clock",
            ttl=self._config.market_cache_clock_ttl,
        )

    async def is_market_open(self) -> bool | None:
        """Cached market-open flag; None when the clock is unavailable."""
        c = await self.clock()
        if c is None:
            return None
        is_open = c.get("is_open")
        return bool(is_open) if is_open is not None else None

    async def calendar(self, *, start: str, end: str) -> list[dict[str, Any]] | None:
        payload = await self._get_json(
            host="trading",
            path=CALENDAR_PATH,
            params={"start": start, "end": end},
            cache_key=f"calendar:{start}:{end}",
            ttl=self._config.market_cache_calendar_ttl,
        )
        if payload is None:
            return None
        return payload if isinstance(payload, list) else payload.get("calendar")

    # ---- screener --------------------------------------------------------------------

    async def most_actives(self, *, by: str = "volume", top: int = 10) -> dict[str, Any] | None:
        return await self._get_json(
            host="data",
            path=MOST_ACTIVES_PATH,
            params={"by": by, "top": top},
            cache_key=f"most_actives:{by}:{top}",
            ttl=self._config.market_cache_screener_ttl,
        )

    async def movers(self, *, top: int = 10) -> dict[str, Any] | None:
        return await self._get_json(
            host="data",
            path=MOVERS_PATH,
            params={"top": top},
            cache_key=f"movers:{top}",
            ttl=self._config.market_cache_screener_ttl,
        )

    # ---- snapshots / latest -----------------------------------------------------------

    async def snapshots(self, symbols: list[str]) -> dict[str, Any] | None:
        joined = ",".join(sorted({s.upper() for s in symbols}))
        return await self._get_json(
            host="data",
            path=SNAPSHOTS_PATH,
            params={"symbols": joined},
            cache_key=f"snapshots:{joined}",
            ttl=self._config.market_cache_snapshot_ttl,
        )

    async def latest(self, kind: str, symbols: list[str]) -> dict[str, Any] | None:
        """kind: trades|quotes|bars. Returns the raw Alpaca payload
        ({'trades': {SYM: {...}}} etc.)."""
        path = LATEST_PATHS.get(kind)
        if path is None:
            raise ValueError(f"invalid latest kind: {kind}")
        joined = ",".join(sorted({s.upper() for s in symbols}))
        return await self._get_json(
            host="data",
            path=path,
            params={"symbols": joined},
            cache_key=f"latest:{kind}:{joined}",
            ttl=self._config.market_cache_latest_ttl,
        )

    # ---- corporate actions ---------------------------------------------------------------

    async def corporate_actions(
        self,
        *,
        symbols: list[str] | None = None,
        types: list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any] | None:
        params: dict[str, Any] = {"limit": limit}
        if symbols:
            params["symbols"] = ",".join(sorted({s.upper() for s in symbols}))
        if types:
            params["types"] = ",".join(types)
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        cache_key = "ca:" + ":".join(
            str(params.get(k, "")) for k in ("symbols", "types", "start", "end", "limit")
        )
        return await self._get_json(
            host="data",
            path=CORPORATE_ACTIONS_PATH,
            params=params,
            cache_key=cache_key,
            ttl=self._config.market_cache_corporate_actions_ttl,
        )

    # ---- historical bars (gap-fill + tool fallback) ---------------------------------

    async def bars(
        self,
        symbols: list[str],
        *,
        start_iso: str,
        end_iso: str | None = None,
        timeframe: str = "1Min",
        limit_per_page: int = 1000,
        feed: str | None = None,
    ) -> dict[str, list[dict[str, Any]]]:
        """Paginated /v2/stocks/bars for multiple symbols. Returns
        {SYMBOL: [bar dicts with t/o/h/l/c/v/n/vw]}. Not cached (gap-fill use).

        `feed` defaults to the configured stream feed so backfilled bars come
        from the same feed the WebSocket ingests — otherwise Alpaca picks the
        account's best feed and gap-fill could mix feeds within one series.

        Raises MarketDataError when any page fails — a partial result must not
        look like a completed backfill, or the gap-fill retry/alert path in
        BaseStreamWorker never runs and missing bars stay missing silently.
        """
        out: dict[str, list[dict[str, Any]]] = {}
        if not symbols:
            return out
        params: dict[str, Any] = {
            "symbols": ",".join(sorted({s.upper() for s in symbols})),
            "timeframe": timeframe,
            "start": start_iso,
            "limit": limit_per_page,
            "feed": feed or self._config.alpaca_stock_feed,
        }
        if end_iso:
            params["end"] = end_iso
        pages = 0
        page_token: str | None = None
        while pages < MAX_BARS_PAGES:
            if page_token:
                params["page_token"] = page_token
            else:
                params.pop("page_token", None)
            payload = await self._get_json(host="data", path=BARS_PATH, params=params)
            if payload is None:
                raise MarketDataError(
                    f"bars request failed after {pages} page(s) "
                    f"(timeframe={timeframe}, start={start_iso})"
                )
            for sym, bars in (payload.get("bars") or {}).items():
                out.setdefault(sym, []).extend(bars or [])
            pages += 1
            page_token = payload.get("next_page_token") or None
            if not page_token:
                break
        if page_token:
            # Page cap hit with more data remaining (very long disconnect with
            # a large watchlist). Treat as incomplete so the caller retries /
            # alerts rather than silently missing the tail.
            raise MarketDataError(
                f"bars pagination truncated at {pages} pages with data remaining "
                f"(timeframe={timeframe}, start={start_iso})"
            )
        return out

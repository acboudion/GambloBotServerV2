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

MAX_BARS_PAGES = 40


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

    # ---- historical bars (gap-fill + tool fallback) ---------------------------------

    async def bars(
        self,
        symbols: list[str],
        *,
        start_iso: str,
        end_iso: str | None = None,
        timeframe: str = "1Min",
        limit_per_page: int = 1000,
    ) -> dict[str, list[dict[str, Any]]]:
        """Paginated /v2/stocks/bars for multiple symbols. Returns
        {SYMBOL: [bar dicts with t/o/h/l/c/v/n/vw]}. Not cached (gap-fill use)."""
        out: dict[str, list[dict[str, Any]]] = {}
        if not symbols:
            return out
        params: dict[str, Any] = {
            "symbols": ",".join(sorted({s.upper() for s in symbols})),
            "timeframe": timeframe,
            "start": start_iso,
            "limit": limit_per_page,
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
                break
            for sym, bars in (payload.get("bars") or {}).items():
                out.setdefault(sym, []).extend(bars or [])
            pages += 1
            page_token = payload.get("next_page_token") or None
            if not page_token:
                break
        return out

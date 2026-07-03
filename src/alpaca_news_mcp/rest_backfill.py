"""Alpaca News REST backfill worker (startup, gap-fill, manual)."""

from __future__ import annotations

import asyncio
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

import httpx

from .alerts import AlertEngine
from .config import Config
from .logging_utils import get_logger
from .normalize import NormalizationError, normalize_news_message
from .state import State
from .store import Store

log = get_logger(__name__)

REST_NEWS_PATH = "/v1beta1/news"

IngestStatus = Literal["new", "updated", "duplicate", "failed"]

MAX_BACKFILL_PAGES = 200
MAX_429_RETRIES = 5


class BackfillError(RuntimeError):
    """A backfill that must not be treated as complete (used by reconnect
    gap-fills so BaseStreamWorker's retry/alert path engages)."""


def _empty_counts(*, skipped: int = 0) -> dict[str, int]:
    """Canonical zero-shape so callers can destructure regardless of branch."""
    out: dict[str, int] = {
        "ingested": 0,
        "new": 0,
        "updated": 0,
        "duplicate": 0,
        "failed": 0,
        "pages": 0,
    }
    if skipped:
        out["skipped"] = skipped
    return out


class RestBackfillWorker:
    def __init__(
        self,
        config: Config,
        store: Store,
        state: State,
        alerts: AlertEngine,
    ) -> None:
        self._config = config
        self._store = store
        self._state = state
        self._alerts = alerts
        self._client: httpx.AsyncClient | None = None
        self._client_lock = asyncio.Lock()
        self._run_lock = asyncio.Lock()

    async def _ensure_client(self) -> httpx.AsyncClient:
        async with self._client_lock:
            if self._client is None or self._client.is_closed:
                self._client = httpx.AsyncClient(
                    base_url=self._config.alpaca_data_base_url,
                    headers={
                        "APCA-API-KEY-ID": self._config.alpaca_api_key,
                        "APCA-API-SECRET-KEY": self._config.alpaca_secret_key,
                        "Accept": "application/json",
                    },
                    timeout=httpx.Timeout(20.0, connect=10.0),
                )
            return self._client

    async def close(self) -> None:
        async with self._client_lock:
            if self._client is not None and not self._client.is_closed:
                await self._client.aclose()
            self._client = None

    async def backfill_startup(self) -> dict[str, int]:
        if not self._config.enable_rest_backfill:
            log.info("rest_backfill disabled at startup")
            return _empty_counts()
        start = datetime.now(UTC) - timedelta(
            minutes=self._config.backfill_lookback_minutes
        )
        return await self._run(start_iso=start.isoformat(), reason="startup")

    async def gap_fill(self, since_iso: str | None = None) -> dict[str, int]:
        if not self._config.enable_rest_backfill:
            return _empty_counts()
        if since_iso is None:
            since_iso = self._state.last_seen_updated_at
        if since_iso is None:
            start = datetime.now(UTC) - timedelta(
                minutes=self._config.backfill_lookback_minutes
            )
            since_iso = start.isoformat()
        # Apply overlap window
        try:
            since_dt = datetime.fromisoformat(since_iso)
        except ValueError:
            since_dt = datetime.now(UTC) - timedelta(
                minutes=self._config.backfill_lookback_minutes
            )
        adjusted = since_dt - timedelta(seconds=self._config.rest_backfill_overlap_seconds)
        # Gap-fill recovers a disconnect window: a partial or failed run must
        # raise (BackfillError) so the caller's retry/alert path engages —
        # returning partial counts here would silently drop articles.
        return await self._run(
            start_iso=adjusted.isoformat(), reason="gap_fill", raise_on_failure=True
        )

    async def manual(self, minutes: int) -> dict[str, int]:
        start = datetime.now(UTC) - timedelta(minutes=minutes)
        return await self._run(start_iso=start.isoformat(), reason="manual")

    async def _run(
        self, *, start_iso: str, reason: str, raise_on_failure: bool = False
    ) -> dict[str, int]:
        if self._run_lock.locked():
            log.info("rest_backfill already running; skipping reason=%s", reason)
            if raise_on_failure:
                # The concurrent run may not cover this gap window — retry.
                raise BackfillError("backfill already running; gap window not covered")
            return _empty_counts(skipped=1)
        failure: str | None = None
        async with self._run_lock:
            client = await self._ensure_client()
            params: dict[str, Any] = {
                "start": start_iso,
                "sort": "asc",
                "limit": 50,
            }
            if self._config.rest_include_content:
                params["include_content"] = "true"
            if self._config.rest_exclude_contentless:
                params["exclude_contentless"] = "true"

            counts: Counter[str] = Counter()
            pages = 0
            page_token: str | None = None
            backoff = 1.0
            max_pages = MAX_BACKFILL_PAGES
            max_429_retries = MAX_429_RETRIES
            retries_429 = 0

            while pages < max_pages:
                if page_token:
                    params["page_token"] = page_token
                else:
                    params.pop("page_token", None)

                try:
                    resp = await client.get(REST_NEWS_PATH, params=params)
                except httpx.HTTPError as e:
                    log.warning("rest_backfill request failed: %s", e)
                    failure = f"request failed: {e}"
                    break

                if resp.status_code == 429:
                    retries_429 += 1
                    if retries_429 > max_429_retries:
                        log.warning(
                            "rest_backfill aborting after %d consecutive 429s reason=%s",
                            retries_429,
                            reason,
                        )
                        self._state.update_health(last_error="rest 429 retry budget exhausted")
                        failure = "429 retry budget exhausted"
                        break
                    retry_after = float(resp.headers.get("Retry-After", "0") or 0)
                    wait = retry_after if retry_after > 0 else backoff
                    backoff = min(backoff * 2, 60.0)
                    log.warning(
                        "rest_backfill 429 (%d/%d), sleeping %.1fs reason=%s",
                        retries_429,
                        max_429_retries,
                        wait,
                        reason,
                    )
                    await asyncio.sleep(wait)
                    continue
                retries_429 = 0
                if resp.status_code == 403:
                    log.warning("rest_backfill 403 entitlement_error reason=%s", reason)
                    self._state.update_health(entitlement_error=True, last_error="rest 403 forbidden")
                    failure = "403 entitlement error"
                    break
                if resp.status_code >= 400:
                    log.warning(
                        "rest_backfill http %d reason=%s body=%s",
                        resp.status_code,
                        reason,
                        resp.text[:200],
                    )
                    failure = f"http {resp.status_code}"
                    break

                data = resp.json()
                news = data.get("news") or []
                for item in news:
                    counts[await self._ingest_one(item)] += 1
                pages += 1
                page_token = data.get("next_page_token") or None
                backoff = max(1.0, backoff / 2)
                if not page_token:
                    break

            if failure is None and page_token:
                # Page cap reached with data remaining — the tail was skipped.
                failure = f"page cap ({max_pages}) reached with pages remaining"
                log.warning("rest_backfill %s reason=%s", failure, reason)

            if raise_on_failure and failure is not None:
                raise BackfillError(f"gap-fill incomplete ({failure})")

            ingested = counts["new"] + counts["updated"] + counts["duplicate"]
            log.info(
                "rest_backfill complete reason=%s pages=%d "
                "ingested=%d new=%d updated=%d duplicate=%d failed=%d",
                reason,
                pages,
                ingested,
                counts["new"],
                counts["updated"],
                counts["duplicate"],
                counts["failed"],
            )
            return {
                "ingested": ingested,
                "new": counts["new"],
                "updated": counts["updated"],
                "duplicate": counts["duplicate"],
                "failed": counts["failed"],
                "pages": pages,
            }

    async def _ingest_one(self, payload: dict[str, Any]) -> IngestStatus:
        try:
            normalized = normalize_news_message(payload)
        except NormalizationError as e:
            log.warning("rest_backfill normalization failed: %s", e)
            return "failed"
        try:
            result = await self._store.upsert_article(normalized, source_kind="rest")
        except Exception as e:
            log.exception("rest_backfill upsert failed: %s", e)
            return "failed"
        self._state.record_article(result.article, was_new=result.was_new)
        # Run alerts only for new or version-inserted articles. Suppress
        # high-latency alerts on REST ingest: `latency_ms` is computed against
        # `created_at`, so historical backfill items would always trip the
        # threshold and drown out genuine real-time pipeline alerts.
        if result.was_new or result.version_inserted:
            interest = self._state.get_interest_symbols()
            for alert in self._alerts.evaluate_article(
                result.article,
                interest_symbols=interest,
                suppress_high_latency=True,
            ):
                inserted = await self._store.record_alert(
                    alert,
                    raw_json=normalized.raw_json,
                    content_hash=normalized.content_hash,
                )
                if inserted:
                    self._state.record_alert(alert)
        if result.was_new:
            return "new"
        if result.version_inserted:
            return "updated"
        return "duplicate"

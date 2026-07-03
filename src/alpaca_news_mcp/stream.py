"""Single-connection Alpaca News WebSocket worker.

Owns the *only* Alpaca news WebSocket connection in the process. Maintains a bounded
asyncio.Queue between the recv loop and a persister coroutine; persistence + alert
scoring happens on the persister so the recv loop never blocks.
"""

from __future__ import annotations

import asyncio
import random
import threading
from datetime import UTC, datetime
from typing import Any, Literal

import orjson
import websockets
from websockets.exceptions import ConnectionClosed

from .alerts import AlertEngine
from .config import Config
from .logging_utils import get_logger
from .models import SubscriptionState
from .normalize import NormalizationError, normalize_news_message
from .state import State
from .store import Store

SubscriptionMode = Literal["wildcard", "fallback", "rest_only", "disconnected"]

log = get_logger(__name__)

AUTH_TIMEOUT_SECONDS = 10.0
SUBSCRIPTION_TIMEOUT_SECONDS = 10.0
# Max articles persisted per batch commit. News volume is low; this only
# matters during backfill floods and reconnect bursts.
PERSIST_BATCH_MAX = 64


class SingletonViolation(RuntimeError):
    pass


class NewsStreamWorker:
    _instance_lock = threading.Lock()
    _instance: NewsStreamWorker | None = None

    def __init__(
        self,
        config: Config,
        store: Store,
        state: State,
        alerts: AlertEngine,
        *,
        gap_fill_callback=None,
    ) -> None:
        with NewsStreamWorker._instance_lock:
            if NewsStreamWorker._instance is not None:
                raise SingletonViolation(
                    "NewsStreamWorker already constructed; only one Alpaca news WebSocket allowed per process"
                )
            NewsStreamWorker._instance = self

        self._config = config
        self._store = store
        self._state = state
        self._alerts = alerts
        self._gap_fill = gap_fill_callback

        self._queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(
            maxsize=config.queue_maxsize
        )
        self._stop_event = asyncio.Event()
        self._main_task: asyncio.Task[None] | None = None
        self._persister_task: asyncio.Task[None] | None = None
        self._fatal_auth = False

    @classmethod
    def reset_singleton(cls) -> None:
        """Test-only: clear the singleton guard so a new instance can be created."""
        with cls._instance_lock:
            cls._instance = None

    async def start(self) -> None:
        if self._main_task is not None:
            return
        self._persister_task = asyncio.create_task(
            self._persister_loop(), name="news-persister"
        )
        self._main_task = asyncio.create_task(self._run(), name="news-stream")

    async def stop(self) -> None:
        self._stop_event.set()
        for t in (self._main_task, self._persister_task):
            if t is not None:
                t.cancel()
        for t in (self._main_task, self._persister_task):
            if t is not None:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        self._main_task = None
        self._persister_task = None

    async def _run(self) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            if self._fatal_auth:
                log.error("stream worker not retrying due to fatal auth; sleeping")
                await asyncio.sleep(60)
                continue
            attempt += 1
            self._state.update_health(connection_attempts=attempt, last_error=None)
            try:
                await self._connect_and_run()
            except SingletonViolation:
                raise
            except Exception as e:
                log.exception("stream loop crashed: %s", e)
                self._state.update_health(connected=False, last_error=str(e))

            if self._stop_event.is_set():
                break

            # exponential backoff + jitter
            base_min = self._config.reconnect_min_seconds
            base_max = self._config.reconnect_max_seconds
            wait = min(base_max, base_min * (2 ** min(attempt - 1, 6)))
            wait = wait * (0.7 + 0.6 * random.random())
            log.info("reconnecting in %.1fs (attempt=%d)", wait, attempt)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait)
            except TimeoutError:
                pass

            if self._gap_fill is not None and not self._stop_event.is_set():
                try:
                    await self._gap_fill()
                except Exception as e:
                    log.warning("gap fill failed: %s", e)

            self._state.update_health(
                reconnect_count=self._state.snapshot_health().reconnect_count + 1
            )

    async def _connect_and_run(self) -> None:
        url = self._config.alpaca_news_stream_url
        log.info("connecting to alpaca news stream")
        headers = [
            ("APCA-API-KEY-ID", self._config.alpaca_api_key),
            ("APCA-API-SECRET-KEY", self._config.alpaca_secret_key),
        ]
        async with websockets.connect(
            url,
            additional_headers=headers,
            ping_interval=20,
            ping_timeout=20,
            max_size=2**22,
        ) as ws:
            self._state.update_health(connected=True, last_error=None)
            try:
                authed = await self._await_auth(ws)
                if not authed:
                    # Fall back to message auth.
                    authed = await self._auth_via_message(ws)
                if not authed:
                    self._state.update_health(authenticated=False, last_error="auth failed")
                    return

                self._state.update_health(authenticated=True)
                await self._subscribe(ws)
                await self._receive_loop(ws)
            finally:
                self._state.update_health(connected=False, authenticated=False)

    async def _await_auth(self, ws: Any) -> bool:
        """Wait for connected + authenticated success messages from header-based auth."""
        deadline = asyncio.get_event_loop().time() + AUTH_TIMEOUT_SECONDS
        connected = False
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return False
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except TimeoutError:
                return False
            self._state.update_health(last_message_at=datetime.now(UTC).isoformat())
            try:
                items = self._parse_frame(raw)
            except orjson.JSONDecodeError:
                continue
            for item in items:
                t = item.get("T")
                msg = item.get("msg") or ""
                code = item.get("code")
                if t == "success" and msg == "connected":
                    connected = True
                elif t == "success" and msg == "authenticated":
                    return True
                elif t == "error":
                    await self._handle_error(item)
                    if code == 402:
                        # Already-authenticated quirks — message auth attempt may still work
                        return False
                    if code in (406,):
                        return False
            if connected and asyncio.get_event_loop().time() > deadline - 1:
                return False

    async def _auth_via_message(self, ws: Any) -> bool:
        log.info("attempting message-based authentication")
        try:
            await ws.send(
                orjson.dumps(
                    {
                        "action": "auth",
                        "key": self._config.alpaca_api_key,
                        "secret": self._config.alpaca_secret_key,
                    }
                ).decode("utf-8")
            )
        except ConnectionClosed:
            return False
        deadline = asyncio.get_event_loop().time() + AUTH_TIMEOUT_SECONDS
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return False
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except TimeoutError:
                return False
            self._state.update_health(last_message_at=datetime.now(UTC).isoformat())
            try:
                items = self._parse_frame(raw)
            except orjson.JSONDecodeError:
                continue
            for item in items:
                t = item.get("T")
                msg = item.get("msg") or ""
                if t == "success" and msg == "authenticated":
                    return True
                if t == "error":
                    await self._handle_error(item)
                    return False

    async def _subscribe(self, ws: Any) -> None:
        mode_str = self._config.news_subscription_mode
        mode: SubscriptionMode = "wildcard" if mode_str == "wildcard" else "fallback"
        if mode == "wildcard":
            requested = {"news": ["*"]}
        else:
            symbols = self._config.news_fallback_symbols or ["AAPL", "MSFT", "NVDA"]
            requested = {"news": symbols}
        await ws.send(
            orjson.dumps({"action": "subscribe", **requested}).decode("utf-8")
        )
        self._state.subscription_state = SubscriptionState(
            requested=requested, mode=mode, last_ack_at=None
        )
        self._state.update_health(
            requested_subscription=requested, subscription_mode=mode
        )

        deadline = asyncio.get_event_loop().time() + SUBSCRIPTION_TIMEOUT_SECONDS
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                log.warning("subscription ack timeout")
                return
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            except TimeoutError:
                return
            self._state.update_health(last_message_at=datetime.now(UTC).isoformat())
            try:
                items = self._parse_frame(raw)
            except orjson.JSONDecodeError:
                continue
            ack_received = False
            for item in items:
                t = item.get("T")
                if t == "subscription":
                    ack = {k: v for k, v in item.items() if k != "T"}
                    self._state.subscription_state = SubscriptionState(
                        requested=requested,
                        acknowledged=ack,
                        mode=mode,
                        last_ack_at=datetime.now(UTC).isoformat(),
                    )
                    # A successful ack clears any stale entitlement flag — e.g.
                    # after a wildcard 409 → fallback ack, monitoring shouldn't
                    # keep reporting an entitlement failure indefinitely.
                    self._state.update_health(
                        acknowledged_subscription=ack, entitlement_error=False
                    )
                    ack_received = True
                elif t == "n":
                    # Alpaca can batch articles in the same frame as the ack;
                    # don't drop them — hand them off to the persister queue.
                    await self._enqueue_article(item)
                elif t == "error":
                    await self._handle_error(item)
                    # Only entitlement rejection (409) should downgrade
                    # wildcard → fallback. Transient/unrelated errors must
                    # not permanently narrow coverage for this session.
                    if (
                        item.get("code") == 409
                        and mode == "wildcard"
                        and self._config.news_fallback_symbols
                    ):
                        log.info("falling back to symbol-list subscription after entitlement rejection")
                        mode = "fallback"
                        requested = {"news": self._config.news_fallback_symbols}
                        await ws.send(
                            orjson.dumps(
                                {"action": "subscribe", **requested}
                            ).decode("utf-8")
                        )
                        self._state.subscription_state = SubscriptionState(
                            requested=requested,
                            mode=mode,
                            last_ack_at=None,
                        )
                        self._state.update_health(
                            requested_subscription=requested, subscription_mode=mode
                        )
            if ack_received:
                return

    async def _receive_loop(self, ws: Any) -> None:
        while not self._stop_event.is_set():
            try:
                raw = await ws.recv()
            except ConnectionClosed as e:
                log.info("websocket closed: %s", e)
                self._state.update_health(connected=False)
                return
            try:
                items = self._parse_frame(raw)
            except orjson.JSONDecodeError:
                await self._store.record_raw_event(
                    endpoint="news_ws",
                    message_type="malformed",
                    raw_json=str(raw)[:4000],
                )
                continue

            now_iso = datetime.now(UTC).isoformat()
            self._state.update_health(last_message_at=now_iso)
            for item in items:
                t = item.get("T")
                if t == "n":
                    await self._enqueue_article(item)
                elif t == "subscription":
                    ack = {k: v for k, v in item.items() if k != "T"}
                    self._state.subscription_state = SubscriptionState(
                        requested=self._state.subscription_state.requested,
                        acknowledged=ack,
                        mode=self._state.subscription_state.mode,
                        last_ack_at=now_iso,
                    )
                    self._state.update_health(
                        acknowledged_subscription=ack, entitlement_error=False
                    )
                elif t == "error":
                    await self._handle_error(item)
                    if item.get("code") == 406:
                        return
                    if item.get("code") == 402:
                        self._fatal_auth = True
                        return
                elif t == "success":
                    pass  # heartbeat-like
                else:
                    await self._store.record_raw_event(
                        endpoint="news_ws",
                        message_type=str(t),
                        raw_json=orjson.dumps(item).decode("utf-8"),
                    )

            depth = self._queue.qsize()
            if depth >= self._config.slow_client_warning_queue_depth:
                log.warning("queue depth high: %d", depth)

    async def _enqueue_article(self, item: dict[str, Any]) -> None:
        # Fast path: queue has space.
        try:
            self._queue.put_nowait(("n", item))
        except asyncio.QueueFull:
            # Apply backpressure first — yield to the persister so it can drain.
            try:
                await asyncio.wait_for(
                    self._queue.put(("n", item)),
                    timeout=self._config.queue_backpressure_seconds,
                )
            except TimeoutError:
                # Backpressure exhausted. Persist as a raw event and bump the
                # dropped counter so loss is visible instead of silent.
                await self._record_dropped_article(item)
                return
        self._state.update_health(last_article_at=datetime.now(UTC).isoformat())

    async def _record_dropped_article(self, item: dict[str, Any]) -> None:
        dropped = self._state.record_article_drop()
        log.warning(
            "ingest queue overflow; dropped article id=%s total_dropped=%d",
            item.get("id"),
            dropped,
        )
        try:
            await self._store.record_raw_event(
                endpoint="news_ws",
                message_type="queue_overflow_drop",
                raw_json=orjson.dumps(item).decode("utf-8"),
            )
        except Exception as e:  # pragma: no cover - defensive
            log.exception("failed to record dropped article: %s", e)

    async def _handle_error(self, item: dict[str, Any]) -> None:
        code = int(item.get("code") or 0)
        msg = str(item.get("msg") or "")
        log.warning("alpaca stream error code=%d msg=%s", code, msg)
        await self._store.record_raw_event(
            endpoint="news_ws",
            message_type=f"error_{code}",
            raw_json=orjson.dumps(item).decode("utf-8"),
        )

        entitlement = code == 409
        alert = self._alerts.stream_error_alert(code=code, message=msg, entitlement=entitlement)
        inserted = await self._store.record_alert(
            alert, raw_json=orjson.dumps(item).decode("utf-8")
        )
        if inserted:
            self._state.record_alert(alert)

        if code == 402:
            self._state.update_health(authenticated=False, last_error=f"402 {msg}")
            self._fatal_auth = True
        elif code == 406:
            self._state.update_health(
                connection_limit_blocked=True,
                last_error=f"406 {msg}",
            )
            await asyncio.sleep(self._config.connection_limit_backoff_seconds)
            self._state.update_health(connection_limit_blocked=False)
        elif code == 409:
            self._state.update_health(
                entitlement_error=True,
                last_error=f"409 {msg}",
            )
        elif code == 410:
            log.error("BUG: invalid subscribe action for this feed (code=410)")
            self._state.update_health(last_error=f"410 {msg}")
        else:
            self._state.update_health(last_error=f"{code} {msg}")

    async def _persister_loop(self) -> None:
        while True:
            # Block for the first item, then drain whatever else is already
            # queued (no waiting — batching must never add visibility latency
            # for a single article; it only amortizes commits under bursts).
            try:
                batch = [await self._queue.get()]
            except asyncio.CancelledError:
                return
            while len(batch) < PERSIST_BATCH_MAX:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                await self._persist_batch([p for kind, p in batch if kind == "n"])
            except Exception as e:
                log.exception("persister error: %s", e)
            finally:
                for _ in batch:
                    self._queue.task_done()

    async def _persist_batch(self, payloads: list[dict[str, Any]]) -> None:
        if not payloads:
            return
        # Normalization (BeautifulSoup HTML stripping) is pure CPU — run it off
        # the event loop so a burst of long articles can't stall the recv loop.
        normalized_items, bad_items = await asyncio.to_thread(
            self._normalize_payloads, payloads
        )

        for payload, err in bad_items:
            log.warning("normalization failed: %s", err)
            await self._store.record_raw_event(
                endpoint="news_ws",
                message_type="bad_article",
                raw_json=orjson.dumps(payload).decode("utf-8"),
            )

        if not normalized_items:
            return
        interest = self._state.get_interest_symbols()
        pending_alerts = []
        async with self._store.batch_writer() as writer:
            for normalized in normalized_items:
                result = await writer.upsert_article(normalized, source_kind="ws")
                self._state.record_article(result.article, was_new=result.was_new)
                if result.was_new or result.version_inserted:
                    for alert in self._alerts.evaluate_article(
                        result.article, interest_symbols=interest
                    ):
                        inserted = await writer.record_alert(
                            alert, raw_json=normalized.raw_json
                        )
                        if inserted:
                            pending_alerts.append(alert)
        # Count alerts only after the batch commit actually lands.
        for alert in pending_alerts:
            self._state.record_alert(alert)

    @staticmethod
    def _normalize_payloads(
        payloads: list[dict[str, Any]],
    ) -> tuple[list[Any], list[tuple[dict[str, Any], NormalizationError]]]:
        ok: list[Any] = []
        bad: list[tuple[dict[str, Any], NormalizationError]] = []
        for payload in payloads:
            try:
                ok.append(normalize_news_message(payload))
            except NormalizationError as e:
                bad.append((payload, e))
        return ok, bad

    @staticmethod
    def _parse_frame(raw: str | bytes) -> list[dict[str, Any]]:
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = orjson.loads(raw)
        if isinstance(data, list):
            return [d for d in data if isinstance(d, dict)]
        if isinstance(data, dict):
            return [data]
        return []

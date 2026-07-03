"""Shared Alpaca WebSocket worker machinery.

BaseStreamWorker owns everything common to Alpaca data streams: the
connect/reconnect loop with jittered backoff (and backoff reset after a stable
session), header + message auth, an application-level idle watchdog, the
bounded ingest queue with backpressure/drop accounting, and the draining batch
persister. Subclasses (news, stocks) provide the URL, frame decoding,
subscription handshake, message routing, and batch persistence.

Each subclass is a per-process singleton: Alpaca allows one concurrent
connection per data endpoint, and the endpoints are distinct per stream, so
one news worker plus one stock worker is fine — two of either is not.
"""

from __future__ import annotations

import asyncio
import random
import threading
from datetime import UTC, datetime
from typing import Any, ClassVar

import orjson
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from .alerts import AlertEngine
from .config import Config
from .logging_utils import get_logger
from .state import State
from .store import Store

log = get_logger(__name__)

AUTH_TIMEOUT_SECONDS = 10.0
STALE_ALERT_MIN_INTERVAL_SECONDS = 3600.0
GAP_FILL_MAX_ATTEMPTS = 3
GAP_FILL_RETRY_WAIT_SECONDS = 30.0
SHUTDOWN_DRAIN_TIMEOUT_SECONDS = 5.0


class SingletonViolation(RuntimeError):
    pass


class BaseStreamWorker:
    stream_name: str = "stream"

    _instances_lock: ClassVar[threading.Lock] = threading.Lock()
    _instances: ClassVar[dict[type, BaseStreamWorker | None]] = {}

    def __init__(
        self,
        config: Config,
        store: Store,
        state: State,
        alerts: AlertEngine,
        *,
        queue_maxsize: int,
        gap_fill_callback=None,
    ) -> None:
        cls = type(self)
        with BaseStreamWorker._instances_lock:
            if BaseStreamWorker._instances.get(cls) is not None:
                raise SingletonViolation(
                    f"{cls.__name__} already constructed; only one Alpaca "
                    f"{self.stream_name} WebSocket allowed per process"
                )
            BaseStreamWorker._instances[cls] = self

        self._config = config
        self._store = store
        self._state = state
        self._alerts = alerts
        self._gap_fill = gap_fill_callback

        self._queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(
            maxsize=queue_maxsize
        )
        self._stop_event = asyncio.Event()
        self._main_task: asyncio.Task[None] | None = None
        self._persister_task: asyncio.Task[None] | None = None
        self._fatal_auth = False
        self._authed_this_session = False
        self._last_stale_alert: float | None = None
        self._sessions_authed = 0
        self._gap_fill_task: asyncio.Task[None] | None = None
        # Live socket while connected+authenticated; lets subclasses send
        # subscription deltas at runtime.
        self._ws: Any | None = None

    @classmethod
    def reset_singleton(cls) -> None:
        """Test-only: clear the singleton guard so a new instance can be created."""
        with BaseStreamWorker._instances_lock:
            BaseStreamWorker._instances[cls] = None

    # ---- subclass hooks ----------------------------------------------------

    def stream_url(self) -> str:
        raise NotImplementedError

    def connect_headers(self) -> list[tuple[str, str]]:
        return [
            ("APCA-API-KEY-ID", self._config.alpaca_api_key),
            ("APCA-API-SECRET-KEY", self._config.alpaca_secret_key),
        ]

    def decode_frame(self, raw: str | bytes) -> list[dict[str, Any]]:
        """Decode one WS frame into a list of message dicts. Raises ValueError-family
        (orjson.JSONDecodeError included) on malformed frames."""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        data = orjson.loads(raw)
        if isinstance(data, list):
            return [d for d in data if isinstance(d, dict)]
        if isinstance(data, dict):
            return [data]
        return []

    def encode_message(self, message: dict[str, Any]) -> str | bytes:
        """Encode an outbound control message (auth/subscribe/unsubscribe) in
        the stream's wire codec. Subclasses negotiating a non-JSON codec must
        override this to match — Alpaca rejects JSON control frames on a
        msgpack-negotiated connection."""
        return orjson.dumps(message).decode("utf-8")

    def idle_reconnect_seconds(self) -> float:
        """Idle watchdog threshold; 0 disables. Subclasses override."""
        return 0.0

    async def watchdog_should_fire(self) -> bool:
        """Called when the idle timeout elapses. Return False to keep waiting
        (e.g. the stock stream is legitimately silent while the market is
        closed)."""
        return True

    async def on_authenticated(self, ws: Any) -> None:
        """Send subscriptions after successful auth."""

    async def handle_item(self, ws: Any, item: dict[str, Any]) -> bool:
        """Route one decoded message. Return False to force a reconnect."""
        raise NotImplementedError

    async def handle_error(self, item: dict[str, Any]) -> None:
        """Handle a T='error' message (alerting, health, backoff side effects)."""
        raise NotImplementedError

    def batch_max(self) -> int:
        return 64

    def queue_warning_depth(self) -> int:
        return self._config.slow_client_warning_queue_depth

    async def persist_batch(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        raise NotImplementedError

    async def on_dropped_item(self, kind: str, item: dict[str, Any]) -> None:
        """Called when backpressure is exhausted and an item is dropped."""

    async def on_session_ended(self, *, authed: bool) -> None:
        """Called after every connection ends, before the reconnect backoff."""

    def gap_fill_on_first_session(self) -> bool:
        """Whether gap-fill should also run on the FIRST authenticated session.
        False for news (app_setup runs a startup REST backfill already); the
        stock worker overrides to True since it has no startup backfill and a
        restart would otherwise silently lose bars until the next reconnect."""
        return False

    async def capture_gap_fill_watermark(self) -> Any:
        """Snapshot the gap-fill start watermark BEFORE the receive loop runs.

        The gap-fill task runs concurrently with the receive loop; if the
        callback read its watermark lazily, a post-reconnect message persisted
        first would advance it past the outage window and the fill would
        silently skip the gap. The captured value is passed to the gap-fill
        callback."""
        return None

    # ---- lifecycle ----------------------------------------------------------

    async def start(self) -> None:
        if self._main_task is not None:
            return
        self._persister_task = asyncio.create_task(
            self._persister_loop(), name=f"{self.stream_name}-persister"
        )
        self._main_task = asyncio.create_task(self._run(), name=f"{self.stream_name}-stream")

    async def stop(self) -> None:
        self._stop_event.set()

        async def _await_cancelled(task: asyncio.Task[None] | None) -> None:
            if task is None:
                return
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning("%s task raised during shutdown: %s", self.stream_name, e)

        # Stop the receiver first so nothing new is enqueued...
        await _await_cancelled(self._main_task)
        await _await_cancelled(self._gap_fill_task)
        # ...then let the persister drain items already accepted into the
        # queue (bounded) so a graceful restart doesn't discard the last
        # batch, and only then cancel it.
        if self._persister_task is not None and not self._persister_task.done():
            try:
                await asyncio.wait_for(
                    self._queue.join(), timeout=SHUTDOWN_DRAIN_TIMEOUT_SECONDS
                )
            except TimeoutError:
                log.warning(
                    "%s shutdown: queue drain timed out with ~%d item(s) unpersisted",
                    self.stream_name,
                    self._queue.qsize(),
                )
        await _await_cancelled(self._persister_task)
        self._main_task = None
        self._persister_task = None
        self._gap_fill_task = None

    # ---- connect / reconnect loop -------------------------------------------

    async def _run(self) -> None:
        attempt = 0
        auth_failures = 0
        loop = asyncio.get_event_loop()
        while not self._stop_event.is_set():
            attempt += 1
            self._state.update_health(
                self.stream_name, connection_attempts=attempt, last_error=None
            )
            self._authed_this_session = False
            self._fatal_auth = False
            session_started = loop.time()
            try:
                await self._connect_and_run()
            except SingletonViolation:
                raise
            except Exception as e:
                log.exception("%s stream loop crashed: %s", self.stream_name, e)
                self._state.update_health(
                    self.stream_name, connected=False, last_error=str(e)
                )

            if self._stop_event.is_set():
                break

            try:
                await self.on_session_ended(authed=self._authed_this_session)
            except Exception as e:  # pragma: no cover - defensive
                log.exception("%s on_session_ended failed: %s", self.stream_name, e)

            if self._authed_this_session and auth_failures:
                auth_failures = 0
                self._state.update_health(self.stream_name, auth_failed=False)

            if self._fatal_auth:
                # Bad credentials. Do not silently stall forever: retry on a
                # slow cadence and re-alert periodically so the operator sees
                # it in the alert feed, not just in logs.
                auth_failures += 1
                self._state.update_health(self.stream_name, auth_failed=True)
                # A non-positive AUTH_ALERT_EVERY_N disables repeated alerts
                # (first failure still alerts) — it must not ZeroDivisionError
                # the stream task out of its slow retry cadence.
                every_n = self._config.auth_alert_every_n
                if auth_failures == 1 or (every_n > 0 and auth_failures % every_n == 0):
                    await self._record_stream_error_alert(
                        code=402,
                        message=(
                            f"{self.stream_name} stream authentication failing "
                            f"(failure #{auth_failures}); retrying every "
                            f"{self._config.auth_retry_seconds}s"
                        ),
                    )
                wait = float(self._config.auth_retry_seconds)
                log.error(
                    "%s fatal auth failure #%d; retrying in %.0fs",
                    self.stream_name,
                    auth_failures,
                    wait,
                )
            else:
                if loop.time() - session_started >= self._config.stable_connection_seconds:
                    # The session was healthy for a while — this disconnect is
                    # not part of a flap storm, so restart the backoff curve.
                    attempt = 1
                base_min = self._config.reconnect_min_seconds
                base_max = self._config.reconnect_max_seconds
                wait = min(base_max, base_min * (2 ** min(attempt - 1, 6)))
                wait = wait * (0.7 + 0.6 * random.random())
                log.info(
                    "%s reconnecting in %.1fs (attempt=%d)", self.stream_name, wait, attempt
                )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=wait)
            except TimeoutError:
                pass

            self._state.update_health(
                self.stream_name,
                reconnect_count=self._state.snapshot_health(self.stream_name).reconnect_count
                + 1,
            )

    async def _connect_and_run(self) -> None:
        url = self.stream_url()
        log.info("connecting to alpaca %s stream", self.stream_name)
        # Explicitly the asyncio client: the top-level websockets.connect alias
        # resolved to the legacy client (extra_headers, no additional_headers)
        # before 14.0.
        async with ws_connect(
            url,
            additional_headers=self.connect_headers(),
            ping_interval=20,
            ping_timeout=20,
            max_size=2**22,
        ) as ws:
            self._state.update_health(self.stream_name, connected=True, last_error=None)
            try:
                authed = await self._await_auth(ws)
                if not authed:
                    # Fall back to message auth.
                    authed = await self._auth_via_message(ws)
                if not authed:
                    self._state.update_health(
                        self.stream_name, authenticated=False, last_error="auth failed"
                    )
                    return

                self._authed_this_session = True
                self._state.update_health(
                    self.stream_name, authenticated=True, auth_failed=False
                )
                # The watermark must be captured BEFORE on_authenticated:
                # the news subscribe handshake can already consume and enqueue
                # data frames batched with the subscription ack, and a
                # persisted post-gap article would advance the watermark past
                # the outage window. (_sessions_authed is pre-increment here,
                # so > 0 means "this is a reconnect".)
                will_gap_fill = self._gap_fill is not None and (
                    self._sessions_authed > 0 or self.gap_fill_on_first_session()
                )
                watermark: Any = None
                if will_gap_fill:
                    watermark = await self.capture_gap_fill_watermark()
                # Publish the live socket BEFORE the subscription replay so a
                # watchlist mutation racing the handshake sends its delta over
                # this connection instead of silently waiting for the next one.
                self._ws = ws
                await self.on_authenticated(ws)
                self._sessions_authed += 1
                # Recover anything missed while disconnected. Runs AFTER the
                # subscription is live (so nothing new is missed during the
                # fill) and concurrently with the receive loop.
                if will_gap_fill:
                    self._spawn_gap_fill(watermark)
                await self._receive_loop(ws)
            finally:
                self._ws = None
                self._state.update_health(
                    self.stream_name, connected=False, authenticated=False
                )

    # ---- auth ----------------------------------------------------------------

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
            self._touch_last_message()
            try:
                items = self.decode_frame(raw)
            except (orjson.JSONDecodeError, ValueError):
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
                    await self.handle_error(item)
                    if code == 402:
                        # Already-authenticated quirks — message auth attempt may still work
                        return False
                    if code in (406,):
                        return False
            if connected and asyncio.get_event_loop().time() > deadline - 1:
                return False

    async def _auth_via_message(self, ws: Any) -> bool:
        log.info("attempting message-based authentication (%s)", self.stream_name)
        try:
            await ws.send(
                self.encode_message(
                    {
                        "action": "auth",
                        "key": self._config.alpaca_api_key,
                        "secret": self._config.alpaca_secret_key,
                    }
                )
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
            self._touch_last_message()
            try:
                items = self.decode_frame(raw)
            except (orjson.JSONDecodeError, ValueError):
                continue
            for item in items:
                t = item.get("T")
                msg = item.get("msg") or ""
                if t == "success" and msg == "authenticated":
                    return True
                if t == "error":
                    await self.handle_error(item)
                    return False

    # ---- receive loop ---------------------------------------------------------

    async def _receive_loop(self, ws: Any) -> None:
        while not self._stop_event.is_set():
            idle = self.idle_reconnect_seconds()
            try:
                if idle and idle > 0:
                    raw = await asyncio.wait_for(ws.recv(), timeout=idle)
                else:
                    raw = await ws.recv()
            except TimeoutError:
                if not await self.watchdog_should_fire():
                    continue
                stale = self._state.snapshot_health(self.stream_name).stale_reconnects + 1
                log.warning(
                    "%s stream idle for %.0fs; forcing reconnect (stale_reconnects=%d)",
                    self.stream_name,
                    idle,
                    stale,
                )
                self._state.update_health(
                    self.stream_name,
                    stale_reconnects=stale,
                    last_error=f"idle watchdog: no message in {idle:.0f}s",
                )
                await self._maybe_record_stale_alert(idle)
                return
            except ConnectionClosed as e:
                log.info("%s websocket closed: %s", self.stream_name, e)
                self._state.update_health(self.stream_name, connected=False)
                return
            try:
                items = self.decode_frame(raw)
            except (orjson.JSONDecodeError, ValueError):
                await self._store.record_raw_event(
                    endpoint=f"{self.stream_name}_ws",
                    message_type="malformed",
                    raw_json=str(raw)[:4000],
                )
                continue

            self._touch_last_message()
            for item in items:
                keep_running = await self.handle_item(ws, item)
                if not keep_running:
                    return

            depth = self._queue.qsize()
            if depth >= self.queue_warning_depth():
                log.warning("%s queue depth high: %d", self.stream_name, depth)

    def _touch_last_message(self) -> None:
        self._state.update_health(
            self.stream_name, last_message_at=datetime.now(UTC).isoformat()
        )

    def _spawn_gap_fill(self, watermark: Any = None) -> None:
        if self._gap_fill_task is not None and not self._gap_fill_task.done():
            log.info("%s gap fill already running; not spawning another", self.stream_name)
            return
        self._gap_fill_task = asyncio.create_task(
            self._gap_fill_with_retries(watermark), name=f"{self.stream_name}-gap-fill"
        )

    async def _gap_fill_with_retries(self, watermark: Any = None) -> None:
        assert self._gap_fill is not None
        for attempt in range(1, GAP_FILL_MAX_ATTEMPTS + 1):
            try:
                await self._gap_fill(watermark)
                return
            except asyncio.CancelledError:
                raise
            except Exception as e:
                failures = (
                    self._state.snapshot_health(self.stream_name).gap_fill_failures + 1
                )
                self._state.update_health(self.stream_name, gap_fill_failures=failures)
                exhausted = attempt == GAP_FILL_MAX_ATTEMPTS
                log.warning(
                    "%s gap fill failed (attempt %d/%d): %s",
                    self.stream_name,
                    attempt,
                    GAP_FILL_MAX_ATTEMPTS,
                    e,
                )
                # Alert on the first failure (high) and on exhaustion
                # (critical) — not on every retry.
                if attempt == 1 or exhausted:
                    alert = self._alerts.gap_fill_failure_alert(
                        stream=self.stream_name,
                        attempt=attempt,
                        exhausted=exhausted,
                        error=str(e),
                    )
                    try:
                        inserted = await self._store.record_alert(alert, raw_json="{}")
                        if inserted:
                            self._state.record_alert(alert)
                    except Exception:  # pragma: no cover - defensive
                        log.exception("failed to record gap_fill_failure alert")
                if not exhausted:
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(), timeout=GAP_FILL_RETRY_WAIT_SECONDS
                        )
                        return  # stopping
                    except TimeoutError:
                        pass

    async def _maybe_record_stale_alert(self, idle: float) -> None:
        now = asyncio.get_event_loop().time()
        if (
            self._last_stale_alert is not None
            and now - self._last_stale_alert < STALE_ALERT_MIN_INTERVAL_SECONDS
        ):
            return
        self._last_stale_alert = now
        alert = self._alerts.stream_stale_alert(
            stream=self.stream_name, idle_seconds=idle
        )
        inserted = await self._store.record_alert(alert, raw_json="{}")
        if inserted:
            self._state.record_alert(alert)

    async def _record_stream_error_alert(self, *, code: int, message: str) -> None:
        alert = self._alerts.stream_error_alert(code=code, message=message)
        try:
            inserted = await self._store.record_alert(alert, raw_json="{}")
            if inserted:
                self._state.record_alert(alert)
        except Exception as e:  # pragma: no cover - defensive
            log.exception("failed to record stream error alert: %s", e)

    # ---- queue -----------------------------------------------------------------

    async def _enqueue(self, kind: str, item: dict[str, Any]) -> bool:
        # Fast path: queue has space.
        try:
            self._queue.put_nowait((kind, item))
        except asyncio.QueueFull:
            # Apply backpressure first — yield to the persister so it can drain.
            try:
                await asyncio.wait_for(
                    self._queue.put((kind, item)),
                    timeout=self._config.queue_backpressure_seconds,
                )
            except TimeoutError:
                # Backpressure exhausted. Let the subclass make the loss
                # visible instead of silent.
                await self.on_dropped_item(kind, item)
                return False
        return True

    # ---- persister ----------------------------------------------------------------

    async def _persister_loop(self) -> None:
        while True:
            # Block for the first item, then drain whatever else is already
            # queued (no waiting — batching must never add visibility latency
            # for a single item; it only amortizes commits under bursts).
            try:
                batch = [await self._queue.get()]
            except asyncio.CancelledError:
                return
            while len(batch) < self.batch_max():
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            try:
                await self.persist_batch(batch)
            except Exception as e:
                log.exception("%s persister error: %s", self.stream_name, e)
            finally:
                for _ in batch:
                    self._queue.task_done()

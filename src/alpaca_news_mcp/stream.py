"""Single-connection Alpaca News WebSocket worker.

Owns the *only* Alpaca news WebSocket connection in the process. All the
generic connect/auth/backoff/watchdog/queue machinery lives in
ws_base.BaseStreamWorker; this class supplies the news URL, subscription
handshake (wildcard with entitlement fallback), message routing, and the
batched persist path (normalize → upsert → alert scoring).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Literal

import orjson

from .alerts import AlertEngine
from .config import Config
from .logging_utils import get_logger
from .models import SubscriptionState
from .normalize import NormalizationError, normalize_news_message
from .state import State
from .store import Store
from .ws_base import BaseStreamWorker, SingletonViolation

__all__ = ["NewsStreamWorker", "SingletonViolation"]

SubscriptionMode = Literal["wildcard", "fallback", "rest_only", "disconnected"]

log = get_logger(__name__)

SUBSCRIPTION_TIMEOUT_SECONDS = 10.0
# Max articles persisted per batch commit. News volume is low; this only
# matters during backfill floods and reconnect bursts.
PERSIST_BATCH_MAX = 64
# Cadence for re-ingesting articles dropped on queue overflow.
DROP_REPLAY_INTERVAL_SECONDS = 60.0
DROP_REPLAY_BATCH = 100


class NewsStreamWorker(BaseStreamWorker):
    stream_name = "news"

    def __init__(
        self,
        config: Config,
        store: Store,
        state: State,
        alerts: AlertEngine,
        *,
        gap_fill_callback=None,
    ) -> None:
        super().__init__(
            config,
            store,
            state,
            alerts,
            queue_maxsize=config.queue_maxsize,
            gap_fill_callback=gap_fill_callback,
        )
        self._replay_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        await super().start()
        if self._replay_task is None:
            self._replay_task = asyncio.create_task(
                self._drop_replay_loop(), name="news-drop-replay"
            )

    async def stop(self) -> None:
        if self._replay_task is not None:
            self._replay_task.cancel()
            try:
                await self._replay_task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning("drop-replay task raised during shutdown: %s", e)
            self._replay_task = None
        await super().stop()

    # ---- BaseStreamWorker hooks ---------------------------------------------

    def stream_url(self) -> str:
        return self._config.alpaca_news_stream_url

    def idle_reconnect_seconds(self) -> float:
        # 0 disables by default: the news feed is legitimately quiet overnight
        # and the websockets ping/pong (20s/20s) already detects dead sockets.
        return float(self._config.news_idle_reconnect_seconds)

    def batch_max(self) -> int:
        return PERSIST_BATCH_MAX

    async def capture_gap_fill_watermark(self) -> str | None:
        # Passed positionally into RestBackfillWorker.gap_fill(since_iso);
        # captured before the receive loop so a post-reconnect article can't
        # advance it past the outage window.
        return self._state.last_seen_updated_at

    async def on_authenticated(self, ws: Any) -> None:
        await self._subscribe(ws)

    async def handle_item(self, ws: Any, item: dict[str, Any]) -> bool:
        t = item.get("T")
        if t == "n":
            await self._enqueue_article(item)
        elif t == "subscription":
            ack = {k: v for k, v in item.items() if k != "T"}
            self._state.subscription_state = SubscriptionState(
                requested=self._state.subscription_state.requested,
                acknowledged=ack,
                mode=self._state.subscription_state.mode,
                last_ack_at=datetime.now(UTC).isoformat(),
            )
            self._state.update_health(
                acknowledged_subscription=ack, entitlement_error=False
            )
        elif t == "error":
            await self.handle_error(item)
            if item.get("code") == 406:
                return False
            if item.get("code") == 402:
                self._fatal_auth = True
                return False
        elif t == "success":
            pass  # heartbeat-like
        else:
            await self._store.record_raw_event(
                endpoint="news_ws",
                message_type=str(t),
                raw_json=orjson.dumps(item).decode("utf-8"),
            )
        return True

    # ---- subscription ----------------------------------------------------------

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
            self._touch_last_message()
            try:
                items = self.decode_frame(raw)
            except (orjson.JSONDecodeError, ValueError):
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
                    await self.handle_error(item)
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

    # ---- queue -------------------------------------------------------------------

    async def _enqueue_article(self, item: dict[str, Any]) -> None:
        if await self._enqueue("n", item):
            self._state.update_health(last_article_at=datetime.now(UTC).isoformat())

    async def on_dropped_item(self, kind: str, item: dict[str, Any]) -> None:
        dropped = self._state.record_article_drop(self.stream_name)
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

    # ---- coverage-gap + drop replay ---------------------------------------------

    async def on_session_ended(self, *, authed: bool) -> None:
        # With REST backfill enabled the reconnect gap-fill recovers the
        # window. Without it the loss is permanent — make it visible.
        if not authed or self._config.enable_rest_backfill:
            return
        alert = self._alerts.coverage_gap_alert(
            stream=self.stream_name,
            from_iso=self._state.last_seen_updated_at or "unknown",
            to_iso=datetime.now(UTC).isoformat(),
        )
        inserted = await self._store.record_alert(alert, raw_json="{}")
        if inserted:
            self._state.record_alert(alert)

    async def _drop_replay_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=DROP_REPLAY_INTERVAL_SECONDS
                )
                return  # stopping
            except TimeoutError:
                pass
            try:
                await self._replay_dropped_once()
            except asyncio.CancelledError:
                return
            except Exception as e:
                log.exception("drop replay failed: %s", e)

    async def _replay_dropped_once(self) -> int:
        """Re-ingest queue-overflow-dropped articles from raw_events. Returns
        how many were replayed."""
        events = await self._store.unreplayed_dropped_events(limit=DROP_REPLAY_BATCH)
        if not events:
            return 0
        payloads: list[dict[str, Any]] = []
        for ev in events:
            payload = ev.get("payload")
            if isinstance(payload, dict):
                payloads.append(payload)
            else:
                log.warning(
                    "unreplayable dropped event %s (bad payload)", ev.get("event_id")
                )
        if payloads:
            await self._persist_batch(payloads)
        # Mark even unparseable events so they don't wedge the loop forever.
        await self._store.mark_events_replayed([ev["event_id"] for ev in events])
        replayed = (
            self._state.snapshot_health(self.stream_name).dropped_replayed
            + len(payloads)
        )
        self._state.update_health(self.stream_name, dropped_replayed=replayed)
        log.info("replayed %d dropped article(s)", len(payloads))
        return len(payloads)

    # ---- error handling -------------------------------------------------------------

    async def handle_error(self, item: dict[str, Any]) -> None:
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

    # ---- persistence -----------------------------------------------------------------

    async def persist_batch(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        await self._persist_batch([p for kind, p in batch if kind == "n"])

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
        pending_articles = []
        async with self._store.batch_writer() as writer:
            for normalized in normalized_items:
                result = await writer.upsert_article(normalized, source_kind="ws")
                pending_articles.append((result.article, result.was_new))
                if result.was_new or result.version_inserted:
                    for alert in self._alerts.evaluate_article(
                        result.article, interest_symbols=interest
                    ):
                        inserted = await writer.record_alert(
                            alert,
                            raw_json=normalized.raw_json,
                            content_hash=normalized.content_hash,
                        )
                        if inserted:
                            pending_alerts.append(alert)
        # Advance in-memory state only after the batch commit actually lands:
        # record_article moves the gap-fill watermark (last_seen_updated_at),
        # so doing it mid-transaction would let a rollback leave the watermark
        # past articles that never reached SQLite — permanently skipping them
        # on the next gap-fill. Alert quota/state likewise.
        for article, was_new in pending_articles:
            self._state.record_article(article, was_new=was_new)
        for alert in pending_alerts:
            self._alerts.count_emission(alert)
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

"""Alpaca v2 stock market-data WebSocket worker.

Second (and only other) Alpaca data WebSocket in the process — Alpaca's
connection limit is per endpoint, so the news socket and this stock socket
coexist. Full-depth ingestion: trades, quotes, minute/updated/daily bars,
trading statuses, and LULD bands for a runtime-adjustable watchlist.

Data flows into the separate market SQLite database in executemany batches;
the latest tick per symbol is mirrored into an in-memory snapshot cache so
"latest price" tool calls never touch the DB. Trading halts and LULD updates
feed the shared alert table in the news DB so there is a single alert feed.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import msgpack
import orjson
from dateutil import parser as dateparser

from .alerts import AlertEngine
from .config import Config
from .logging_utils import get_logger
from .market_client import MarketDataClient
from .market_store import MarketStore
from .state import STOCK_STREAM, State
from .store import Store
from .ws_base import BaseStreamWorker

log = get_logger(__name__)

# Message type -> canonical channel name (subscription message key).
TYPE_TO_CHANNEL = {
    "t": "trades",
    "q": "quotes",
    "b": "bars",
    "u": "updatedBars",
    "d": "dailyBars",
    "s": "statuses",
    "l": "lulds",
}
DATA_TYPES = frozenset(TYPE_TO_CHANNEL)
RAW_TYPES = frozenset({"c", "x"})  # corrections, cancelErrors

WATCHLIST_STATUS_KEY = "stock_watchlist"
# Default local cap; runtime value comes from Config.max_watchlist_symbols
# (Alpaca's paid plan has no stream symbol limit — this bounds local DB
# volume and snapshot memory).
MAX_WATCHLIST_SYMBOLS = 100

BAR_GAP_FILL_FALLBACK_MINUTES = 60
# Rolling per-symbol bar history kept for derived alerts (price move /
# volume spike / range break) — memory only, tiny.
DERIVED_STATE_MAX_BARS = 60
VOLUME_BASELINE_MIN_SAMPLES = 10
RANGE_BREAK_MIN_BARS = 30


def _to_epoch_us(value: Any) -> int | None:
    """Alpaca timestamps arrive as RFC3339 strings (JSON, possibly with ns
    precision) or datetime objects (msgpack timestamp ext). Normalize to epoch
    microseconds."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1_000_000)
    if isinstance(value, str):
        try:
            dt = dateparser.isoparse(value)
        except (ValueError, TypeError):
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1_000_000)
    return None


def _conditions_str(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return ",".join(str(v) for v in value)
    return str(value)


class StockStreamWorker(BaseStreamWorker):
    stream_name = STOCK_STREAM

    def __init__(
        self,
        config: Config,
        store: Store,
        market_store: MarketStore,
        state: State,
        alerts: AlertEngine,
        *,
        market_client: MarketDataClient | None = None,
    ) -> None:
        super().__init__(
            config,
            store,
            state,
            alerts,
            queue_maxsize=config.stock_queue_maxsize,
            # Bar gap-fill only makes sense when a bar channel is actually
            # ingested — otherwise reconnects would write REST bars the
            # operator explicitly opted out of via STOCK_CHANNELS.
            gap_fill_callback=(
                self._bar_gap_fill
                if market_client is not None
                and {"bars", "updatedBars"} & set(config.stock_channels)
                else None
            ),
        )
        self._market_store = market_store
        self._market_client = market_client
        self._channels: list[str] = list(config.stock_channels)
        self._max_watchlist = config.max_watchlist_symbols
        self._watchlist: set[str] = self._cap_watchlist(
            {s.upper() for s in config.stock_watchlist_symbols}, source="configured"
        )
        self._acknowledged: dict[str, list[str]] | None = None
        self._watchlist_lock = asyncio.Lock()
        # True while this session's statuses:["*"] subscription is believed
        # accepted — market-wide halt coverage beyond the watchlist.
        self._statuses_wildcard_ok = False
        # Per-symbol rolling bar state for derived alerts.
        self._bar_state: dict[str, dict[str, Any]] = {}
        self._add_backfill_tasks: set[asyncio.Task[None]] = set()
        # Per-symbol last-persisted quote-sample bucket. Without cross-batch
        # state the STOCK_QUOTE_SAMPLE_MS knob only thinned within a single
        # drained batch, under-delivering its documented storage savings.
        self._last_quote_bucket: dict[str, int] = {}

    def _cap_watchlist(self, symbols: set[str], *, source: str) -> set[str]:
        """update_watchlist rejects oversized requests outright; the startup
        paths (env config / restored state) clamp instead — an oversized
        subscribe would hit plan limits and leave the stream with no valid
        acknowledged subscription at all. Deterministic: keeps the first
        max_watchlist_symbols in sorted order."""
        if len(symbols) <= self._max_watchlist:
            return symbols
        kept = set(sorted(symbols)[: self._max_watchlist])
        log.warning(
            "%s watchlist has %d symbols; capped to %d (dropped: %s)",
            source,
            len(symbols),
            self._max_watchlist,
            ",".join(sorted(symbols - kept)),
        )
        return kept

    # ---- watchlist -------------------------------------------------------------

    def watchlist(self) -> list[str]:
        return sorted(self._watchlist)

    def channels(self) -> list[str]:
        return list(self._channels)

    def acknowledged_subscription(self) -> dict[str, list[str]] | None:
        return self._acknowledged

    async def restore_watchlist(self) -> None:
        """Load the persisted watchlist (survives restarts; set via tool).
        An explicitly persisted EMPTY list is honored too — an operator who
        cleared the stream must not get the config default back on restart."""
        persisted = await self._store.get_status(WATCHLIST_STATUS_KEY)
        if persisted and isinstance(persisted.get("symbols"), list):
            symbols = self._cap_watchlist(
                {str(s).upper() for s in persisted["symbols"] if str(s).strip()},
                source="restored",
            )
            self._watchlist = symbols
            log.info("restored stock watchlist: %s", sorted(symbols) or "(empty)")

    async def update_watchlist(
        self, symbols: list[str], mode: str = "replace"
    ) -> dict[str, Any]:
        """Adjust the watchlist at runtime. Sends subscribe/unsubscribe deltas
        over the live socket when connected; the full desired state is replayed
        on every (re)connect regardless."""
        cleaned = {s.strip().upper() for s in symbols if s and s.strip()}
        async with self._watchlist_lock:
            current = set(self._watchlist)
            if mode == "replace":
                new = cleaned
            elif mode == "add":
                new = current | cleaned
            elif mode == "remove":
                new = current - cleaned
            else:
                return {"error": "invalid_mode", "mode": mode}
            if len(new) > self._max_watchlist:
                return {
                    "error": "watchlist_too_large",
                    "max_allowed": self._max_watchlist,
                    "requested": len(new),
                    "hint": "remove symbols first, or raise MAX_WATCHLIST_SYMBOLS",
                }
            added = new - current
            removed = current - new
            self._watchlist = new
            # Evict removed symbols from the latest cache — a stale snapshot
            # would keep serving old prices as source="stream" for a symbol
            # that is no longer subscribed.
            for sym in removed:
                self._market_store.snapshots.remove(sym)
            await self._store.set_status(
                WATCHLIST_STATUS_KEY, {"symbols": sorted(new)}
            )
            if added or removed:
                # The old acknowledgement no longer describes the desired
                # subscription; report acknowledged symbols only once Alpaca
                # acks the new state (trust acks, not requests).
                self._acknowledged = None
                self._state.update_health(
                    self.stream_name, acknowledged_subscription=None
                )
            ws = self._ws
            sent_delta = False
            # While the wildcard statuses subscription is live, per-symbol
            # deltas must not touch the statuses channel — unsubscribing a
            # symbol's statuses could subtract from the market-wide coverage.
            delta_channels = [
                c
                for c in self._channels
                if not (c == "statuses" and self._statuses_wildcard_ok)
            ]
            if ws is not None and (added or removed):
                try:
                    if removed:
                        await self._send_subscription(
                            ws, "unsubscribe", sorted(removed), channels=delta_channels
                        )
                    if added:
                        await self._send_subscription(
                            ws, "subscribe", sorted(added), channels=delta_channels
                        )
                    sent_delta = True
                except Exception as e:
                    log.warning("watchlist delta send failed (will apply on reconnect): %s", e)
        # Outside the lock: bounded REST bar backfill for newly added symbols
        # so get_stock_bars/get_symbol_context aren't blind until the next
        # reconnect gap-fill.
        if added:
            self._spawn_added_symbol_backfill(sorted(added))
        return {
            "watchlist": sorted(new),
            "channels": self._channels,
            "added": sorted(added),
            "removed": sorted(removed),
            "connected": ws is not None,
            "delta_sent": sent_delta,
        }

    def _spawn_added_symbol_backfill(self, symbols: list[str]) -> None:
        if (
            self._market_client is None
            or self._config.watchlist_add_backfill_minutes <= 0
            or not {"bars", "updatedBars"} & set(self._channels)
        ):
            return
        task = asyncio.create_task(
            self._backfill_added_symbols(symbols),
            name=f"watchlist-add-backfill-{','.join(symbols[:3])}",
        )
        self._add_backfill_tasks.add(task)
        task.add_done_callback(self._add_backfill_tasks.discard)

    async def _backfill_added_symbols(self, symbols: list[str]) -> None:
        assert self._market_client is not None
        start_ts = (
            int(datetime.now(UTC).timestamp())
            - self._config.watchlist_add_backfill_minutes * 60
        )
        start_iso = datetime.fromtimestamp(start_ts, tz=UTC).isoformat()
        try:
            bars_by_symbol = await self._market_client.bars(
                symbols, start_iso=start_iso, timeframe="1Min"
            )
        except Exception as e:
            log.warning("watchlist-add bar backfill failed for %s: %s", symbols, e)
            return
        rows: list[tuple[Any, ...]] = []
        for symbol, bars in bars_by_symbol.items():
            for bar in bars:
                ts_us = _to_epoch_us(bar.get("t"))
                if ts_us is None:
                    continue
                rows.append(
                    (
                        symbol.upper(), "1min", ts_us // 1_000_000,
                        bar.get("o"), bar.get("h"), bar.get("l"), bar.get("c"),
                        bar.get("v"), bar.get("n"), bar.get("vw"),
                    )
                )
        if rows:
            await self._market_store.upsert_bars(rows)
            log.info(
                "watchlist-add backfill: %d bars for %s", len(rows), ",".join(symbols)
            )

    async def stop(self) -> None:
        for task in list(self._add_backfill_tasks):
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._add_backfill_tasks.clear()
        await super().stop()

    async def _send_subscription(
        self,
        ws: Any,
        action: str,
        symbols: list[str],
        *,
        channels: list[str] | None = None,
    ) -> None:
        if not symbols:
            return
        message: dict[str, Any] = {"action": action}
        for channel in channels if channels is not None else self._channels:
            message[channel] = symbols
        await ws.send(self.encode_message(message))

    # ---- BaseStreamWorker hooks ----------------------------------------------------

    def stream_url(self) -> str:
        if self._config.alpaca_stock_stream_url:
            return self._config.alpaca_stock_stream_url
        return f"wss://stream.data.alpaca.markets/v2/{self._config.alpaca_stock_feed}"

    def connect_headers(self) -> list[tuple[str, str]]:
        headers = super().connect_headers()
        if self._config.stock_stream_codec == "msgpack":
            headers.append(("Content-Type", "application/msgpack"))
        return headers

    def decode_frame(self, raw: str | bytes) -> list[dict[str, Any]]:
        if isinstance(raw, bytes) and self._config.stock_stream_codec == "msgpack":
            data = msgpack.unpackb(raw, timestamp=3, raw=False)
            if isinstance(data, list):
                return [d for d in data if isinstance(d, dict)]
            if isinstance(data, dict):
                return [data]
            return []
        return super().decode_frame(raw)

    def encode_message(self, message: dict[str, Any]) -> str | bytes:
        # On a msgpack-negotiated connection Alpaca expects outbound control
        # frames (auth/subscribe/unsubscribe) to be msgpack too — JSON text
        # frames are rejected with 400 invalid syntax.
        if self._config.stock_stream_codec == "msgpack":
            return msgpack.packb(message)
        return super().encode_message(message)

    def idle_reconnect_seconds(self) -> float:
        return float(self._config.stock_idle_reconnect_seconds)

    def gap_fill_on_first_session(self) -> bool:
        # Unlike news (startup REST backfill in app_setup), stocks have no
        # startup fill — after a process restart the first connection must
        # backfill bars missed while down.
        return True

    async def watchdog_should_fire(self) -> bool:
        """Only treat silence as a stall while the market is open. A closed
        market (or an unavailable clock) keeps the watchdog inert so we don't
        churn reconnects all night."""
        if not self._watchlist:
            # An intentionally cleared watchlist means silence is EXPECTED —
            # reconnecting on the idle timer all day would just churn.
            return False
        if not set(self._channels) & {"trades", "quotes", "bars"}:
            # Event-only subscriptions (statuses/lulds/updatedBars/dailyBars)
            # have no steady message flow — silence during market hours is a
            # healthy subscription, not a stall.
            return False
        if self._market_client is None:
            return False
        try:
            is_open = await self._market_client.is_market_open()
        except Exception:  # pragma: no cover - defensive
            return False
        return bool(is_open)

    def batch_max(self) -> int:
        return self._config.stock_batch_max

    def queue_warning_depth(self) -> int:
        return max(1, int(self._config.stock_queue_maxsize * 0.75))

    async def on_authenticated(self, ws: Any) -> None:
        # Fresh session: quote-sample bucket state must not suppress the
        # first quote after a reconnect.
        self._last_quote_bucket.clear()
        # Hold the watchlist lock across the replay so a concurrent
        # update_watchlist can't interleave: it either lands before the
        # snapshot read (replay includes it) or after the send (its delta
        # goes over the already-published socket) — never in between.
        async with self._watchlist_lock:
            # New session: the previous session's ack no longer describes what
            # Alpaca is delivering. Clear it so health/tools never report
            # symbols the current connection hasn't acknowledged (trust acks,
            # not requests).
            self._acknowledged = None
            self._statuses_wildcard_ok = False
            # Replay the full desired subscription; the ack arrives via the
            # receive loop (handle_item) so we don't block here.
            await self._send_subscription(ws, "subscribe", sorted(self._watchlist))
            requested = {c: sorted(self._watchlist) for c in self._channels}
            # Market-wide halt coverage: statuses are a few tiny events per
            # day even across the whole market, so subscribe them wildcard.
            # Sent as a SEPARATE frame — mixing "*" and symbol lists in one
            # message risks a whole-message rejection.
            if self._config.stock_status_wildcard and "statuses" in self._channels:
                await ws.send(
                    self.encode_message({"action": "subscribe", "statuses": ["*"]})
                )
                self._statuses_wildcard_ok = True
                requested["statuses"] = ["*"]
            self._state.update_health(
                self.stream_name,
                requested_subscription=requested,
                acknowledged_subscription=None,
                subscription_mode="watchlist",
            )

    async def handle_item(self, ws: Any, item: dict[str, Any]) -> bool:
        t = item.get("T")
        if t in DATA_TYPES:
            if await self._enqueue(str(t), item):
                self._state.update_health(
                    self.stream_name, last_article_at=datetime.now(UTC).isoformat()
                )
        elif t == "subscription":
            ack = {
                k: v for k, v in item.items() if k != "T" and isinstance(v, list)
            }
            self._acknowledged = ack
            self._state.update_health(
                self.stream_name,
                acknowledged_subscription=ack,
                entitlement_error=False,
            )
        elif t == "error":
            await self.handle_error(item)
            code = item.get("code")
            if code == 406:
                return False
            if code == 402:
                self._fatal_auth = True
                return False
        elif t == "success":
            pass
        elif t in RAW_TYPES or t is not None:
            await self._enqueue("raw", item)
        return True

    async def handle_error(self, item: dict[str, Any]) -> None:
        code = int(item.get("code") or 0)
        msg = str(item.get("msg") or "")
        log.warning("alpaca stock stream error code=%d msg=%s", code, msg)

        entitlement = code == 409
        # Same dedup as the news worker: one alert per (stream, code) window,
        # not one per backoff cycle.
        if not self._alerts.stream_alert_deduped(self.stream_name, code):
            alert = self._alerts.stream_error_alert(
                code=code, message=f"stocks: {msg}", entitlement=entitlement
            )
            inserted = await self._store.record_alert(
                alert, raw_json=orjson.dumps(item, default=str).decode("utf-8")
            )
            if inserted:
                self._state.record_alert(alert)

        if code == 402:
            self._state.update_health(
                self.stream_name, authenticated=False, last_error=f"402 {msg}"
            )
            self._fatal_auth = True
        elif code == 406:
            self._state.update_health(
                self.stream_name,
                connection_limit_blocked=True,
                last_error=f"406 {msg}",
            )
            # No in-handler sleep — see NewsStreamWorker.handle_error; _run
            # consumes the deadline and clears the flag once it passes.
            self._backoff_until = (
                asyncio.get_event_loop().time()
                + self._config.connection_limit_backoff_seconds
            )
        elif code in (405, 409, 410):
            # Subscription-shaped failures (symbol limit, entitlement, invalid
            # action): alert + record, but keep the connection alive — never
            # crash-loop over a rejected channel.
            self._state.update_health(
                self.stream_name,
                entitlement_error=entitlement,
                last_error=f"{code} {msg}",
            )
            # If the wildcard statuses frame was rejected, fall back to
            # per-symbol statuses so halt coverage for the watchlist survives.
            if self._statuses_wildcard_ok and "statuses" in self._channels:
                self._statuses_wildcard_ok = False
                ws = self._ws
                if ws is not None and self._watchlist:
                    try:
                        await self._send_subscription(
                            ws,
                            "subscribe",
                            sorted(self._watchlist),
                            channels=["statuses"],
                        )
                        log.info(
                            "statuses wildcard rejected; resubscribed watchlist statuses"
                        )
                    except Exception as e:  # pragma: no cover - defensive
                        log.warning("statuses fallback resubscribe failed: %s", e)
        else:
            self._state.update_health(self.stream_name, last_error=f"{code} {msg}")

    async def on_dropped_item(self, kind: str, item: dict[str, Any]) -> None:
        if kind in ("s", "l", "raw", "b", "u", "d"):
            # Halts/LULDs/raw audit events (corrections, cancelErrors,
            # unknowns) are rare, audit-preserved, and alert-relevant — a
            # quote burst overflowing the queue must not lose them. Bars are
            # equally rare (~1/symbol/min) and carry the analytical signal a
            # mid-stream hole in get_bars_since cannot recover. Persist
            # directly, bypassing the queue (persist_batch also emits alerts).
            try:
                await self.persist_batch([(kind, item)])
                log.warning(
                    "stock queue overflow: persisted %s for %s directly",
                    kind,
                    item.get("S"),
                )
                return
            except Exception as e:
                log.exception("direct persist of dropped %s failed: %s", kind, e)
        dropped = self._state.record_article_drop(self.stream_name)
        if dropped == 1 or dropped % 1000 == 0:
            log.warning(
                "stock ingest queue overflow; dropped %s total_dropped=%d",
                kind,
                dropped,
            )

    async def recover_failed_batch(
        self, batch: list[tuple[str, dict[str, Any]]]
    ) -> None:
        """Ticks are accepted loss once the DB write failed twice, but the
        rare audit-critical items (halts/LULDs/bars/raw) each get one more
        direct attempt, and the tick loss lands in the dropped counter."""
        dropped = 0
        for kind, item in batch:
            if kind in ("s", "l", "raw", "b", "u", "d"):
                try:
                    await self.persist_batch([(kind, item)])
                except Exception:
                    log.exception("direct persist of %s during recovery failed", kind)
                    dropped += 1
            else:
                dropped += 1
        if dropped:
            health = self._state.snapshot_health(self.stream_name)
            self._state.update_health(
                self.stream_name,
                articles_dropped=health.articles_dropped + dropped,
            )

    # ---- persistence -----------------------------------------------------------------

    async def persist_batch(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        rows = self._rows_from_batch(batch)
        await self._market_store.persist_stream_batch(
            trades=rows["trades"] or None,
            quotes=rows["quotes"] or None,
            bars=rows["bars"] or None,
            statuses=rows["statuses"] or None,
            lulds=rows["lulds"] or None,
            raw_events=rows["raw"] or None,
        )
        # Quote-sample bucket state commits only AFTER the transaction
        # succeeded: advancing it during row shaping would make the
        # persister's retry of a failed batch see the buckets as already
        # emitted and silently drop the sampled quotes.
        if rows["quote_buckets"]:
            self._last_quote_bucket.update(rows["quote_buckets"])
        self._update_snapshots(batch)
        try:
            await self._emit_market_alerts(rows["status_items"], rows["luld_items"])
            # Derived price/volume alerts run on bar events only (~1 event
            # per symbol per minute) — never on the quote hot path.
            if rows["bar_items"]:
                await self._emit_derived_alerts(rows["bar_items"])
        except Exception:
            # The market transaction above already committed. A failed alert
            # write (news DB lock/I/O) must not bubble into the persister's
            # retry — trades/quotes have no uniqueness, so a replay would
            # duplicate ticks and inflate volume/tape stats. The underlying
            # events stay durable in stock_statuses/stock_lulds/stock_bars.
            log.exception(
                "post-commit alert emission failed (market rows committed)"
            )

    def _rows_from_batch(
        self, batch: list[tuple[str, dict[str, Any]]]
    ) -> dict[str, Any]:
        trades: list[tuple[Any, ...]] = []
        quotes: list[tuple[Any, ...]] = []
        bars: list[tuple[Any, ...]] = []
        statuses: list[tuple[Any, ...]] = []
        lulds: list[tuple[Any, ...]] = []
        raw: list[tuple[str, str]] = []
        status_items: list[dict[str, Any]] = []
        luld_items: list[dict[str, Any]] = []
        bar_items: list[dict[str, Any]] = []

        for kind, item in batch:
            symbol = str(item.get("S") or "").upper()
            ts_us = _to_epoch_us(item.get("t"))
            price = item.get("p")
            # A trade needs a real numeric price — coercing a missing/null
            # price to 0.0 would plant fake zero trades in the tick series.
            # Malformed frames fall through to the raw audit path instead.
            if (
                kind == "t"
                and symbol
                and ts_us is not None
                and isinstance(price, (int, float))
                and not isinstance(price, bool)
            ):
                trades.append(
                    (
                        symbol,
                        ts_us,
                        float(price),
                        item.get("s"),
                        item.get("x"),
                        _conditions_str(item.get("c")),
                        item.get("z"),
                        item.get("i"),
                    )
                )
            elif kind == "q" and symbol and ts_us is not None:
                quotes.append(
                    (
                        symbol,
                        ts_us,
                        item.get("bp"),
                        item.get("bs"),
                        item.get("bx"),
                        item.get("ap"),
                        item.get("as"),
                        item.get("ax"),
                        _conditions_str(item.get("c")),
                        item.get("z"),
                    )
                )
            elif kind in ("b", "u", "d") and symbol and ts_us is not None:
                timeframe = "1day" if kind == "d" else "1min"
                if kind == "b":
                    bar_items.append(item)
                bars.append(
                    (
                        symbol,
                        timeframe,
                        ts_us // 1_000_000,
                        item.get("o"),
                        item.get("h"),
                        item.get("l"),
                        item.get("c"),
                        item.get("v"),
                        item.get("n"),
                        item.get("vw"),
                    )
                )
            elif kind == "s" and symbol and ts_us is not None:
                statuses.append(
                    (
                        symbol,
                        ts_us,
                        item.get("sc"),
                        item.get("sm"),
                        item.get("rc"),
                        item.get("rm"),
                        item.get("z"),
                    )
                )
                status_items.append(item)
                # Statuses/LULDs are rare and audit-critical (they drive halt
                # alerts) — keep the original payload alongside the flattened
                # row. High-volume ticks (t/q/b) stay flattened only: the
                # columns are lossless for documented fields and raw copies
                # would multiply write volume.
                raw.append(("s", orjson.dumps(item, default=str).decode("utf-8")))
            elif kind == "l" and symbol and ts_us is not None:
                lulds.append(
                    (
                        symbol,
                        ts_us,
                        item.get("u"),
                        item.get("d"),
                        item.get("i"),
                        item.get("z"),
                    )
                )
                luld_items.append(item)
                raw.append(("l", orjson.dumps(item, default=str).decode("utf-8")))
            else:
                raw.append(
                    (
                        str(item.get("T")),
                        orjson.dumps(item, default=str).decode("utf-8"),
                    )
                )

        # Optional quote thinning for storage (the snapshot cache always keeps
        # the true latest regardless). Keep the last quote per symbol per
        # sample bucket, remembering buckets ACROSS batches — small drains
        # would otherwise store one quote per batch instead of one per bucket.
        # This method only READS _last_quote_bucket; the candidate updates go
        # back via quote_buckets and are committed by persist_batch after the
        # transaction succeeds, so a retried batch keeps its quotes.
        quote_buckets: dict[str, int] = {}
        sample_ms = self._config.stock_quote_sample_ms
        if sample_ms > 0 and quotes:
            bucketed: dict[tuple[str, int], tuple[Any, ...]] = {}
            for q in quotes:
                bucketed[(q[0], q[1] // (sample_ms * 1000))] = q
            kept: list[tuple[Any, ...]] = []
            for (sym, bucket), q in sorted(bucketed.items()):
                if self._last_quote_bucket.get(sym) == bucket:
                    continue
                quote_buckets[sym] = bucket  # sorted: ends at the max bucket
                kept.append(q)
            quotes = kept

        return {
            "trades": trades,
            "quotes": quotes,
            "bars": bars,
            "statuses": statuses,
            "lulds": lulds,
            "raw": raw,
            "status_items": status_items,
            "luld_items": luld_items,
            "bar_items": bar_items,
            "quote_buckets": quote_buckets,
        }

    def _update_snapshots(self, batch: list[tuple[str, dict[str, Any]]]) -> None:
        kind_map = {
            "t": "trade",
            "q": "quote",
            "b": "minute_bar",
            "u": "minute_bar",
            "d": "daily_bar",
            "s": "status",
            "l": "luld",
        }
        for kind, item in batch:
            slot = kind_map.get(kind)
            symbol = str(item.get("S") or "").upper()
            if slot is None or not symbol:
                continue
            if symbol not in self._watchlist:
                # Queued/in-flight frames for a symbol just removed from the
                # watchlist can arrive after update_watchlist evicted it —
                # repopulating the cache would resurrect source="stream" data
                # for an unsubscribed symbol indefinitely.
                continue
            ts_us = _to_epoch_us(item.get("t"))
            if ts_us is None:
                # Frames without a parseable timestamp aren't persisted as
                # flattened rows (they route to market_raw_events) — they must
                # not overwrite a good latest snapshot either: the cache can't
                # order a None timestamp, so a malformed frame would win.
                continue
            if kind == "t":
                price = item.get("p")
                if not isinstance(price, (int, float)) or isinstance(price, bool):
                    # Same rule as the tick store: a trade without a real
                    # numeric price must not become the cached latest trade.
                    continue
            snap = {
                k: v for k, v in item.items() if k not in ("T", "S")
            }
            snap.pop("t", None)
            snap["ts_us"] = ts_us
            self._market_store.snapshots.update(symbol, slot, snap)

    async def _emit_market_alerts(
        self,
        status_items: list[dict[str, Any]],
        luld_items: list[dict[str, Any]],
    ) -> None:
        interest = self._state.get_interest_symbols()
        for item in status_items:
            symbol = str(item.get("S") or "").upper()
            important = symbol in self._watchlist or symbol in interest
            alert = self._alerts.status_alert(
                symbol=symbol,
                status_code=item.get("sc"),
                status_msg=item.get("sm"),
                reason_code=item.get("rc"),
                reason_msg=item.get("rm"),
                important=important,
                # Wildcard coverage: statuses for symbols the bot neither
                # holds nor watches are discovery signal, not urgency.
                background=self._statuses_wildcard_ok and not important,
            )
            if alert is not None:
                inserted = await self._store.record_alert(
                    alert, raw_json=orjson.dumps(item, default=str).decode("utf-8")
                )
                if inserted:
                    self._state.record_alert(alert)
        for item in luld_items:
            symbol = str(item.get("S") or "").upper()
            alert = self._alerts.luld_alert(
                symbol=symbol,
                limit_up=item.get("u"),
                limit_down=item.get("d"),
                indicator=item.get("i"),
            )
            if alert is not None:
                inserted = await self._store.record_alert(
                    alert, raw_json=orjson.dumps(item, default=str).decode("utf-8")
                )
                if inserted:
                    self._state.record_alert(alert)

    async def _emit_derived_alerts(self, bar_items: list[dict[str, Any]]) -> None:
        """Deterministic price/volume triggers evaluated on minute bars.

        Converts 'invisible until the bot happens to pull bars' moves into
        pushed alerts on the feed it already polls. State is per-symbol and
        in-memory: recent (ts, close) pairs for the move window, recent
        volumes for the spike baseline, and the running day high/low."""
        cfg = self._config
        if (
            cfg.price_move_alert_pct <= 0
            and cfg.volume_spike_ratio <= 0
        ):
            return
        interest = self._state.get_interest_symbols()
        window_s = max(60, cfg.price_move_window_minutes * 60)
        for item in bar_items:
            symbol = str(item.get("S") or "").upper()
            ts_us = _to_epoch_us(item.get("t"))
            close = item.get("c")
            if not symbol or ts_us is None or not isinstance(close, (int, float)):
                continue
            ts = ts_us // 1_000_000
            h_val = item.get("h")
            l_val = item.get("l")
            v_val = item.get("v")
            high = float(h_val) if isinstance(h_val, (int, float)) else float(close)
            low = float(l_val) if isinstance(l_val, (int, float)) else float(close)
            volume = float(v_val) if isinstance(v_val, (int, float)) else 0.0
            st = self._bar_state.setdefault(
                symbol,
                {"closes": [], "vols": [], "day": None, "hi": None, "lo": None},
            )
            if st["day"] != ts // 86_400:
                st.update(day=ts // 86_400, hi=None, lo=None)
                st["closes"].clear()
                st["vols"].clear()
            pending: list[Any] = []
            important = symbol in self._watchlist or symbol in interest

            closes = st["closes"]
            while closes and ts - closes[0][0] > window_s:
                closes.pop(0)
            if cfg.price_move_alert_pct > 0 and closes:
                ref_ts, ref_close = closes[0]
                # Require most of the window to have elapsed so two adjacent
                # bars can't fake a "5-minute" move.
                if ref_close and ts - ref_ts >= window_s * 0.6:
                    pct = (float(close) - ref_close) / ref_close * 100.0
                    if abs(pct) >= cfg.price_move_alert_pct:
                        pending.append(
                            self._alerts.price_move_alert(
                                symbol=symbol,
                                pct=pct,
                                window_minutes=cfg.price_move_window_minutes,
                                important=important,
                            )
                        )

            vols = st["vols"]
            if (
                cfg.volume_spike_ratio > 0
                and len(vols) >= VOLUME_BASELINE_MIN_SAMPLES
                and volume
            ):
                avg = sum(vols) / len(vols)
                if avg > 0 and float(volume) >= avg * cfg.volume_spike_ratio:
                    pending.append(
                        self._alerts.volume_spike_alert(
                            symbol=symbol,
                            ratio=float(volume) / avg,
                            important=important,
                        )
                    )

            if len(closes) >= RANGE_BREAK_MIN_BARS:
                if st["hi"] is not None and float(close) > st["hi"]:
                    pending.append(
                        self._alerts.day_range_break_alert(
                            symbol=symbol, side="high", level=st["hi"],
                            important=important,
                        )
                    )
                elif st["lo"] is not None and float(close) < st["lo"]:
                    pending.append(
                        self._alerts.day_range_break_alert(
                            symbol=symbol, side="low", level=st["lo"],
                            important=important,
                        )
                    )

            closes.append((ts, float(close)))
            vols.append(float(volume or 0))
            if len(closes) > DERIVED_STATE_MAX_BARS:
                del closes[0]
            if len(vols) > DERIVED_STATE_MAX_BARS:
                del vols[0]
            st["hi"] = high if st["hi"] is None else max(st["hi"], high)
            st["lo"] = low if st["lo"] is None else min(st["lo"], low)

            for alert in pending:
                if alert is None:  # deduped inside the factory
                    continue
                inserted = await self._store.record_alert(alert, raw_json="{}")
                if inserted:
                    self._state.record_alert(alert)

    # ---- bar gap-fill ----------------------------------------------------------------

    async def capture_gap_fill_watermark(self) -> dict[str, int]:
        # Latest stored bar per symbol, snapshotted before the receive loop
        # runs — otherwise a post-reconnect stream bar could advance the
        # watermark and the fill would skip the outage window.
        return await self._market_store.latest_bar_ts(
            sorted(self._watchlist), timeframe="1min"
        )

    async def _bar_gap_fill(self, watermark: dict[str, int] | None = None) -> None:
        """After a reconnect, backfill minute bars from REST for the watchlist.
        Trades/quotes gaps are deliberately NOT backfilled (accepted loss for
        tick data; bars carry the analytical signal).

        Windows are per symbol: a single min() across the watchlist would let
        one stale/illiquid symbol drag the fetch back days for everyone and
        blow the REST pagination cap. Symbols are bucketed by their start
        (5-minute rounding) so nearby watermarks still share one REST call,
        and each symbol keeps only rows at/after its own watermark (the
        watermark bar itself is refetched — it may have been partial when the
        connection dropped; the idempotent upsert makes that free)."""
        assert self._market_client is not None
        symbols = sorted(self._watchlist)
        if not symbols:
            return
        latest = (
            watermark
            if watermark is not None
            else await self._market_store.latest_bar_ts(symbols, timeframe="1min")
        )
        now_ts = int(datetime.now(UTC).timestamp())
        fallback_start = now_ts - BAR_GAP_FILL_FALLBACK_MINUTES * 60
        min_start = now_ts - self._config.gap_fill_max_lookback_minutes * 60
        starts = {s: max(latest.get(s, fallback_start), min_start) for s in symbols}
        buckets: dict[int, list[str]] = {}
        for sym, start_ts in starts.items():
            buckets.setdefault(start_ts - (start_ts % 300), []).append(sym)
        total_rows = 0
        filled: set[str] = set()
        for bucket_start, bucket_symbols in sorted(buckets.items()):
            start_iso = datetime.fromtimestamp(bucket_start, tz=UTC).isoformat()
            bars_by_symbol = await self._market_client.bars(
                bucket_symbols, start_iso=start_iso, timeframe="1Min"
            )
            rows: list[tuple[Any, ...]] = []
            for symbol, bars in bars_by_symbol.items():
                sym_u = symbol.upper()
                sym_start = starts.get(sym_u, bucket_start)
                for bar in bars:
                    ts_us = _to_epoch_us(bar.get("t"))
                    if ts_us is None:
                        continue
                    ts_s = ts_us // 1_000_000
                    if ts_s < sym_start:
                        continue
                    rows.append(
                        (
                            sym_u,
                            "1min",
                            ts_s,
                            bar.get("o"),
                            bar.get("h"),
                            bar.get("l"),
                            bar.get("c"),
                            bar.get("v"),
                            bar.get("n"),
                            bar.get("vw"),
                        )
                    )
                if bars:
                    filled.add(sym_u)
            if rows:
                await self._market_store.upsert_bars(rows)
                total_rows += len(rows)
        if total_rows:
            log.info(
                "bar gap-fill upserted %d bars for %d symbols", total_rows, len(filled)
            )

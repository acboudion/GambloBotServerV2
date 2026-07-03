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
MAX_WATCHLIST_SYMBOLS = 100

BAR_GAP_FILL_FALLBACK_MINUTES = 60


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
            gap_fill_callback=self._bar_gap_fill if market_client is not None else None,
        )
        self._market_store = market_store
        self._market_client = market_client
        self._channels: list[str] = list(config.stock_channels)
        self._watchlist: set[str] = {s.upper() for s in config.stock_watchlist_symbols}
        self._acknowledged: dict[str, list[str]] | None = None
        self._watchlist_lock = asyncio.Lock()

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
            symbols = {
                str(s).upper() for s in persisted["symbols"] if str(s).strip()
            }
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
            if len(new) > MAX_WATCHLIST_SYMBOLS:
                return {
                    "error": "watchlist_too_large",
                    "max_allowed": MAX_WATCHLIST_SYMBOLS,
                    "requested": len(new),
                }
            added = new - current
            removed = current - new
            self._watchlist = new
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
            if ws is not None and (added or removed):
                try:
                    if removed:
                        await self._send_subscription(ws, "unsubscribe", sorted(removed))
                    if added:
                        await self._send_subscription(ws, "subscribe", sorted(added))
                    sent_delta = True
                except Exception as e:
                    log.warning("watchlist delta send failed (will apply on reconnect): %s", e)
            return {
                "watchlist": sorted(new),
                "channels": self._channels,
                "added": sorted(added),
                "removed": sorted(removed),
                "connected": ws is not None,
                "delta_sent": sent_delta,
            }

    async def _send_subscription(
        self, ws: Any, action: str, symbols: list[str]
    ) -> None:
        if not symbols:
            return
        message: dict[str, Any] = {"action": action}
        for channel in self._channels:
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
        # New session: the previous session's ack no longer describes what
        # Alpaca is delivering. Clear it so health/tools never report symbols
        # the current connection hasn't acknowledged (trust acks, not requests).
        self._acknowledged = None
        # Replay the full desired subscription; the ack arrives via the
        # receive loop (handle_item) so we don't block here.
        await self._send_subscription(ws, "subscribe", sorted(self._watchlist))
        self._state.update_health(
            self.stream_name,
            requested_subscription={c: sorted(self._watchlist) for c in self._channels},
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
            await asyncio.sleep(self._config.connection_limit_backoff_seconds)
            self._state.update_health(self.stream_name, connection_limit_blocked=False)
        elif code in (405, 409, 410):
            # Subscription-shaped failures (symbol limit, entitlement, invalid
            # action): alert + record, but keep the connection alive — never
            # crash-loop over a rejected channel.
            self._state.update_health(
                self.stream_name,
                entitlement_error=entitlement,
                last_error=f"{code} {msg}",
            )
        else:
            self._state.update_health(self.stream_name, last_error=f"{code} {msg}")

    async def on_dropped_item(self, kind: str, item: dict[str, Any]) -> None:
        if kind in ("s", "l", "raw"):
            # Halts/LULDs/raw audit events (corrections, cancelErrors,
            # unknowns) are rare, audit-preserved, and alert-relevant — a
            # quote burst overflowing the queue must not lose them. Persist
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
        self._update_snapshots(batch)
        await self._emit_market_alerts(rows["status_items"], rows["luld_items"])

    def _rows_from_batch(
        self, batch: list[tuple[str, dict[str, Any]]]
    ) -> dict[str, list[Any]]:
        trades: list[tuple[Any, ...]] = []
        quotes: list[tuple[Any, ...]] = []
        bars: list[tuple[Any, ...]] = []
        statuses: list[tuple[Any, ...]] = []
        lulds: list[tuple[Any, ...]] = []
        raw: list[tuple[str, str]] = []
        status_items: list[dict[str, Any]] = []
        luld_items: list[dict[str, Any]] = []

        for kind, item in batch:
            symbol = str(item.get("S") or "").upper()
            ts_us = _to_epoch_us(item.get("t"))
            if kind == "t" and symbol and ts_us is not None:
                trades.append(
                    (
                        symbol,
                        ts_us,
                        float(item.get("p") or 0.0),
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
        # sample bucket.
        sample_ms = self._config.stock_quote_sample_ms
        if sample_ms > 0 and quotes:
            bucketed: dict[tuple[str, int], tuple[Any, ...]] = {}
            for q in quotes:
                bucketed[(q[0], q[1] // (sample_ms * 1000))] = q
            quotes = sorted(bucketed.values(), key=lambda q: (q[0], q[1]))

        return {
            "trades": trades,
            "quotes": quotes,
            "bars": bars,
            "statuses": statuses,
            "lulds": lulds,
            "raw": raw,
            "status_items": status_items,
            "luld_items": luld_items,
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
            ts_us = _to_epoch_us(item.get("t"))
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
        tick data; bars carry the analytical signal)."""
        assert self._market_client is not None
        symbols = sorted(self._watchlist)
        if not symbols:
            return
        latest = (
            watermark
            if watermark is not None
            else await self._market_store.latest_bar_ts(symbols, timeframe="1min")
        )
        now = datetime.now(UTC)
        fallback_start = int(now.timestamp()) - BAR_GAP_FILL_FALLBACK_MINUTES * 60
        start_ts = min(
            [latest.get(s, fallback_start) for s in symbols], default=fallback_start
        )
        start_iso = datetime.fromtimestamp(start_ts, tz=UTC).isoformat()
        bars_by_symbol = await self._market_client.bars(
            symbols, start_iso=start_iso, timeframe="1Min"
        )
        rows: list[tuple[Any, ...]] = []
        for symbol, bars in bars_by_symbol.items():
            for bar in bars:
                ts_us = _to_epoch_us(bar.get("t"))
                if ts_us is None:
                    continue
                rows.append(
                    (
                        symbol.upper(),
                        "1min",
                        ts_us // 1_000_000,
                        bar.get("o"),
                        bar.get("h"),
                        bar.get("l"),
                        bar.get("c"),
                        bar.get("v"),
                        bar.get("n"),
                        bar.get("vw"),
                    )
                )
        if rows:
            await self._market_store.upsert_bars(rows)
            log.info("bar gap-fill upserted %d bars for %d symbols", len(rows), len(bars_by_symbol))

"""Deterministic alert generation.

Keyword groups are configurable: ALERT_KEYWORDS_FILE may point at a JSON file
of the shape {group: {"phrases": [...], "critical_phrases": [...],
"bullish": [...], "bearish": [...]}} which is deep-merged over the built-ins
(lists are unioned). Regexes are compiled once at engine construction.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from collections import deque
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from dateutil import parser as dateparser

from .logging_utils import get_logger
from .models import Alert, AlertCategory, NewsArticle, Severity

log = get_logger(__name__)

Direction = Literal["bullish", "bearish", "neutral"]

KEYWORDS: dict[str, list[str]] = {
    "mna": [
        "acquisition", "acquires", "merger", "takeover", "buyout",
        "strategic review", "deal", "offer",
    ],
    "financing": [
        "offering", "registered direct", "atm", "private placement",
        "convertible", "notes", "dilution", "shelf",
    ],
    "earnings": [
        "earnings", "revenue", "eps", "guidance", "preliminary results",
        "raises outlook", "cuts outlook", "misses", "beats",
    ],
    "legal_regulatory": [
        "sec", "doj", "ftc", "investigation", "subpoena", "lawsuit",
        "settlement", "fraud", "compliance",
    ],
    "analyst": [
        "upgrade", "downgrade", "initiates", "price target",
        "overweight", "underweight", "buy rating", "sell rating",
    ],
    "halt_risk": [
        "bankruptcy", "chapter 11", "delisting", "nasdaq notice",
        "going concern", "halted", "suspension",
    ],
    "corporate_action": [
        "stock split", "reverse split", "special dividend", "spin-off",
        "spinoff", "tender offer", "buyback", "share repurchase",
        "rights offering", "dividend increase", "dividend cut",
    ],
}

CATEGORY_MAP: dict[str, AlertCategory] = {
    "mna": "mna_keyword",
    "financing": "financing_keyword",
    "earnings": "earnings_keyword",
    "legal_regulatory": "legal_regulatory_keyword",
    "analyst": "analyst_keyword",
    "halt_risk": "halt_risk_keyword",
    "corporate_action": "corporate_action_keyword",
}

CRITICAL_PHRASES: set[str] = {
    "bankruptcy", "chapter 11", "delisting", "going concern",
    "halted", "suspension", "fraud",
}

HIGH_BASE_GROUPS: set[str] = {"mna", "earnings", "financing"}
MEDIUM_BASE_GROUPS: set[str] = {"analyst", "corporate_action"}

# Dedup window for infrastructure alerts (stream_error / coverage_gap /
# persist_failure): a stuck condition re-fires its error path every backoff
# cycle, and without a window the alert feed the bot polls fills with
# dozens of identical critical alerts per hour.
STREAM_ALERT_DEDUP_SECONDS = 900.0

BULLISH_TERMS: list[str] = [
    "beats", "raises outlook", "raises guidance", "upgrade", "overweight",
    "buy rating", "approval", "approved", "record revenue", "dividend increase",
    "buyback", "share repurchase", "wins", "awarded", "surges",
]
BEARISH_TERMS: list[str] = [
    "misses", "cuts outlook", "cuts guidance", "downgrade", "underweight",
    "sell rating", "investigation", "subpoena", "lawsuit", "fraud",
    "bankruptcy", "chapter 11", "delisting", "going concern", "dilution",
    "dividend cut", "recall", "plunges", "halted",
]


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = dateparser.isoparse(value)
    except (ValueError, TypeError):
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def _compile_phrases(phrases: list[str]) -> list[tuple[str, re.Pattern[str]]]:
    # Word-boundary-ish match (avoid matching "secured" for "sec") — but only
    # guard edges that are themselves word characters: a phrase ending in
    # punctuation ("alert:") would otherwise need a word char right after the
    # colon, so "Alert: headline" (colon then space) could never match.
    compiled: list[tuple[str, re.Pattern[str]]] = []
    for p in phrases:
        lowered = p.lower()
        if not lowered:
            continue
        prefix = r"\b" if re.match(r"\w", lowered[0]) else ""
        suffix = r"\b" if re.match(r"\w", lowered[-1]) else ""
        compiled.append((p, re.compile(prefix + re.escape(lowered) + suffix)))
    return compiled


def _match_compiled(
    text: str, compiled: list[tuple[str, re.Pattern[str]]]
) -> list[str]:
    return [p for p, pattern in compiled if pattern.search(text)]


KEYWORD_SPEC_KEYS = ("phrases", "critical_phrases", "bullish", "bearish")


def load_keyword_overrides(path: str) -> dict[str, dict[str, list[str]]]:
    """Parse an ALERT_KEYWORDS_FILE. Returns {} on any problem (logged)."""
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning("ALERT_KEYWORDS_FILE unusable (%s); using built-ins", e)
        return {}
    if not isinstance(raw, dict):
        log.warning("ALERT_KEYWORDS_FILE must be a JSON object; using built-ins")
        return {}
    return clean_keyword_overrides(raw, source="ALERT_KEYWORDS_FILE")


def clean_keyword_overrides(
    raw: dict[str, Any], *, source: str = "keyword overrides"
) -> dict[str, dict[str, list[str]]]:
    """Validate a {group: {phrases/critical_phrases/bullish/bearish}} dict.
    Unknown keys are warned about instead of silently dropped — a typo like
    'phrase' would otherwise be an invisible no-op."""
    out: dict[str, dict[str, list[str]]] = {}
    for group, spec in raw.items():
        if not isinstance(spec, dict):
            log.warning("%s: group %r is not an object (ignored)", source, group)
            continue
        cleaned: dict[str, list[str]] = {}
        for key, values in spec.items():
            if key not in KEYWORD_SPEC_KEYS:
                log.warning(
                    "%s: group %r has unknown key %r (ignored; valid: %s)",
                    source,
                    group,
                    key,
                    ", ".join(KEYWORD_SPEC_KEYS),
                )
                continue
            if isinstance(values, list):
                cleaned[key] = [str(v).lower() for v in values if str(v).strip()]
            else:
                log.warning(
                    "%s: group %r key %r is not a list (ignored)", source, group, key
                )
        out[str(group)] = cleaned
    return out


class AlertEngine:
    def __init__(
        self,
        *,
        high_latency_alert_ms: int = 30_000,
        halt_alert_dedup_seconds: int = 300,
        keywords_file: str | None = None,
        rate_limit_per_symbol_hour: int = 0,
    ) -> None:
        self.high_latency_alert_ms = high_latency_alert_ms
        self.halt_alert_dedup_seconds = halt_alert_dedup_seconds
        self.rate_limit_per_symbol_hour = rate_limit_per_symbol_hour
        self.suppressed_alerts = 0
        # (symbol, category) -> monotonic timestamp of last emitted alert
        self._recent_market_alerts: dict[tuple[str, str], float] = {}
        # (stream, code) -> monotonic timestamp of last infrastructure alert
        self._recent_stream_alerts: dict[tuple[str, str], float] = {}
        # (symbols-key, category) -> deque of monotonic emit times (rate limit)
        self._emit_times: dict[tuple[str, str], deque[float]] = {}
        # Provisional charges from an open batch transaction (see
        # count_emission(pending=True) / commit_pending / discard_pending).
        self._pending_emit: dict[tuple[str, str], int] = {}
        # alert_id -> under-quota symbols to charge (set by _admit; consumed
        # by count_emission). An alert carries its FULL symbol list, but only
        # the symbols that were under quota pay for it.
        self._charge_symbols: dict[str, list[str]] = {}

        self._keywords_file = keywords_file
        # Runtime-managed groups (set via MCP tools, persisted by the caller).
        # Merged AFTER the file overrides so the bot can extend or amend them
        # intraday without a restart.
        self._runtime_overrides: dict[str, dict[str, list[str]]] = {}
        self.rebuild()

    def rebuild(
        self, runtime_overrides: dict[str, dict[str, list[str]]] | None = None
    ) -> None:
        """(Re)compile keyword groups from built-ins + ALERT_KEYWORDS_FILE +
        runtime overrides. Everything is computed into locals first and
        swapped in with plain attribute assignment at the end — evaluation
        runs on the same event loop, so a rebuild mid-stream can never
        expose a half-compiled state."""
        if runtime_overrides is not None:
            self._runtime_overrides = runtime_overrides
        keywords = {group: list(phrases) for group, phrases in KEYWORDS.items()}
        critical = set(CRITICAL_PHRASES)
        bullish = list(BULLISH_TERMS)
        bearish = list(BEARISH_TERMS)
        override_layers: list[dict[str, dict[str, list[str]]]] = []
        if self._keywords_file:
            override_layers.append(load_keyword_overrides(self._keywords_file))
        override_layers.append(self._runtime_overrides)
        for layer in override_layers:
            for group, spec in layer.items():
                merged = keywords.setdefault(group, [])
                for phrase in spec.get("phrases", []):
                    if phrase not in merged:
                        merged.append(phrase)
                # Critical phrases must also be matchable: escalation only
                # consults the critical set AFTER a group phrase matched, so
                # a critical phrase absent from phrases was a silent no-op.
                for phrase in spec.get("critical_phrases", []):
                    critical.add(phrase)
                    if phrase not in merged:
                        merged.append(phrase)
                for term in spec.get("bullish", []):
                    if term not in bullish:
                        bullish.append(term)
                for term in spec.get("bearish", []):
                    if term not in bearish:
                        bearish.append(term)
        compiled_groups = {
            group: _compile_phrases(phrases) for group, phrases in keywords.items()
        }
        compiled_bullish = _compile_phrases(bullish)
        compiled_bearish = _compile_phrases(bearish)
        compiled_breaking = _compile_phrases(["breaking", "alert:", "developing"])
        # Atomic swap.
        self.critical_phrases = critical
        self._compiled_groups = compiled_groups
        self._compiled_bullish = compiled_bullish
        self._compiled_bearish = compiled_bearish
        self._compiled_breaking = compiled_breaking

    def runtime_keyword_groups(self) -> dict[str, dict[str, list[str]]]:
        return {g: {k: list(v) for k, v in spec.items()} for g, spec in self._runtime_overrides.items()}

    def set_runtime_group(self, group: str, spec: dict[str, list[str]]) -> None:
        overrides = self.runtime_keyword_groups()
        overrides[group] = spec
        self.rebuild(overrides)

    def delete_runtime_group(self, group: str) -> bool:
        overrides = self.runtime_keyword_groups()
        if group not in overrides:
            return False
        del overrides[group]
        self.rebuild(overrides)
        return True

    def keyword_group_summary(self) -> dict[str, dict[str, Any]]:
        """Current effective groups: which are built-in, which category each
        maps to, and every matchable phrase."""
        out: dict[str, dict[str, Any]] = {}
        for group, compiled in self._compiled_groups.items():
            out[group] = {
                "builtin": group in KEYWORDS,
                "runtime_override": group in self._runtime_overrides,
                "category": CATEGORY_MAP.get(group, "custom_keyword"),
                "phrases": [p for p, _ in compiled],
            }
        return out

    def direction_of(self, text_lower: str) -> Direction:
        bull = len(_match_compiled(text_lower, self._compiled_bullish))
        bear = len(_match_compiled(text_lower, self._compiled_bearish))
        if bull > bear:
            return "bullish"
        if bear > bull:
            return "bearish"
        return "neutral"

    @staticmethod
    def _quota_keys(symbols: list[str], category: str) -> list[tuple[str, str]]:
        """One bucket per individual symbol — a co-mention (AAPL+MSFT) checks
        and charges every mentioned symbol's bucket, so noisy co-mentions
        can't route around a single symbol's hourly cap."""
        if not symbols:
            return [("", category)]
        return [(s, category) for s in sorted({s.upper() for s in symbols})]

    def _admit(
        self, symbols: list[str], category: str, severity: Severity
    ) -> list[str] | None:
        """Per-(symbol, category) hourly cap check. Returns the under-quota
        symbols to charge — the alert is emitted with its FULL symbol list —
        or None when every mentioned symbol is over quota. Suppressing on
        ANY over-quota symbol would let one hot co-mentioned ticker (SPY/QQQ
        in the default interest list) starve alerts for quiet symbols in the
        same story. Critical alerts bypass the quota. Checks only — quota is
        charged via count_emission() once the alert is actually inserted, so
        store-level dedup (same content hash) doesn't burn quota."""
        if self.rate_limit_per_symbol_hour <= 0 or severity == "critical":
            return [key[0] for key in self._quota_keys(symbols, category)]
        now = time.monotonic()
        admitted: list[str] = []
        for key in self._quota_keys(symbols, category):
            times = self._emit_times.setdefault(key, deque())
            while times and now - times[0] > 3600:
                times.popleft()
            used = len(times) + self._pending_emit.get(key, 0)
            if used < self.rate_limit_per_symbol_hour:
                admitted.append(key[0])
        if not admitted:
            self.suppressed_alerts += 1
            return None
        return admitted

    def _register_admitted(self, alert: Alert, admitted: list[str]) -> Alert:
        """Remember which symbols pay for this alert once it is inserted."""
        if len(self._charge_symbols) > 4096:  # leak guard for deduped alerts
            self._charge_symbols.clear()
        self._charge_symbols[alert.alert_id] = admitted
        return alert

    def count_emission(self, alert: Alert, *, pending: bool = False) -> None:
        """Charge the hourly quota for an alert that was actually inserted.
        Persisters call this after Store.record_alert returns True. Only the
        symbols _admit() returned as under-quota are charged.

        pending=True records a provisional charge (used by the batched news
        persister while its transaction is still open): it counts against
        _admit() for later articles in the same batch, and is either
        promoted by commit_pending() once the batch commits or dropped by
        discard_pending() on rollback."""
        charge = self._charge_symbols.pop(alert.alert_id, None)
        if self.rate_limit_per_symbol_hour <= 0 or alert.severity == "critical":
            return
        now = time.monotonic()
        keys: list[tuple[str, str]]
        if charge is not None:
            keys = [(s, alert.category) for s in dict.fromkeys(charge)]
        else:
            keys = self._quota_keys(alert.symbols, alert.category)
        for key in keys:
            if pending:
                self._pending_emit[key] = self._pending_emit.get(key, 0) + 1
            else:
                self._emit_times.setdefault(key, deque()).append(now)

    def commit_pending(self) -> None:
        """Promote provisional charges to committed quota (batch committed)."""
        now = time.monotonic()
        for key, count in self._pending_emit.items():
            times = self._emit_times.setdefault(key, deque())
            times.extend(now for _ in range(count))
        self._pending_emit.clear()

    def discard_pending(self) -> None:
        """Drop provisional charges (batch rolled back — nothing was emitted)."""
        self._pending_emit.clear()

    def stream_alert_deduped(self, stream: str, code: int | str) -> bool:
        """True when an identical (stream, code) infrastructure alert fired
        within STREAM_ALERT_DEDUP_SECONDS. Health flags stay the continuous
        signal; the alert feed gets at most one entry per window."""
        now = time.monotonic()
        key = (stream, str(code))
        last = self._recent_stream_alerts.get(key)
        if last is not None and now - last < STREAM_ALERT_DEDUP_SECONDS:
            return True
        self._recent_stream_alerts[key] = now
        return False

    def _market_alert_deduped(self, symbol: str, category: str) -> bool:
        """True when an identical (symbol, category) alert fired recently."""
        now = time.monotonic()
        key = (symbol.upper(), category)
        last = self._recent_market_alerts.get(key)
        if last is not None and now - last < self.halt_alert_dedup_seconds:
            return True
        self._recent_market_alerts[key] = now
        return False

    def status_alert(
        self,
        *,
        symbol: str,
        status_code: str | None,
        status_msg: str | None,
        reason_code: str | None,
        reason_msg: str | None,
        important: bool,
    ) -> Alert | None:
        """Trading-status change → trading_halt / trading_resume alert.
        Returns None for uninteresting statuses or recent duplicates."""
        code = (status_code or "").upper()
        msg = (status_msg or "").lower()
        if code == "H" or "halt" in msg or "pause" in msg:
            category: AlertCategory = "trading_halt"
            severity: Severity = "critical" if important else "high"
        elif code in ("T", "R") or "resum" in msg or "trading" in msg:
            category = "trading_resume"
            severity = "medium"
        else:
            return None
        if self._market_alert_deduped(symbol, category):
            return None
        detail = "; ".join(
            p for p in (status_msg, reason_msg or reason_code) if p
        ) or code
        return Alert(
            alert_id=str(uuid.uuid4()),
            article_id=None,
            created_at=_utcnow_iso(),
            severity=severity,
            category=category,
            symbols=[symbol.upper()],
            headline=None,
            reason=f"{symbol.upper()} trading status: {detail}",
            acknowledged=False,
        )

    def luld_alert(
        self,
        *,
        symbol: str,
        limit_up: float | None,
        limit_down: float | None,
        indicator: str | None,
    ) -> Alert | None:
        if self._market_alert_deduped(symbol, "luld"):
            return None
        return Alert(
            alert_id=str(uuid.uuid4()),
            article_id=None,
            created_at=_utcnow_iso(),
            severity="medium",
            category="luld",
            symbols=[symbol.upper()],
            headline=None,
            reason=(
                f"{symbol.upper()} LULD band update: "
                f"limit_up={limit_up} limit_down={limit_down} indicator={indicator}"
            ),
            acknowledged=False,
        )

    def evaluate_article(
        self,
        article: NewsArticle,
        *,
        interest_symbols: set[str],
        suppress_high_latency: bool = False,
    ) -> list[Alert]:
        alerts: list[Alert] = []
        text_parts = [article.headline or "", article.summary or "", article.content_text or ""]
        text_lower = " ".join(text_parts).lower()
        interest_hits = sorted(set(article.symbols) & interest_symbols)

        direction = self.direction_of(text_lower)

        if interest_hits:
            admitted = self._admit(interest_hits, "held_or_interested_symbol", "high")
            if admitted is not None:
                alerts.append(
                    self._register_admitted(
                        self._mk_alert(
                            article=article,
                            severity="high",
                            category="held_or_interested_symbol",
                            symbols=interest_hits,
                            reason=f"article mentions interest symbol(s): {', '.join(interest_hits)}",
                            direction=direction,
                        ),
                        admitted,
                    )
                )

        # Keyword groups (compiled at engine construction)
        for group, compiled in self._compiled_groups.items():
            matched = _match_compiled(text_lower, compiled)
            if not matched:
                continue
            severity: Severity
            if any(p in self.critical_phrases for p in matched):
                severity = "critical"
            elif group in HIGH_BASE_GROUPS:
                severity = "high"
            elif interest_hits:
                severity = "high"
            elif group in MEDIUM_BASE_GROUPS or group not in CATEGORY_MAP:
                # User-defined groups exist because the operator/bot cares —
                # they must not be demoted to low like unmatched noise.
                severity = "medium"
            else:
                severity = "low"

            if interest_hits and severity == "low":
                severity = "medium"
            if interest_hits and severity == "medium":
                severity = "high"

            category = CATEGORY_MAP.get(group, "custom_keyword")
            # Full symbol list: collapsing to interest hits misattributed
            # multi-symbol stories (the interest bump is in severity, not
            # in which symbols the alert names).
            symbols = list(article.symbols) or interest_hits
            admitted = self._admit(symbols, category, severity)
            if admitted is None:
                continue
            alerts.append(
                self._register_admitted(
                    self._mk_alert(
                        article=article,
                        severity=severity,
                        category=category,
                        symbols=symbols,
                        reason=f"keyword match in {group}: {', '.join(sorted(set(matched)))}",
                        direction=direction,
                    ),
                    admitted,
                )
            )

        # Generic breaking flag
        if _match_compiled(text_lower, self._compiled_breaking):
            severity = "high" if interest_hits else "medium"
            symbols = list(article.symbols) or interest_hits
            admitted = self._admit(symbols, "breaking_keyword", severity)
            if admitted is not None:
                alerts.append(
                    self._register_admitted(
                        self._mk_alert(
                            article=article,
                            severity=severity,
                            category="breaking_keyword",
                            symbols=symbols,
                            reason="article tagged as breaking/developing",
                            direction=direction,
                        ),
                        admitted,
                    )
                )

        # High-latency: pipeline delay computed from the article's own
        # timestamps (receipt minus newest of created/updated). The stored
        # latency_ms is frozen at first ingest against created_at only, so
        # a REST-frozen value — or an update to an old article — would keep
        # re-flagging articles that actually arrived promptly. Suppressed
        # for ingestion paths that replay history (REST backfill).
        if not suppress_high_latency:
            eff_latency = self._effective_latency_ms(article)
            if eff_latency is not None and eff_latency >= self.high_latency_alert_ms:
                admitted = self._admit(list(article.symbols), "high_latency", "medium")
                if admitted is not None:
                    alerts.append(
                        self._register_admitted(
                            self._mk_alert(
                                article=article,
                                severity="medium",
                                category="high_latency",
                                symbols=list(article.symbols),
                                reason=(
                                    f"latency_ms={eff_latency} exceeds threshold "
                                    f"{self.high_latency_alert_ms}"
                                ),
                            ),
                            admitted,
                        )
                    )

        return alerts

    def _effective_latency_ms(self, article: NewsArticle) -> int | None:
        """Receipt time minus the newest content timestamp, falling back to
        the stored latency_ms when timestamps are unparseable."""
        received = _parse_iso(article.last_seen_at)
        candidates = [
            d
            for d in (_parse_iso(article.created_at), _parse_iso(article.updated_at))
            if d is not None
        ]
        if received is None or not candidates:
            return article.latency_ms
        return int((received - max(candidates)).total_seconds() * 1000)

    def watched_story_alert(self, article: NewsArticle) -> Alert:
        """An update landed on a story the bot explicitly watches.

        article_id stays None: the (article_id, category) unique index
        dedups repeat categories per article, and a watched story must be
        allowed to alert on EVERY meaningful update — the id travels in the
        reason instead."""
        return Alert(
            alert_id=str(uuid.uuid4()),
            article_id=None,
            created_at=_utcnow_iso(),
            severity="high",
            category="watched_story_update",
            symbols=sorted(set(article.symbols)),
            headline=article.headline,
            reason=(
                f"watched story updated (article_id={article.id}, "
                f"update #{article.update_count})"
            ),
            acknowledged=False,
            direction=self.direction_of(
                " ".join(
                    filter(None, [article.headline, article.summary])
                ).lower()
            ),
        )

    def stream_stale_alert(self, *, stream: str, idle_seconds: float) -> Alert:
        return Alert(
            alert_id=str(uuid.uuid4()),
            article_id=None,
            created_at=_utcnow_iso(),
            severity="high",
            category="stream_stale",
            symbols=[],
            headline=None,
            reason=(
                f"{stream} stream received no messages for {idle_seconds:.0f}s; "
                "watchdog forced a reconnect"
            ),
            acknowledged=False,
        )

    def gap_fill_failure_alert(
        self, *, stream: str, attempt: int, exhausted: bool, error: str
    ) -> Alert:
        return Alert(
            alert_id=str(uuid.uuid4()),
            article_id=None,
            created_at=_utcnow_iso(),
            severity="critical" if exhausted else "high",
            category="gap_fill_failure",
            symbols=[],
            headline=None,
            reason=(
                f"{stream} reconnect gap-fill failed (attempt {attempt}"
                f"{', retries exhausted — articles during the disconnect may be missing'
                   if exhausted else ''}): {error}"
            ),
            acknowledged=False,
        )

    def persist_failure_alert(
        self, *, stream: str, batch_size: int, error: str
    ) -> Alert:
        return Alert(
            alert_id=str(uuid.uuid4()),
            article_id=None,
            created_at=_utcnow_iso(),
            severity="critical",
            category="persist_failure",
            symbols=[],
            headline=None,
            reason=(
                f"{stream} persister failed twice; {batch_size} item(s) routed "
                f"to recovery — some data may be lost: {error}"
            ),
            acknowledged=False,
        )

    def coverage_gap_alert(self, *, stream: str, from_iso: str, to_iso: str) -> Alert:
        return Alert(
            alert_id=str(uuid.uuid4()),
            article_id=None,
            created_at=_utcnow_iso(),
            severity="medium",
            category="coverage_gap",
            symbols=[],
            headline=None,
            reason=(
                f"{stream} stream disconnected with REST backfill disabled; "
                f"data between {from_iso} and {to_iso} may be missing"
            ),
            acknowledged=False,
        )

    def stream_error_alert(
        self, *, code: int, message: str, entitlement: bool = False
    ) -> Alert:
        category: AlertCategory = "entitlement_error" if entitlement else "stream_error"
        severity: Severity = "critical" if code in (402, 406, 409) else "high"
        return Alert(
            alert_id=str(uuid.uuid4()),
            article_id=None,
            created_at=_utcnow_iso(),
            severity=severity,
            category=category,
            symbols=[],
            headline=None,
            reason=f"stream error code={code} msg={message}",
            acknowledged=False,
        )

    @staticmethod
    def _mk_alert(
        *,
        article: NewsArticle,
        severity: Severity,
        category: AlertCategory,
        symbols: list[str],
        reason: str,
        direction: Direction = "neutral",
    ) -> Alert:
        return Alert(
            alert_id=str(uuid.uuid4()),
            article_id=article.id,
            created_at=_utcnow_iso(),
            severity=severity,
            category=category,
            symbols=sorted(set(symbols)),
            headline=article.headline,
            reason=reason,
            acknowledged=False,
            direction=direction,
        )

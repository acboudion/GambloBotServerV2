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
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

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


def _compile_phrases(phrases: list[str]) -> list[tuple[str, re.Pattern[str]]]:
    # word-boundary-ish match: avoid matching "secured" for "sec"
    return [
        (p, re.compile(r"\b" + re.escape(p.lower()) + r"\b")) for p in phrases
    ]


def _match_compiled(
    text: str, compiled: list[tuple[str, re.Pattern[str]]]
) -> list[str]:
    return [p for p, pattern in compiled if pattern.search(text)]


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
    out: dict[str, dict[str, list[str]]] = {}
    for group, spec in raw.items():
        if not isinstance(spec, dict):
            continue
        cleaned: dict[str, list[str]] = {}
        for key in ("phrases", "critical_phrases", "bullish", "bearish"):
            values = spec.get(key)
            if isinstance(values, list):
                cleaned[key] = [str(v).lower() for v in values if str(v).strip()]
        out[str(group)] = cleaned
    return out


@dataclass
class _ArticleAlertContext:
    article: NewsArticle
    text_lower: str
    interest_hits: list[str]


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
        # (symbols-key, category) -> deque of monotonic emit times (rate limit)
        self._emit_times: dict[tuple[str, str], deque[float]] = {}

        keywords = {group: list(phrases) for group, phrases in KEYWORDS.items()}
        critical = set(CRITICAL_PHRASES)
        bullish = list(BULLISH_TERMS)
        bearish = list(BEARISH_TERMS)
        if keywords_file:
            for group, spec in load_keyword_overrides(keywords_file).items():
                merged = keywords.setdefault(group, [])
                for phrase in spec.get("phrases", []):
                    if phrase not in merged:
                        merged.append(phrase)
                critical.update(spec.get("critical_phrases", []))
                for term in spec.get("bullish", []):
                    if term not in bullish:
                        bullish.append(term)
                for term in spec.get("bearish", []):
                    if term not in bearish:
                        bearish.append(term)
        self.critical_phrases = critical
        # Compile once; per-article evaluation only runs the compiled patterns.
        self._compiled_groups: dict[str, list[tuple[str, re.Pattern[str]]]] = {
            group: _compile_phrases(phrases) for group, phrases in keywords.items()
        }
        self._compiled_bullish = _compile_phrases(bullish)
        self._compiled_bearish = _compile_phrases(bearish)
        self._compiled_breaking = _compile_phrases(["breaking", "alert:", "developing"])

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

    def _rate_limited(
        self, symbols: list[str], category: str, severity: Severity
    ) -> bool:
        """Per-(symbol, category) hourly cap check; suppress when ANY mentioned
        symbol is over quota. Critical alerts exempt. Checks only — quota is
        charged via count_emission() once the alert is actually inserted, so
        store-level dedup (same content hash) doesn't burn quota and suppress
        later distinct alerts."""
        if self.rate_limit_per_symbol_hour <= 0 or severity == "critical":
            return False
        now = time.monotonic()
        for key in self._quota_keys(symbols, category):
            times = self._emit_times.setdefault(key, deque())
            while times and now - times[0] > 3600:
                times.popleft()
            if len(times) >= self.rate_limit_per_symbol_hour:
                self.suppressed_alerts += 1
                return True
        return False

    def count_emission(self, alert: Alert) -> None:
        """Charge the hourly quota for an alert that was actually inserted.
        Persisters call this after Store.record_alert returns True."""
        if self.rate_limit_per_symbol_hour <= 0 or alert.severity == "critical":
            return
        now = time.monotonic()
        for key in self._quota_keys(alert.symbols, alert.category):
            self._emit_times.setdefault(key, deque()).append(now)

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

        ctx = _ArticleAlertContext(
            article=article,
            text_lower=text_lower,
            interest_hits=interest_hits,
        )
        direction = self.direction_of(text_lower)

        if interest_hits and not self._rate_limited(
            interest_hits, "held_or_interested_symbol", "high"
        ):
            alerts.append(
                self._mk_alert(
                    article=article,
                    severity="high",
                    category="held_or_interested_symbol",
                    symbols=interest_hits,
                    reason=f"article mentions interest symbol(s): {', '.join(interest_hits)}",
                    direction=direction,
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
            else:
                severity = "medium" if group in MEDIUM_BASE_GROUPS else "low"

            if interest_hits and severity == "low":
                severity = "medium"
            if interest_hits and severity == "medium":
                severity = "high"

            category = CATEGORY_MAP.get(group, "breaking_keyword")
            symbols = ctx.interest_hits or list(article.symbols)
            if self._rate_limited(symbols, category, severity):
                continue
            alerts.append(
                self._mk_alert(
                    article=article,
                    severity=severity,
                    category=category,
                    symbols=symbols,
                    reason=f"keyword match in {group}: {', '.join(sorted(set(matched)))}",
                    direction=direction,
                )
            )

        # Generic breaking flag
        if _match_compiled(text_lower, self._compiled_breaking):
            severity = "high" if interest_hits else "medium"
            symbols = interest_hits or list(article.symbols)
            if not self._rate_limited(symbols, "breaking_keyword", severity):
                alerts.append(
                    self._mk_alert(
                        article=article,
                        severity=severity,
                        category="breaking_keyword",
                        symbols=symbols,
                        reason="article tagged as breaking/developing",
                        direction=direction,
                    )
                )

        # High-latency. Suppressed for ingestion paths where `latency_ms` does
        # not reflect real-time pipeline delay (e.g. REST backfill, where
        # `created_at` may be hours/days old).
        if (
            not suppress_high_latency
            and article.latency_ms is not None
            and article.latency_ms >= self.high_latency_alert_ms
            and not self._rate_limited(
                list(article.symbols), "high_latency", "medium"
            )
        ):
            alerts.append(
                self._mk_alert(
                    article=article,
                    severity="medium",
                    category="high_latency",
                    symbols=list(article.symbols),
                    reason=f"latency_ms={article.latency_ms} exceeds threshold {self.high_latency_alert_ms}",
                )
            )

        return alerts

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

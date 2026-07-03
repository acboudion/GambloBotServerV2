"""Deterministic alert generation."""

from __future__ import annotations

import re
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from .models import Alert, AlertCategory, NewsArticle, Severity

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
}

CATEGORY_MAP: dict[str, AlertCategory] = {
    "mna": "mna_keyword",
    "financing": "financing_keyword",
    "earnings": "earnings_keyword",
    "legal_regulatory": "legal_regulatory_keyword",
    "analyst": "analyst_keyword",
    "halt_risk": "halt_risk_keyword",
}

CRITICAL_PHRASES: set[str] = {
    "bankruptcy", "chapter 11", "delisting", "going concern",
    "halted", "suspension", "fraud",
}

HIGH_BASE_GROUPS: set[str] = {"mna", "earnings", "financing"}


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def _contains_any(text: str, phrases: list[str]) -> list[str]:
    matched: list[str] = []
    for p in phrases:
        # word-boundary-ish match: avoid matching "secured" for "sec"
        pattern = r"\b" + re.escape(p.lower()) + r"\b"
        if re.search(pattern, text):
            matched.append(p)
    return matched


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
    ) -> None:
        self.high_latency_alert_ms = high_latency_alert_ms
        self.halt_alert_dedup_seconds = halt_alert_dedup_seconds
        # (symbol, category) -> monotonic timestamp of last emitted alert
        self._recent_market_alerts: dict[tuple[str, str], float] = {}

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

        if interest_hits:
            alerts.append(
                self._mk_alert(
                    article=article,
                    severity="high",
                    category="held_or_interested_symbol",
                    symbols=interest_hits,
                    reason=f"article mentions interest symbol(s): {', '.join(interest_hits)}",
                )
            )

        # Keyword groups
        for group, phrases in KEYWORDS.items():
            matched = _contains_any(text_lower, phrases)
            if not matched:
                continue
            severity: Severity
            if any(p in CRITICAL_PHRASES for p in matched):
                severity = "critical"
            elif group in HIGH_BASE_GROUPS:
                severity = "high"
            elif interest_hits:
                severity = "high"
            else:
                severity = "medium" if group == "analyst" else "low"

            if interest_hits and severity == "low":
                severity = "medium"
            if interest_hits and severity == "medium":
                severity = "high"

            alerts.append(
                self._mk_alert(
                    article=article,
                    severity=severity,
                    category=CATEGORY_MAP[group],
                    symbols=ctx.interest_hits or list(article.symbols),
                    reason=f"keyword match in {group}: {', '.join(sorted(set(matched)))}",
                )
            )

        # Generic breaking flag
        breaking_phrases = ["breaking", "alert:", "developing"]
        if _contains_any(text_lower, breaking_phrases):
            alerts.append(
                self._mk_alert(
                    article=article,
                    severity="high" if interest_hits else "medium",
                    category="breaking_keyword",
                    symbols=interest_hits or list(article.symbols),
                    reason="article tagged as breaking/developing",
                )
            )

        # High-latency. Suppressed for ingestion paths where `latency_ms` does
        # not reflect real-time pipeline delay (e.g. REST backfill, where
        # `created_at` may be hours/days old).
        if (
            not suppress_high_latency
            and article.latency_ms is not None
            and article.latency_ms >= self.high_latency_alert_ms
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
        )

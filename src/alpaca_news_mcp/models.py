"""Pydantic v2 models for Alpaca news, alerts, stream health."""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field

Severity = Literal["critical", "high", "medium", "low"]
AlertCategory = Literal[
    "held_or_interested_symbol",
    "breaking_keyword",
    "corporate_action_keyword",
    "earnings_keyword",
    "analyst_keyword",
    "legal_regulatory_keyword",
    "financing_keyword",
    "mna_keyword",
    "halt_risk_keyword",
    "high_latency",
    "stream_error",
    "entitlement_error",
]


class NewsArticle(BaseModel):
    id: int
    headline: str
    summary: str | None = None
    author: str | None = None
    created_at: str | None = None
    updated_at: str | None = None
    content_html: str | None = None
    content_text: str | None = None
    url: str | None = None
    source: str | None = None
    symbols: list[str] = Field(default_factory=list)
    first_seen_at: str
    last_seen_at: str
    last_seen_source: str
    update_count: int = 0
    latency_ms: int | None = None
    is_content_present: bool = False
    raw: dict[str, Any] | None = None


class NewsArticleVersion(BaseModel):
    version_id: str
    article_id: int
    updated_at: str | None = None
    received_at: str
    source: str
    headline: str | None = None
    summary: str | None = None
    content_hash: str | None = None


class Alert(BaseModel):
    alert_id: str
    article_id: int | None = None
    created_at: str
    severity: Severity
    category: AlertCategory
    symbols: list[str] = Field(default_factory=list)
    headline: str | None = None
    reason: str
    acknowledged: bool = False


class SubscriptionState(BaseModel):
    requested: dict[str, list[str]] = Field(default_factory=dict)
    acknowledged: dict[str, list[str]] | None = None
    mode: Literal["wildcard", "fallback", "rest_only", "disconnected"] = "disconnected"
    last_ack_at: str | None = None
    notes: str | None = None


class StreamHealth(BaseModel):
    service: str = "alpaca-news-mcp"
    connected: bool = False
    authenticated: bool = False
    subscription_mode: str = "disconnected"
    requested_subscription: dict[str, list[str]] | None = None
    acknowledged_subscription: dict[str, list[str]] | None = None
    last_message_at: str | None = None
    last_article_at: str | None = None
    last_error: str | None = None
    connection_attempts: int = 0
    reconnect_count: int = 0
    connection_limit_blocked: bool = False
    entitlement_error: bool = False
    rest_backfill_enabled: bool = False
    article_count: int = 0
    alert_count: int = 0
    articles_dropped: int = 0


class LatencyStats(BaseModel):
    window_minutes: int
    sample_count: int
    p50_ms: int | None
    p90_ms: int | None
    p99_ms: int | None
    max_ms: int | None
    avg_ms: int | None


class IngestionStats(BaseModel):
    window_minutes: int
    article_count: int
    version_count: int
    distinct_symbols: int
    distinct_sources: int
    raw_event_count: int


class LimitExceededError(BaseModel):
    error: Literal["limit_exceeded"] = "limit_exceeded"
    max_allowed: int
    requested: int

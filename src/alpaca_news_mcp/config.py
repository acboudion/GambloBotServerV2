"""Environment-driven configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Final


def _get_str(key: str, default: str | None = None, *, required: bool = False) -> str:
    val = os.getenv(key, default)
    if required and not val:
        raise RuntimeError(f"Required environment variable missing: {key}")
    return val or ""


def _get_int(key: str, default: int) -> int:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError as e:
        raise RuntimeError(f"Invalid integer for {key}: {raw!r}") from e


def _get_float(key: str, default: float) -> float:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as e:
        raise RuntimeError(f"Invalid float for {key}: {raw!r}") from e


def _get_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    if raw is None or raw == "":
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "y", "t")


def _get_csv(key: str, default: str = "") -> list[str]:
    raw = os.getenv(key, default)
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


VALID_SUBSCRIPTION_MODES: Final = ("wildcard", "fallback")


@dataclass(frozen=True)
class Config:
    alpaca_api_key: str
    alpaca_secret_key: str

    alpaca_news_stream_url: str
    alpaca_data_base_url: str

    mcp_host: str
    mcp_port: int
    mcp_path: str

    storage_path: str
    log_level: str

    news_subscription_mode: str
    news_fallback_symbols: list[str]
    news_interest_symbols: list[str]

    enable_rest_backfill: bool
    backfill_lookback_minutes: int
    rest_backfill_overlap_seconds: int
    rest_include_content: bool
    rest_exclude_contentless: bool

    max_recent_articles_memory: int
    event_retention_days: int
    raw_event_retention_days: int
    retention_interval_seconds: int

    reconnect_min_seconds: int
    reconnect_max_seconds: int
    connection_limit_backoff_seconds: int

    queue_maxsize: int
    slow_client_warning_queue_depth: int
    queue_backpressure_seconds: float

    enable_manual_rest_backfill: bool

    high_latency_alert_ms: int = 30_000

    extra_env_seen: dict[str, str] = field(default_factory=dict, repr=False)

    @classmethod
    def from_env(cls) -> Config:
        sub_mode = _get_str("NEWS_SUBSCRIPTION_MODE", "wildcard").lower()
        if sub_mode not in VALID_SUBSCRIPTION_MODES:
            raise RuntimeError(
                f"Invalid NEWS_SUBSCRIPTION_MODE={sub_mode!r}; must be one of {VALID_SUBSCRIPTION_MODES}"
            )
        return cls(
            alpaca_api_key=_get_str("ALPACA_API_KEY", required=True),
            alpaca_secret_key=_get_str("ALPACA_SECRET_KEY", required=True),
            alpaca_news_stream_url=_get_str(
                "ALPACA_NEWS_STREAM_URL",
                "wss://stream.data.alpaca.markets/v1beta1/news",
            ),
            alpaca_data_base_url=_get_str(
                "ALPACA_DATA_BASE_URL", "https://data.alpaca.markets"
            ),
            mcp_host=_get_str("MCP_HOST", "0.0.0.0"),
            mcp_port=_get_int("MCP_PORT", 8000),
            mcp_path=_get_str("MCP_PATH", "/mcp"),
            storage_path=_get_str("STORAGE_PATH", "/data/alpaca_news.sqlite"),
            log_level=_get_str("LOG_LEVEL", "info").lower(),
            news_subscription_mode=sub_mode,
            news_fallback_symbols=_get_csv("NEWS_FALLBACK_SYMBOLS"),
            news_interest_symbols=_get_csv(
                "NEWS_INTEREST_SYMBOLS", "AAPL,MSFT,NVDA,TSLA,SPY,QQQ"
            ),
            enable_rest_backfill=_get_bool("ENABLE_REST_BACKFILL", True),
            backfill_lookback_minutes=_get_int("BACKFILL_LOOKBACK_MINUTES", 60),
            rest_backfill_overlap_seconds=_get_int("REST_BACKFILL_OVERLAP_SECONDS", 180),
            rest_include_content=_get_bool("REST_INCLUDE_CONTENT", True),
            rest_exclude_contentless=_get_bool("REST_EXCLUDE_CONTENTLESS", False),
            max_recent_articles_memory=_get_int("MAX_RECENT_ARTICLES_MEMORY", 5000),
            event_retention_days=_get_int("EVENT_RETENTION_DAYS", 14),
            raw_event_retention_days=_get_int("RAW_EVENT_RETENTION_DAYS", 7),
            retention_interval_seconds=_get_int("RETENTION_INTERVAL_SECONDS", 3600),
            reconnect_min_seconds=_get_int("RECONNECT_MIN_SECONDS", 5),
            reconnect_max_seconds=_get_int("RECONNECT_MAX_SECONDS", 120),
            connection_limit_backoff_seconds=_get_int("CONNECTION_LIMIT_BACKOFF_SECONDS", 90),
            queue_maxsize=_get_int("QUEUE_MAXSIZE", 10_000),
            slow_client_warning_queue_depth=_get_int("SLOW_CLIENT_WARNING_QUEUE_DEPTH", 7500),
            queue_backpressure_seconds=_get_float("QUEUE_BACKPRESSURE_SECONDS", 2.0),
            enable_manual_rest_backfill=_get_bool("ENABLE_MANUAL_REST_BACKFILL", True),
            high_latency_alert_ms=_get_int("HIGH_LATENCY_ALERT_MS", 30_000),
        )

    def safe_repr(self) -> dict[str, object]:
        """Return a dict suitable for logging — secrets redacted."""
        d = self.__dict__.copy()
        d.pop("alpaca_api_key", None)
        d.pop("alpaca_secret_key", None)
        d.pop("extra_env_seen", None)
        return d

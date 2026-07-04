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
    val = raw.strip().lower()
    if val in ("1", "true", "yes", "on", "y", "t"):
        return True
    if val in ("0", "false", "no", "off", "n", "f"):
        return False
    # A typo like ENABLE_STOCK_STREAM=ture must not silently mean False.
    raise RuntimeError(f"Invalid boolean for {key}: {raw!r}")


def _get_csv(key: str, default: str = "") -> list[str]:
    raw = os.getenv(key, default)
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


VALID_SUBSCRIPTION_MODES: Final = ("wildcard", "fallback")
VALID_STOCK_CODECS: Final = ("msgpack", "json")
# Feed names Alpaca accepts both as the v2 stream path segment and as the
# REST `feed` query param ("test" is the credential-free smoke-test stream).
VALID_STOCK_FEEDS: Final = ("sip", "iex", "delayed_sip", "test")
# Canonical Alpaca v2 stock-stream channel names (subscription message keys).
VALID_STOCK_CHANNELS: Final = (
    "trades",
    "quotes",
    "bars",
    "updatedBars",
    "dailyBars",
    "statuses",
    "lulds",
)


def _get_channels(key: str, default: str) -> list[str]:
    """Case-insensitive channel list normalized to canonical channel names."""
    raw = os.getenv(key)
    if raw is None:
        raw = default
    elif not raw.strip():
        # Explicitly-set empty previously fell back to ALL channels — the
        # exact opposite of what the operator asked for.
        raise RuntimeError(
            f"{key} is set but empty; unset it for the default "
            f"({default}) or list channels explicitly"
        )
    canonical = {c.lower(): c for c in VALID_STOCK_CHANNELS}
    out: list[str] = []
    for token in raw.split(","):
        token = token.strip().lower()
        if not token:
            continue
        if token not in canonical:
            raise RuntimeError(
                f"Invalid channel {token!r} in {key}; valid: {', '.join(VALID_STOCK_CHANNELS)}"
            )
        if canonical[token] not in out:
            out.append(canonical[token])
    return out


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

    event_retention_days: int
    raw_event_retention_days: int
    retention_interval_seconds: int

    reconnect_min_seconds: int
    reconnect_max_seconds: int
    connection_limit_backoff_seconds: int
    stable_connection_seconds: int
    news_idle_reconnect_seconds: int
    auth_retry_seconds: int
    auth_alert_every_n: int

    queue_maxsize: int
    slow_client_warning_queue_depth: int
    queue_backpressure_seconds: float

    enable_manual_rest_backfill: bool

    # Stock market-data stream
    enable_stock_stream: bool
    alpaca_stock_feed: str
    alpaca_stock_stream_url: str
    stock_stream_codec: str
    stock_watchlist_symbols: list[str]
    stock_channels: list[str]
    stock_idle_reconnect_seconds: int
    stock_queue_maxsize: int
    stock_batch_max: int
    stock_quote_sample_ms: int
    market_storage_path: str
    stock_tick_retention_minutes: int
    stock_bar_retention_days: int
    status_retention_days: int
    market_retention_interval_seconds: int
    gap_fill_max_lookback_minutes: int

    # REST market context
    alpaca_trading_base_url: str
    market_cache_clock_ttl: int
    market_cache_calendar_ttl: int
    market_cache_screener_ttl: int
    market_cache_snapshot_ttl: int
    market_cache_latest_ttl: int
    market_cache_corporate_actions_ttl: int

    # Alerts
    halt_alert_dedup_seconds: int
    alert_keywords_file: str
    alert_rate_limit_per_symbol_hour: int

    high_latency_alert_ms: int = 30_000

    extra_env_seen: dict[str, str] = field(default_factory=dict, repr=False)

    @classmethod
    def from_env(cls) -> Config:
        sub_mode = _get_str("NEWS_SUBSCRIPTION_MODE", "wildcard").lower()
        if sub_mode not in VALID_SUBSCRIPTION_MODES:
            raise RuntimeError(
                f"Invalid NEWS_SUBSCRIPTION_MODE={sub_mode!r}; must be one of {VALID_SUBSCRIPTION_MODES}"
            )
        stock_codec = _get_str("STOCK_STREAM_CODEC", "msgpack").lower()
        if stock_codec not in VALID_STOCK_CODECS:
            raise RuntimeError(
                f"Invalid STOCK_STREAM_CODEC={stock_codec!r}; must be one of {VALID_STOCK_CODECS}"
            )
        stock_feed = _get_str("ALPACA_STOCK_FEED", "sip").lower()
        if stock_feed not in VALID_STOCK_FEEDS:
            # The feed is both the stream URL path segment and the REST
            # `feed` param — a typo would poison snapshots and gap-fill too.
            raise RuntimeError(
                f"Invalid ALPACA_STOCK_FEED={stock_feed!r}; must be one of {VALID_STOCK_FEEDS}"
            )
        # A zero/negative floor would allow a zero-backoff reconnect loop.
        reconnect_min = max(1, _get_int("RECONNECT_MIN_SECONDS", 5))
        reconnect_max = max(reconnect_min, _get_int("RECONNECT_MAX_SECONDS", 120))
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
            event_retention_days=_get_int("EVENT_RETENTION_DAYS", 14),
            raw_event_retention_days=_get_int("RAW_EVENT_RETENTION_DAYS", 7),
            retention_interval_seconds=_get_int("RETENTION_INTERVAL_SECONDS", 3600),
            reconnect_min_seconds=reconnect_min,
            reconnect_max_seconds=reconnect_max,
            connection_limit_backoff_seconds=_get_int("CONNECTION_LIMIT_BACKOFF_SECONDS", 90),
            stable_connection_seconds=_get_int("STABLE_CONNECTION_SECONDS", 60),
            news_idle_reconnect_seconds=_get_int("NEWS_IDLE_RECONNECT_SECONDS", 0),
            auth_retry_seconds=_get_int("AUTH_RETRY_SECONDS", 900),
            auth_alert_every_n=_get_int("AUTH_ALERT_EVERY_N", 4),
            queue_maxsize=_get_int("QUEUE_MAXSIZE", 10_000),
            slow_client_warning_queue_depth=_get_int("SLOW_CLIENT_WARNING_QUEUE_DEPTH", 7500),
            queue_backpressure_seconds=_get_float("QUEUE_BACKPRESSURE_SECONDS", 2.0),
            enable_manual_rest_backfill=_get_bool("ENABLE_MANUAL_REST_BACKFILL", True),
            enable_stock_stream=_get_bool("ENABLE_STOCK_STREAM", True),
            alpaca_stock_feed=stock_feed,
            alpaca_stock_stream_url=_get_str("ALPACA_STOCK_STREAM_URL", ""),
            stock_stream_codec=stock_codec,
            stock_watchlist_symbols=_get_csv(
                "STOCK_WATCHLIST_SYMBOLS", "AAPL,MSFT,NVDA,TSLA,SPY,QQQ"
            ),
            stock_channels=_get_channels(
                "STOCK_CHANNELS",
                "trades,quotes,bars,updatedBars,dailyBars,statuses,lulds",
            ),
            stock_idle_reconnect_seconds=_get_int("STOCK_IDLE_RECONNECT_SECONDS", 120),
            stock_queue_maxsize=_get_int("STOCK_QUEUE_MAXSIZE", 50_000),
            stock_batch_max=_get_int("STOCK_BATCH_MAX", 2000),
            stock_quote_sample_ms=_get_int("STOCK_QUOTE_SAMPLE_MS", 0),
            market_storage_path=_get_str("MARKET_STORAGE_PATH", "/data/alpaca_market.sqlite"),
            stock_tick_retention_minutes=_get_int("STOCK_TICK_RETENTION_MINUTES", 240),
            stock_bar_retention_days=_get_int("STOCK_BAR_RETENTION_DAYS", 30),
            status_retention_days=_get_int("STATUS_RETENTION_DAYS", 30),
            market_retention_interval_seconds=_get_int(
                "MARKET_RETENTION_INTERVAL_SECONDS", 900
            ),
            gap_fill_max_lookback_minutes=_get_int(
                "GAP_FILL_MAX_LOOKBACK_MINUTES", 1440
            ),
            alpaca_trading_base_url=_get_str(
                "ALPACA_TRADING_BASE_URL", "https://paper-api.alpaca.markets"
            ),
            market_cache_clock_ttl=_get_int("MARKET_CACHE_CLOCK_TTL", 15),
            market_cache_calendar_ttl=_get_int("MARKET_CACHE_CALENDAR_TTL", 21_600),
            market_cache_screener_ttl=_get_int("MARKET_CACHE_SCREENER_TTL", 30),
            market_cache_snapshot_ttl=_get_int("MARKET_CACHE_SNAPSHOT_TTL", 5),
            market_cache_latest_ttl=_get_int("MARKET_CACHE_LATEST_TTL", 2),
            market_cache_corporate_actions_ttl=_get_int(
                "MARKET_CACHE_CORPORATE_ACTIONS_TTL", 600
            ),
            halt_alert_dedup_seconds=_get_int("HALT_ALERT_DEDUP_SECONDS", 300),
            alert_keywords_file=_get_str("ALERT_KEYWORDS_FILE", ""),
            alert_rate_limit_per_symbol_hour=_get_int(
                "ALERT_RATE_LIMIT_PER_SYMBOL_HOUR", 10
            ),
            high_latency_alert_ms=_get_int("HIGH_LATENCY_ALERT_MS", 30_000),
        )

    def safe_repr(self) -> dict[str, object]:
        """Return a dict suitable for logging — secrets redacted."""
        d = self.__dict__.copy()
        d.pop("alpaca_api_key", None)
        d.pop("alpaca_secret_key", None)
        d.pop("extra_env_seen", None)
        return d

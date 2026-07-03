"""Shared mutable state: per-stream health, subscription, interest symbols.

Reads of article/alert data always come from the SQLite Store — this class
deliberately holds no article caches, only counters and health snapshots.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import RLock

from .models import Alert, NewsArticle, StreamHealth, SubscriptionState

NEWS_STREAM = "news"
STOCK_STREAM = "stocks"


@dataclass
class State:
    interest_symbols: set[str] = field(default_factory=set)
    stream_health: dict[str, StreamHealth] = field(default_factory=dict)
    subscription_state: SubscriptionState = field(default_factory=SubscriptionState)

    last_seen_updated_at: str | None = None
    article_count: int = 0
    alert_count: int = 0

    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        self.stream_health = {
            NEWS_STREAM: StreamHealth(service="alpaca-news-mcp"),
            STOCK_STREAM: StreamHealth(service="alpaca-stock-stream"),
        }

    def record_article(self, article: NewsArticle, *, was_new: bool) -> None:
        with self._lock:
            if was_new:
                self.article_count += 1
            if article.updated_at:
                if self.last_seen_updated_at is None or article.updated_at > self.last_seen_updated_at:
                    self.last_seen_updated_at = article.updated_at
            elif article.created_at:
                if self.last_seen_updated_at is None or article.created_at > self.last_seen_updated_at:
                    self.last_seen_updated_at = article.created_at

    def record_alert(self, alert: Alert) -> None:
        with self._lock:
            self.alert_count += 1

    def record_article_drop(self, stream: str = NEWS_STREAM) -> int:
        """Atomically increment and return the dropped-article counter."""
        with self._lock:
            health = self.stream_health[stream]
            data = health.model_dump()
            data["articles_dropped"] = int(data.get("articles_dropped") or 0) + 1
            self.stream_health[stream] = StreamHealth.model_validate(data)
            return self.stream_health[stream].articles_dropped

    def set_interest_symbols(self, symbols: list[str], mode: str) -> set[str]:
        normalized = {s.strip().upper() for s in symbols if s and s.strip()}
        with self._lock:
            if mode == "replace":
                self.interest_symbols = set(normalized)
            elif mode == "add":
                self.interest_symbols |= normalized
            elif mode == "remove":
                self.interest_symbols -= normalized
            elif mode == "clear":
                self.interest_symbols.clear()
            else:
                raise ValueError(f"invalid mode: {mode!r}")
            return set(self.interest_symbols)

    def get_interest_symbols(self) -> set[str]:
        with self._lock:
            return set(self.interest_symbols)

    def update_health(self, stream: str = NEWS_STREAM, **kwargs: object) -> None:
        with self._lock:
            data = self.stream_health[stream].model_dump()
            # Pass None through so callers can clear nullable fields (e.g.
            # `last_error=None` on recovery). Omit keys to leave them unchanged.
            data.update(kwargs)
            self.stream_health[stream] = StreamHealth.model_validate(data)

    def snapshot_health(self, stream: str = NEWS_STREAM) -> StreamHealth:
        with self._lock:
            data = self.stream_health[stream].model_dump()
            if stream == NEWS_STREAM:
                data["article_count"] = self.article_count
                data["alert_count"] = self.alert_count
            return StreamHealth.model_validate(data)

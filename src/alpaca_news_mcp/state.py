"""In-memory caches and shared mutable state."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from threading import RLock

from .models import Alert, NewsArticle, StreamHealth, SubscriptionState


@dataclass
class State:
    max_recent_articles: int
    max_recent_alerts: int = 1000
    latency_window_size: int = 5000

    latest_articles_by_id: dict[int, NewsArticle] = field(default_factory=dict)
    recent_articles: deque[int] = field(default_factory=deque)
    articles_by_symbol: dict[str, deque[int]] = field(default_factory=dict)
    recent_alerts: deque[Alert] = field(default_factory=deque)
    latency_window: deque[int] = field(default_factory=deque)
    interest_symbols: set[str] = field(default_factory=set)
    stream_health: StreamHealth = field(default_factory=StreamHealth)
    subscription_state: SubscriptionState = field(default_factory=SubscriptionState)

    last_seen_updated_at: str | None = None
    article_count: int = 0
    alert_count: int = 0

    _lock: RLock = field(default_factory=RLock)

    def __post_init__(self) -> None:
        self.recent_articles = deque(maxlen=self.max_recent_articles)
        self.recent_alerts = deque(maxlen=self.max_recent_alerts)
        self.latency_window = deque(maxlen=self.latency_window_size)

    def record_article(self, article: NewsArticle, *, was_new: bool) -> None:
        with self._lock:
            self.latest_articles_by_id[article.id] = article
            if was_new:
                self.recent_articles.append(article.id)
                self.article_count += 1
            for sym in article.symbols:
                bucket = self.articles_by_symbol.setdefault(sym, deque(maxlen=500))
                if not bucket or bucket[-1] != article.id:
                    bucket.append(article.id)
            if article.latency_ms is not None and article.latency_ms >= 0:
                self.latency_window.append(article.latency_ms)
            if article.updated_at:
                if self.last_seen_updated_at is None or article.updated_at > self.last_seen_updated_at:
                    self.last_seen_updated_at = article.updated_at
            elif article.created_at:
                if self.last_seen_updated_at is None or article.created_at > self.last_seen_updated_at:
                    self.last_seen_updated_at = article.created_at

    def record_alert(self, alert: Alert) -> None:
        with self._lock:
            self.recent_alerts.append(alert)
            self.alert_count += 1

    def record_article_drop(self) -> int:
        """Atomically increment and return the dropped-article counter."""
        with self._lock:
            data = self.stream_health.model_dump()
            data["articles_dropped"] = int(data.get("articles_dropped") or 0) + 1
            self.stream_health = StreamHealth.model_validate(data)
            return self.stream_health.articles_dropped

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

    def update_health(self, **kwargs: object) -> None:
        with self._lock:
            data = self.stream_health.model_dump()
            # Pass None through so callers can clear nullable fields (e.g.
            # `last_error=None` on recovery). Omit keys to leave them unchanged.
            data.update(kwargs)
            self.stream_health = StreamHealth.model_validate(data)

    def snapshot_health(self) -> StreamHealth:
        with self._lock:
            data = self.stream_health.model_dump()
            data["article_count"] = self.article_count
            data["alert_count"] = self.alert_count
            return StreamHealth.model_validate(data)

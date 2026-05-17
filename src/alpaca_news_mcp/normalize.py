"""News message normalization: HTML stripping, symbol cleanup, latency, content hashing."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import orjson
from bs4 import BeautifulSoup
from dateutil import parser as dateparser


class NormalizationError(ValueError):
    pass


@dataclass
class NormalizedArticle:
    id: int
    headline: str
    summary: str | None
    author: str | None
    created_at: str | None
    updated_at: str | None
    content_html: str | None
    content_text: str | None
    url: str | None
    source: str | None
    symbols: list[str]
    raw_json: str
    received_at: str
    latency_ms: int | None
    content_hash: str
    is_content_present: bool
    extra_fields: dict[str, Any] = field(default_factory=dict)


_KNOWN_FIELDS = {
    "T",
    "id",
    "headline",
    "summary",
    "author",
    "created_at",
    "updated_at",
    "content",
    "url",
    "symbols",
    "source",
}


def _strip_html(html: str | None) -> str | None:
    if html is None:
        return None
    if not html:
        return ""
    try:
        soup = BeautifulSoup(html, "html.parser")
        text = soup.get_text(separator=" ", strip=True)
        return " ".join(text.split())
    except Exception:
        return html


def _normalize_symbols(raw: Any) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, str):
        raw = [raw]
    out: set[str] = set()
    for s in raw:
        if not isinstance(s, str):
            continue
        cleaned = s.strip().upper()
        if cleaned:
            out.add(cleaned)
    return sorted(out)


def _to_iso8601_utc(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, str):
        try:
            dt = dateparser.isoparse(value)
        except (ValueError, TypeError):
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        else:
            dt = dt.astimezone(UTC)
        return dt.isoformat()
    return None


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _compute_latency_ms(created_at: str | None, received_at_iso: str) -> int | None:
    if not created_at:
        return None
    try:
        created = dateparser.isoparse(created_at)
        received = dateparser.isoparse(received_at_iso)
        if created.tzinfo is None:
            created = created.replace(tzinfo=UTC)
        if received.tzinfo is None:
            received = received.replace(tzinfo=UTC)
        delta = (received - created).total_seconds() * 1000.0
        return int(delta)
    except (ValueError, TypeError):
        return None


def _content_hash(headline: str, summary: str | None, content_html: str | None) -> str:
    h = hashlib.sha256()
    h.update((headline or "").encode("utf-8"))
    h.update(b"\x00")
    h.update((summary or "").encode("utf-8"))
    h.update(b"\x00")
    h.update((content_html or "").encode("utf-8"))
    return h.hexdigest()


def normalize_news_message(payload: dict[str, Any]) -> NormalizedArticle:
    """Normalize an Alpaca news message (T='n' or REST shape) into NormalizedArticle.

    Raises NormalizationError when the payload cannot be interpreted as a news article.
    """
    if not isinstance(payload, dict):
        raise NormalizationError("payload is not a dict")

    msg_type = payload.get("T")
    if msg_type is not None and msg_type != "n":
        raise NormalizationError(f"not a news message: T={msg_type!r}")

    raw_id = payload.get("id")
    if raw_id is None:
        raise NormalizationError("missing id")
    try:
        article_id = int(raw_id)
    except (TypeError, ValueError) as e:
        raise NormalizationError(f"invalid id: {raw_id!r}") from e

    headline = payload.get("headline") or ""
    if not isinstance(headline, str):
        headline = str(headline)

    summary = payload.get("summary")
    if summary is not None and not isinstance(summary, str):
        summary = str(summary)

    author = payload.get("author")
    if author is not None and not isinstance(author, str):
        author = str(author)

    content_html = payload.get("content")
    if content_html is not None and not isinstance(content_html, str):
        content_html = str(content_html)

    url = payload.get("url")
    if url is not None and not isinstance(url, str):
        url = str(url)

    source = payload.get("source")
    if source is not None and not isinstance(source, str):
        source = str(source)

    created_at = _to_iso8601_utc(payload.get("created_at"))
    updated_at = _to_iso8601_utc(payload.get("updated_at"))
    symbols = _normalize_symbols(payload.get("symbols"))

    received_at = _now_iso()
    latency_ms = _compute_latency_ms(created_at, received_at)
    content_text = _strip_html(content_html)
    is_content_present = bool(content_html and content_html.strip())
    raw_json = orjson.dumps(payload).decode("utf-8")
    digest = _content_hash(headline, summary, content_html)
    extras = {k: v for k, v in payload.items() if k not in _KNOWN_FIELDS}

    return NormalizedArticle(
        id=article_id,
        headline=headline,
        summary=summary,
        author=author,
        created_at=created_at,
        updated_at=updated_at,
        content_html=content_html,
        content_text=content_text,
        url=url,
        source=source,
        symbols=symbols,
        raw_json=raw_json,
        received_at=received_at,
        latency_ms=latency_ms,
        content_hash=digest,
        is_content_present=is_content_present,
        extra_fields=extras,
    )


def truncate(text: str | None, max_chars: int) -> str | None:
    if text is None:
        return None
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 3].rstrip() + "..."

from __future__ import annotations

import pytest

from alpaca_news_mcp.normalize import (
    NormalizationError,
    normalize_news_message,
    truncate,
)


def test_normalizes_websocket_article(fixture_loader):
    payload = fixture_loader("news_websocket_article.json")[0]
    article = normalize_news_message(payload)
    assert article.id == 24736073
    assert article.headline.startswith("Apple")
    assert article.symbols == ["AAPL"]
    assert article.source == "benzinga"
    assert article.content_html and "<p>" in article.content_html
    # HTML stripped to text
    assert article.content_text and "AAPL" in article.content_text
    assert "<" not in article.content_text and ">" not in article.content_text
    assert article.is_content_present is True
    assert article.created_at and "T" in article.created_at
    assert article.updated_at and "T" in article.updated_at
    assert article.content_hash and len(article.content_hash) == 64


def test_normalizes_rest_payload(fixture_loader):
    rest = fixture_loader("news_rest_page.json")
    msft = rest["news"][0]
    article = normalize_news_message(msft)
    assert article.id == 24736074
    assert article.symbols == ["MSFT"]
    assert "MSFT" in (article.content_text or "")


def test_symbol_normalization():
    payload = {
        "T": "n",
        "id": 1,
        "headline": "h",
        "symbols": ["aapl", " MSFT ", "aapl", ""],
        "created_at": "2026-04-28T00:00:00Z",
        "updated_at": "2026-04-28T00:00:00Z",
    }
    a = normalize_news_message(payload)
    assert a.symbols == ["AAPL", "MSFT"]


def test_html_strip_decodes_entities():
    payload = {
        "T": "n",
        "id": 2,
        "headline": "h",
        "content": "<p>Tom &amp; Jerry &lt;news&gt;</p>",
    }
    a = normalize_news_message(payload)
    assert a.content_text == "Tom & Jerry <news>"


def test_latency_ms_computed():
    payload = {
        "T": "n",
        "id": 3,
        "headline": "h",
        "created_at": "2020-01-01T00:00:00Z",
        "updated_at": "2020-01-01T00:00:00Z",
    }
    a = normalize_news_message(payload)
    assert a.latency_ms is not None
    assert a.latency_ms > 0


def test_unknown_fields_preserved_in_raw_json():
    payload = {
        "T": "n",
        "id": 4,
        "headline": "h",
        "future_field": {"x": 1},
        "another_one": "yes",
    }
    a = normalize_news_message(payload)
    # raw_json preserves the entire original payload
    import orjson

    raw = orjson.loads(a.raw_json)
    assert raw["future_field"] == {"x": 1}
    assert raw["another_one"] == "yes"


def test_invalid_message_type_rejected():
    with pytest.raises(NormalizationError):
        normalize_news_message({"T": "trade", "id": 1, "headline": "h"})


def test_missing_id_rejected():
    with pytest.raises(NormalizationError):
        normalize_news_message({"T": "n", "headline": "h"})


def test_unparseable_timestamps_become_none():
    payload = {
        "T": "n",
        "id": 5,
        "headline": "h",
        "created_at": "not-a-real-timestamp",
        "updated_at": "also bogus",
    }
    a = normalize_news_message(payload)
    assert a.created_at is None
    assert a.updated_at is None


def test_truncate_helper():
    assert truncate("hello", 10) == "hello"
    out = truncate("hello world", 5)
    assert out is not None
    assert out.endswith("...")
    assert len(out) <= 5
    assert truncate(None, 5) is None

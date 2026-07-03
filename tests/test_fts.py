"""FTS5 search: ranking, stemming, sync on update/prune, LIKE fallback."""

from __future__ import annotations

import pytest

from alpaca_news_mcp.normalize import normalize_news_message


def _payload(id: int, headline: str, *, summary: str | None = None,
             content: str | None = None, updated_at: str = "2026-04-28T20:30:00Z"):
    p = {
        "T": "n",
        "id": id,
        "headline": headline,
        "created_at": "2026-04-28T20:29:50Z",
        "updated_at": updated_at,
        "symbols": ["AAPL"],
        "source": "benzinga",
    }
    if summary is not None:
        p["summary"] = summary
    if content is not None:
        p["content"] = content
    return p


async def _ingest(store, payload):
    return await store.upsert_article(
        normalize_news_message(payload), source_kind="ws"
    )


@pytest.mark.asyncio
async def test_fts_porter_stemming_matches_inflections(open_store):
    await _ingest(open_store, _payload(1, "MegaCorp acquires TinyStartup"))
    await _ingest(open_store, _payload(2, "Nothing relevant here"))
    out = await open_store.search_articles(query="acquire", limit=10)
    assert {a.id for a in out} == {1}
    # And the other direction: stored "acquisition" ~ query "acquisitions"
    await _ingest(open_store, _payload(3, "Acquisition of assets announced"))
    out = await open_store.search_articles(query="acquisitions", limit=10)
    assert 3 in {a.id for a in out}


@pytest.mark.asyncio
async def test_fts_multi_token_is_anded(open_store):
    await _ingest(open_store, _payload(1, "Apple beats earnings estimates"))
    await _ingest(open_store, _payload(2, "Apple ships new product"))
    out = await open_store.search_articles(query="apple earnings", limit=10)
    assert {a.id for a in out} == {1}


@pytest.mark.asyncio
async def test_fts_index_follows_headline_update(open_store):
    await _ingest(open_store, _payload(1, "original headline text"))
    await _ingest(
        open_store,
        _payload(1, "totally rewritten banner", updated_at="2026-04-28T21:00:00Z"),
    )
    assert not await open_store.search_articles(query="original", limit=10)
    out = await open_store.search_articles(query="rewritten", limit=10)
    assert {a.id for a in out} == {1}


@pytest.mark.asyncio
async def test_fts_searches_summary_and_content(open_store):
    await _ingest(
        open_store,
        _payload(1, "plain headline", summary="hidden gem in summary",
                 content="<p>buried treasure in body</p>"),
    )
    assert {a.id for a in await open_store.search_articles(query="gem", limit=10)} == {1}
    assert {a.id for a in await open_store.search_articles(query="treasure", limit=10)} == {1}


@pytest.mark.asyncio
async def test_fts_syntax_characters_are_neutralized(open_store):
    """LLM-supplied queries with FTS5 syntax must not raise — every token is
    quoted before matching."""
    await _ingest(open_store, _payload(1, "results AND (guidance) NEAR misc"))
    # Unbalanced quote + operators + parens: every token is quoted, so this is
    # a plain AND of [guidance, AND, results] — no syntax error, and it matches.
    out = await open_store.search_articles(query='"guidance AND results', limit=10)
    assert 1 in {a.id for a in out}
    # Operator-only garbage must not raise either (falls through to LIKE).
    out2 = await open_store.search_articles(query='" ( ) NEAR NOT', limit=10)
    assert isinstance(out2, list)


@pytest.mark.asyncio
async def test_substring_fallback_when_fts_finds_nothing(open_store):
    """FTS is token-based, so a mid-word fragment finds nothing; the LIKE
    fallback preserves the old substring behavior."""
    await _ingest(open_store, _payload(1, "MegaCorp acquires TinyStartup"))
    out = await open_store.search_articles(query="cquir", limit=10)
    assert {a.id for a in out} == {1}


@pytest.mark.asyncio
async def test_prune_removes_fts_rows(open_store):
    await _ingest(
        open_store,
        _payload(1, "ancient searchable story", updated_at="2020-01-01T00:00:00Z"),
    )
    # Force first_seen_at into the past so retention catches it.
    await open_store.conn.execute(
        "UPDATE news_articles SET first_seen_at = '2020-01-01T00:00:00+00:00' WHERE id = 1"
    )
    await open_store.conn.commit()
    await open_store.prune_retention(event_days=14, raw_event_days=7)
    assert not await open_store.search_articles(query="ancient", limit=10)
    # FTS integrity: no orphaned rows that would break later queries.
    cur = await open_store.conn.execute(
        "SELECT COUNT(*) FROM news_fts WHERE news_fts MATCH '\"ancient\"'"
    )
    row = await cur.fetchone()
    await cur.close()
    assert row[0] == 0


@pytest.mark.asyncio
async def test_prune_survives_recent_alert_on_expired_article(open_store):
    """A recent alert referencing an expired article must not trip the
    alerts.article_id foreign key mid-prune — that used to abort AFTER the
    FTS purge with no rollback, leaving FTS missing rows for live articles."""
    from alpaca_news_mcp.models import Alert

    await _ingest(
        open_store,
        _payload(1, "ancient story with fresh alert",
                 updated_at="2020-01-01T00:00:00Z"),
    )
    await open_store.conn.execute(
        "UPDATE news_articles SET first_seen_at = '2020-01-01T00:00:00+00:00' WHERE id = 1"
    )
    await open_store.conn.commit()
    # Alert created NOW (newer than the cutoff) referencing the old article.
    assert await open_store.record_alert(
        Alert(
            alert_id="fresh-on-old",
            article_id=1,
            created_at="2099-01-01T00:00:00+00:00",
            severity="high",
            category="mna_keyword",
            symbols=["AAPL"],
            headline="h",
            reason="r",
        ),
        raw_json="{}",
    )
    counts = await open_store.prune_retention(event_days=14, raw_event_days=7)
    assert counts["articles"] == 1
    assert counts["alerts"] >= 1  # the referencing alert went with it
    assert not await open_store.search_articles(query="ancient", limit=10)
    cur = await open_store.conn.execute("SELECT COUNT(*) FROM alerts")
    row = await cur.fetchone()
    await cur.close()
    assert row[0] == 0

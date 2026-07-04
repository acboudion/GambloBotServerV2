from __future__ import annotations

import pytest

from alpaca_news_mcp.normalize import normalize_news_message


def _payload(id: int = 100, *, headline: str = "test", updated_at: str = "2026-04-28T20:30:00Z"):
    return {
        "T": "n",
        "id": id,
        "headline": headline,
        "summary": "summary",
        "author": "author",
        "created_at": "2026-04-28T20:29:50Z",
        "updated_at": updated_at,
        "content": "<p>body</p>",
        "url": "https://x.example/y",
        "symbols": ["AAPL"],
        "source": "benzinga",
    }


@pytest.mark.asyncio
async def test_upsert_inserts_new_article(open_store):
    a = normalize_news_message(_payload(101))
    res = await open_store.upsert_article(a, source_kind="ws")
    assert res.was_new is True
    assert res.version_inserted is True

    fetched = await open_store.get_article(101)
    assert fetched is not None
    assert fetched.id == 101
    assert fetched.symbols == ["AAPL"]


@pytest.mark.asyncio
async def test_upsert_idempotent_when_unchanged(open_store):
    a = normalize_news_message(_payload(102))
    await open_store.upsert_article(a, source_kind="ws")
    res = await open_store.upsert_article(a, source_kind="rest")
    assert res.was_new is False
    assert res.version_inserted is False


@pytest.mark.asyncio
async def test_upsert_creates_version_on_change(open_store):
    a = normalize_news_message(_payload(103, headline="v1"))
    await open_store.upsert_article(a, source_kind="ws")
    a2 = normalize_news_message(
        _payload(103, headline="v2", updated_at="2026-04-28T20:31:00Z")
    )
    res = await open_store.upsert_article(a2, source_kind="ws")
    assert res.was_new is False
    assert res.version_inserted is True

    versions = await open_store.get_versions(103)
    assert len(versions) == 2
    assert {v.headline for v in versions} == {"v1", "v2"}


@pytest.mark.asyncio
async def test_symbol_index_populated(open_store):
    a = normalize_news_message({**_payload(104), "symbols": ["AAPL", "MSFT"]})
    await open_store.upsert_article(a, source_kind="ws")
    bucketed = await open_store.articles_for_symbols(
        ["AAPL", "MSFT"], minutes=60, limit_per_symbol=10
    )
    assert {a.id for a in bucketed["AAPL"]} == {104}
    assert {a.id for a in bucketed["MSFT"]} == {104}


@pytest.mark.asyncio
async def test_get_recent_with_filters(open_store):
    a1 = normalize_news_message({**_payload(105), "symbols": ["AAPL"], "source": "benzinga"})
    a2 = normalize_news_message({**_payload(106), "symbols": ["TSLA"], "source": "other"})
    await open_store.upsert_article(a1, source_kind="ws")
    await open_store.upsert_article(a2, source_kind="ws")
    recent = await open_store.get_recent_articles(minutes=60, sources=["benzinga"], limit=10)
    assert {a.id for a in recent} == {105}
    recent2 = await open_store.get_recent_articles(minutes=60, symbols=["TSLA"], limit=10)
    assert {a.id for a in recent2} == {106}


@pytest.mark.asyncio
async def test_record_alert_dedup_by_article_category(open_store):
    from alpaca_news_mcp.models import Alert

    article = normalize_news_message(_payload(200))
    await open_store.upsert_article(article, source_kind="ws")

    alert = Alert(
        alert_id="aaa",
        article_id=200,
        created_at="2026-04-28T20:00:00Z",
        severity="high",
        category="mna_keyword",
        symbols=["AAPL"],
        headline="h",
        reason="r",
    )
    inserted = await open_store.record_alert(alert, raw_json="{}")
    assert inserted is True

    alert2 = Alert(
        alert_id="bbb",
        article_id=200,
        created_at="2026-04-28T20:00:00Z",
        severity="high",
        category="mna_keyword",
        symbols=["AAPL"],
        headline="h",
        reason="r",
    )
    inserted2 = await open_store.record_alert(alert2, raw_json="{}")
    assert inserted2 is False  # (article_id, category) unique


@pytest.mark.asyncio
async def test_symbol_index_refreshes_when_only_symbols_change(open_store):
    a1 = normalize_news_message({**_payload(108), "symbols": ["AAPL", "MSFT"]})
    await open_store.upsert_article(a1, source_kind="ws")

    # Same headline/summary/content/updated_at but different symbols
    a2 = normalize_news_message({**_payload(108), "symbols": ["MSFT", "NVDA"]})
    await open_store.upsert_article(a2, source_kind="ws")

    bucketed = await open_store.articles_for_symbols(
        ["AAPL", "MSFT", "NVDA"], minutes=60, limit_per_symbol=10
    )
    # AAPL was removed, NVDA was added, MSFT stays.
    assert {a.id for a in bucketed["AAPL"]} == set()
    assert {a.id for a in bucketed["MSFT"]} == {108}
    assert {a.id for a in bucketed["NVDA"]} == {108}

    sym_map = await open_store.symbol_map(minutes=60, min_articles=1)
    assert "AAPL" not in sym_map
    assert sym_map.get("MSFT") == 1
    assert sym_map.get("NVDA") == 1


@pytest.mark.asyncio
async def test_reingest_without_optional_fields_is_idempotent(open_store):
    """A re-ingest payload missing optional fields (summary/content) should not
    re-mark the article as changed or create a redundant version row, since the
    UPDATE COALESCEs those fields and persisted content is unchanged."""
    a1 = normalize_news_message(_payload(120))
    res1 = await open_store.upsert_article(a1, source_kind="ws")
    assert res1.was_new is True
    assert res1.article.summary == "summary"
    assert res1.article.content_html == "<p>body</p>"

    # Re-ingest with summary and content stripped out (None after normalize).
    partial = {**_payload(120)}
    partial.pop("summary")
    partial.pop("content")
    a2 = normalize_news_message(partial)
    assert a2.summary is None
    assert a2.content_html is None

    res2 = await open_store.upsert_article(a2, source_kind="rest")
    assert res2.was_new is False
    assert res2.version_inserted is False
    assert res2.article.update_count == 0
    # Existing values should be preserved by COALESCE.
    assert res2.article.summary == "summary"
    assert res2.article.content_html == "<p>body</p>"

    versions = await open_store.get_versions(120)
    assert len(versions) == 1


@pytest.mark.asyncio
async def test_latency_ms_preserved_across_upserts(open_store):
    """First-insert latency_ms is pinned; later upserts (e.g. REST refresh
    of an already-stored WS article) must not overwrite it. Otherwise the
    REST-derived `now - created_at` (article age) replaces the genuine WS
    ingest latency and pollutes get_news_latency_stats."""
    a1 = normalize_news_message(_payload(400))
    a1.latency_ms = 1234
    await open_store.upsert_article(a1, source_kind="ws")
    fetched = await open_store.get_article(400)
    assert fetched is not None
    assert fetched.latency_ms == 1234

    a2 = normalize_news_message(_payload(400))
    a2.latency_ms = 9_999_999
    await open_store.upsert_article(a2, source_kind="rest")
    fetched2 = await open_store.get_article(400)
    assert fetched2 is not None
    assert fetched2.latency_ms == 1234


@pytest.mark.asyncio
async def test_latency_stats_excludes_negative_samples(open_store):
    """Clock skew can produce negative latency_ms values. They live in the
    column for forensic value but must NOT contribute to the percentile
    sample, otherwise avg_ms is dragged toward zero or below."""
    pos = [200, 400, 1000]
    neg = [-100, -5000]
    aid = 500
    for v in pos + neg:
        a = normalize_news_message(_payload(aid))
        a.latency_ms = v
        await open_store.upsert_article(a, source_kind="ws")
        aid += 1

    stats = await open_store.latency_stats(60)
    assert stats.sample_count == len(pos)
    assert stats.avg_ms == int(sum(pos) / len(pos))
    assert stats.max_ms == max(pos)


@pytest.mark.asyncio
async def test_latency_ms_backfills_when_initially_null(open_store):
    """If the first insert had no latency_ms (e.g. unparseable created_at),
    a later upsert with a real value should still be able to fill it in."""
    a1 = normalize_news_message(_payload(401))
    a1.latency_ms = None
    await open_store.upsert_article(a1, source_kind="ws")
    fetched = await open_store.get_article(401)
    assert fetched is not None
    assert fetched.latency_ms is None

    a2 = normalize_news_message(_payload(401))
    a2.latency_ms = 5000
    await open_store.upsert_article(a2, source_kind="rest")
    fetched2 = await open_store.get_article(401)
    assert fetched2 is not None
    assert fetched2.latency_ms == 5000


@pytest.mark.asyncio
async def test_search_articles(open_store):
    a = normalize_news_message(
        _payload(107, headline="Major acquisition announced for unique target")
    )
    await open_store.upsert_article(a, source_kind="ws")
    out = await open_store.search_articles(query="acquisition", limit=10)
    assert {x.id for x in out} == {107}


@pytest.mark.asyncio
async def test_search_date_bounds_use_single_effective_timestamp(open_store):
    """Both since/until must compare against one effective timestamp. An
    article whose created_at is before `since` but updated_at is after `until`
    used to be admitted because the OR-combined predicate let updated_at
    satisfy `since` and created_at satisfy `until` independently."""
    out_of_window = {**_payload(300, headline="straddles the window")}
    out_of_window["created_at"] = "2026-01-01T00:00:00Z"
    out_of_window["updated_at"] = "2026-12-01T00:00:00Z"
    in_window = {**_payload(301, headline="inside the window")}
    in_window["created_at"] = "2026-06-15T00:00:00Z"
    in_window["updated_at"] = "2026-06-15T00:00:00Z"

    await open_store.upsert_article(
        normalize_news_message(out_of_window), source_kind="ws"
    )
    await open_store.upsert_article(
        normalize_news_message(in_window), source_kind="ws"
    )

    out = await open_store.search_articles(
        query="window",
        since="2026-06-01T00:00:00Z",
        until="2026-09-01T00:00:00Z",
        limit=10,
    )
    assert {x.id for x in out} == {301}


@pytest.mark.asyncio
async def test_raw_event_recording(open_store):
    await open_store.record_raw_event(
        endpoint="news_ws", message_type="error_406", raw_json='{"foo":1}'
    )
    events = await open_store.recent_raw_events(minutes=60, limit=10)
    assert len(events) == 1
    assert events[0]["message_type"] == "error_406"
    assert events[0]["raw"] == {"foo": 1}


@pytest.mark.asyncio
async def test_upsert_result_article_matches_db_row(open_store):
    """upsert_article builds its returned article in Python (no re-SELECT);
    it must be byte-identical to what a subsequent get_article reads back —
    on insert, on a partial update (COALESCE keeps old values), and on an
    unchanged re-delivery."""
    # Insert
    r1 = await open_store.upsert_article(
        normalize_news_message(_payload(900)), source_kind="ws"
    )
    db1 = await open_store.get_article(900)
    assert r1.article.model_dump(exclude={"last_seen_at", "first_seen_at"}) == \
        db1.model_dump(exclude={"last_seen_at", "first_seen_at"})
    assert r1.article.first_seen_at == db1.first_seen_at

    # Partial update: summary omitted → existing kept; headline changes.
    partial = {"T": "n", "id": 900, "headline": "changed headline",
               "updated_at": "2026-04-28T21:00:00Z"}
    r2 = await open_store.upsert_article(
        normalize_news_message(partial), source_kind="rest"
    )
    db2 = await open_store.get_article(900)
    assert r2.article.model_dump(exclude={"last_seen_at", "raw"}) == \
        db2.model_dump(exclude={"last_seen_at", "raw"})
    assert r2.article.summary == "summary"  # kept via COALESCE
    assert r2.article.headline == "changed headline"
    assert r2.article.update_count == db2.update_count == 1
    assert r2.article.seq == db2.seq

    # Unchanged re-delivery: seq stays put.
    r3 = await open_store.upsert_article(
        normalize_news_message(partial), source_kind="rest"
    )
    db3 = await open_store.get_article(900)
    assert r3.article.seq == db3.seq == r2.article.seq
    assert r3.article.update_count == 1


@pytest.mark.asyncio
async def test_batch_writer_commits_once_and_persists(open_store):
    async with open_store.batch_writer() as w:
        for i in range(5):
            await w.upsert_article(
                normalize_news_message(_payload(1000 + i)), source_kind="ws"
            )
    arts = await open_store.get_recent_articles(minutes=60, limit=50)
    assert {a.id for a in arts} >= {1000, 1001, 1002, 1003, 1004}


@pytest.mark.asyncio
async def test_batch_writer_rolls_back_on_error(open_store):
    with pytest.raises(RuntimeError):
        async with open_store.batch_writer() as w:
            await w.upsert_article(
                normalize_news_message(_payload(2000)), source_kind="ws"
            )
            raise RuntimeError("boom")
    assert await open_store.get_article(2000) is None
    # The store stays usable after the rollback.
    await open_store.upsert_article(
        normalize_news_message(_payload(2001)), source_kind="ws"
    )
    assert await open_store.get_article(2001) is not None


@pytest.mark.asyncio
async def test_alert_cross_source_dedup_by_content_hash(open_store):
    """The same story under a different article id (same content hash) must
    not produce a second alert in the same category within 24h."""
    from alpaca_news_mcp.models import Alert

    await open_store.upsert_article(
        normalize_news_message(_payload(1, headline="MegaCorp merger")),
        source_kind="ws",
    )
    await open_store.upsert_article(
        normalize_news_message(_payload(2, headline="MegaCorp merger repost")),
        source_kind="ws",
    )

    def _alert(alert_id, article_id):
        from datetime import UTC, datetime
        return Alert(
            alert_id=alert_id, article_id=article_id,
            created_at=datetime.now(UTC).isoformat(),
            severity="high", category="mna_keyword", symbols=["AAPL"],
            reason="keyword match",
        )

    assert await open_store.record_alert(
        _alert("a1", 1), raw_json="{}", content_hash="samehash"
    )
    # Different article, same content hash + category → deduped.
    assert not await open_store.record_alert(
        _alert("a2", 2), raw_json="{}", content_hash="samehash"
    )
    # Different category still records.
    from alpaca_news_mcp.models import Alert as A
    other = _alert("a3", 2).model_copy(update={"category": "breaking_keyword"})
    assert isinstance(other, A)
    assert await open_store.record_alert(other, raw_json="{}", content_hash="samehash")
    # Direction round-trips through get_alerts.
    alerts = await open_store.get_alerts(minutes=5, limit=10)
    assert all(a.direction == "neutral" for a in alerts)


@pytest.mark.asyncio
async def test_batch_writer_rolls_back_when_commit_fails(open_store, monkeypatch):
    """A commit failure (SQLITE_BUSY / I/O) must roll back — an open
    transaction would let a later unrelated write commit this failed batch."""
    import sqlite3

    async def failing_commit():
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(open_store.conn, "commit", failing_commit)
    with pytest.raises(sqlite3.OperationalError):
        async with open_store.batch_writer() as w:
            await w.upsert_article(
                normalize_news_message(_payload(3000)), source_kind="ws"
            )
    monkeypatch.undo()
    # A later successful write must not resurrect the failed batch's rows.
    await open_store.upsert_article(
        normalize_news_message(_payload(3001)), source_kind="ws"
    )
    assert await open_store.get_article(3000) is None
    assert await open_store.get_article(3001) is not None


@pytest.mark.asyncio
async def test_reads_use_isolated_connection(open_store):
    # File-backed store gets a dedicated read-only connection.
    assert open_store._read_conn is not None
    assert open_store.rconn is open_store._read_conn


@pytest.mark.asyncio
async def test_reader_does_not_see_uncommitted_batch(open_store):
    """Tool reads must see only committed data: a batch transaction in
    flight (or later rolled back) must never leak phantom articles."""
    committed = normalize_news_message(_payload(900, headline="committed"))
    await open_store.upsert_article(committed, source_kind="ws")

    class Boom(RuntimeError):
        pass

    with pytest.raises(Boom):
        async with open_store.batch_writer() as writer:
            uncommitted = normalize_news_message(_payload(901, headline="phantom"))
            await writer.upsert_article(uncommitted, source_kind="ws")
            # Mid-transaction: the writer connection has the row, but tool
            # reads (rconn) must not observe it.
            articles, _, _ = await open_store.articles_since(cursor=0, limit=10)
            assert [a.id for a in articles] == [900]
            assert await open_store.get_article(901) is None
            raise Boom()

    # Rolled back: still invisible.
    articles, _, _ = await open_store.articles_since(cursor=0, limit=10)
    assert [a.id for a in articles] == [900]

    # A committed batch becomes visible to the read connection.
    async with open_store.batch_writer() as writer:
        replacement = normalize_news_message(_payload(902, headline="real"))
        await writer.upsert_article(replacement, source_kind="ws")
    articles, _, _ = await open_store.articles_since(cursor=0, limit=10)
    assert [a.id for a in articles] == [900, 902]


def _mk_alert(alert_id: str):
    from alpaca_news_mcp.models import Alert

    return Alert(
        alert_id=alert_id,
        article_id=None,
        created_at="2020-01-01T00:00:00+00:00",
        severity="high",
        category="mna_keyword",
        symbols=["AAPL"],
        headline="h",
        reason="r",
        acknowledged=False,
    )


@pytest.mark.asyncio
async def test_seq_watermarks_survive_prune_and_restart(tmp_path):
    """Retention can delete the rows carrying MAX(seq); after a restart the
    allocators must not re-issue consumed seq values or the bot's saved
    cursors would silently hide all new rows."""
    from alpaca_news_mcp.store import Store

    path = str(tmp_path / "wm.sqlite")
    store = await Store.open(path)
    await store.init_schema()
    # Article + alert with old timestamps so retention prunes them.
    payload = _payload(500)
    payload["created_at"] = "2020-01-01T00:00:00Z"
    payload["updated_at"] = "2020-01-01T00:00:01Z"
    await store.upsert_article(normalize_news_message(payload), source_kind="ws")
    await store.record_alert(_mk_alert("old-alert"), raw_json="{}")
    articles, news_cursor, _ = await store.articles_since(cursor=0, limit=10)
    alerts, alert_cursor, _ = await store.alerts_since(cursor=0, limit=10)
    assert news_cursor >= 1 and alert_cursor >= 1

    # Backdate first_seen/created so the prune removes everything.
    async with store._write_lock:
        await store.conn.execute(
            "UPDATE news_articles SET first_seen_at = '2020-01-02T00:00:00+00:00'"
        )
        await store.conn.commit()
    pruned = await store.prune_retention(event_days=1, raw_event_days=1)
    assert pruned["articles"] == 1 and pruned["alerts"] >= 1
    await store.close()

    # Restart on the same file: new rows must allocate ABOVE the old cursors.
    store = await Store.open(path)
    await store.init_schema()
    try:
        await store.upsert_article(
            normalize_news_message(_payload(501)), source_kind="ws"
        )
        await store.record_alert(_mk_alert("new-alert"), raw_json="{}")
        articles, _, _ = await store.articles_since(cursor=news_cursor, limit=10)
        assert [a.id for a in articles] == [501]
        alerts, _, _ = await store.alerts_since(cursor=alert_cursor, limit=10)
        assert [a["alert_id"] for a in alerts] == ["new-alert"]
    finally:
        await store.close()

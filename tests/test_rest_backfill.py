from __future__ import annotations

import httpx
import pytest
import respx

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.rest_backfill import RestBackfillWorker
from alpaca_news_mcp.state import State
from alpaca_news_mcp.store import Store


@pytest.fixture
async def wired(tmp_path, monkeypatch):
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "x.sqlite"))
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.example")
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "true")
    monkeypatch.setenv("BACKFILL_LOOKBACK_MINUTES", "5")
    cfg = Config.from_env()
    store = await Store.open(cfg.storage_path)
    await store.init_schema()
    state = State()
    alerts = AlertEngine()
    worker = RestBackfillWorker(cfg, store, state, alerts)
    yield worker, store, state, cfg
    await worker.close()
    await store.close()


@pytest.mark.asyncio
async def test_backfill_paginates(wired):
    worker, store, _, _ = wired
    page1 = {
        "news": [
            {
                "id": 1,
                "headline": "A",
                "summary": "s",
                "created_at": "2026-04-28T19:00:00Z",
                "updated_at": "2026-04-28T19:00:00Z",
                "content": "<p>x</p>",
                "symbols": ["AAPL"],
                "source": "benzinga",
            }
        ],
        "next_page_token": "PT1",
    }
    page2 = {
        "news": [
            {
                "id": 2,
                "headline": "B",
                "summary": "s",
                "created_at": "2026-04-28T19:01:00Z",
                "updated_at": "2026-04-28T19:01:00Z",
                "content": "<p>y</p>",
                "symbols": ["MSFT"],
                "source": "benzinga",
            }
        ],
        "next_page_token": None,
    }

    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(
            side_effect=[
                httpx.Response(200, json=page1),
                httpx.Response(200, json=page2),
            ]
        )
        result = await worker.backfill_startup()

    assert result["pages"] >= 2
    assert result["ingested"] == 2
    assert result["new"] == 2
    assert result["updated"] == 0
    assert result["duplicate"] == 0
    assert result["failed"] == 0
    a1 = await store.get_article(1)
    a2 = await store.get_article(2)
    assert a1 is not None and a2 is not None


@pytest.mark.asyncio
async def test_429_with_retry_after_then_success(wired, monkeypatch):
    worker, _store, _, _ = wired
    success_page = {
        "news": [
            {
                "id": 10,
                "headline": "ok",
                "created_at": "2026-04-28T19:00:00Z",
                "updated_at": "2026-04-28T19:00:00Z",
            }
        ],
        "next_page_token": None,
    }

    sleep_calls: list[float] = []

    async def fake_sleep(t):
        sleep_calls.append(t)

    monkeypatch.setattr("alpaca_news_mcp.rest_backfill.asyncio.sleep", fake_sleep)

    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(
            side_effect=[
                httpx.Response(429, headers={"Retry-After": "0.1"}),
                httpx.Response(200, json=success_page),
            ]
        )
        result = await worker.backfill_startup()

    assert result["ingested"] == 1
    assert result["new"] == 1
    assert sleep_calls and sleep_calls[0] == pytest.approx(0.1)


@pytest.mark.asyncio
async def test_403_marks_entitlement_error(wired):
    worker, _, state, _ = wired
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(return_value=httpx.Response(403, json={"error": "forbidden"}))
        result = await worker.backfill_startup()

    assert result["ingested"] == 0
    assert state.snapshot_health().entitlement_error is True
    assert "rest 403 forbidden" in (state.snapshot_health().last_error or "")


@pytest.mark.asyncio
async def test_429_retry_budget_aborts_to_avoid_infinite_loop(wired, monkeypatch):
    """If Alpaca keeps returning 429, _run() must give up after a bounded number
    of retries instead of blocking startup forever."""
    worker, _store, state, _ = wired

    async def fake_sleep(_t):
        return None

    monkeypatch.setattr("alpaca_news_mcp.rest_backfill.asyncio.sleep", fake_sleep)

    with respx.mock(base_url="https://data.alpaca.example") as mock:
        route = mock.get("/v1beta1/news").mock(
            return_value=httpx.Response(429, headers={"Retry-After": "0"})
        )
        result = await worker.backfill_startup()

    assert result["ingested"] == 0
    # Bounded retry budget: must not be unbounded.
    assert route.call_count <= 10
    assert route.call_count >= 2
    assert state.snapshot_health().last_error is not None
    assert "429" in (state.snapshot_health().last_error or "")


@pytest.mark.asyncio
async def test_rest_backfill_does_not_emit_high_latency_alerts(wired):
    """REST backfill ingests historical articles whose `latency_ms` (now -
    created_at) routinely exceeds the high-latency threshold. Those should
    NOT be alerted on — they're not real-time pipeline delay."""
    worker, store, _state, _ = wired
    stale_page = {
        "news": [
            {
                "id": 5001,
                "headline": "stale historical article",
                # Created hours ago — latency_ms will be huge.
                "created_at": "2026-04-01T00:00:00Z",
                "updated_at": "2026-04-01T00:00:00Z",
                "symbols": ["AAPL"],
                "source": "benzinga",
            }
        ],
        "next_page_token": None,
    }
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(
            return_value=httpx.Response(200, json=stale_page)
        )
        await worker.backfill_startup()

    alerts_db = await store.get_alerts(minutes=60 * 24 * 60, severity=None)
    assert all(a.category != "high_latency" for a in alerts_db), (
        "REST backfill must not emit high_latency alerts"
    )


@pytest.mark.asyncio
async def test_backfill_distinguishes_new_updated_duplicate(wired):
    """A second call over an overlapping window must report `duplicate` for
    rows it has already stored — operators previously could not tell from
    the response whether new content was actually ingested."""
    worker, _store, _, _ = wired
    page = {
        "news": [
            {
                "id": 700,
                "headline": "v1",
                "created_at": "2026-04-28T19:00:00Z",
                "updated_at": "2026-04-28T19:00:00Z",
                "content": "<p>v1</p>",
                "symbols": ["AAPL"],
                "source": "benzinga",
            }
        ],
        "next_page_token": None,
    }
    page_v2 = {
        "news": [
            {
                "id": 700,
                "headline": "v2",
                "created_at": "2026-04-28T19:00:00Z",
                "updated_at": "2026-04-28T19:05:00Z",
                "content": "<p>v2</p>",
                "symbols": ["AAPL"],
                "source": "benzinga",
            }
        ],
        "next_page_token": None,
    }

    with respx.mock(base_url="https://data.alpaca.example") as mock:
        route = mock.get("/v1beta1/news")
        route.mock(return_value=httpx.Response(200, json=page))
        first = await worker.manual(60)
        route.mock(return_value=httpx.Response(200, json=page_v2))
        second = await worker.manual(60)
        route.mock(return_value=httpx.Response(200, json=page_v2))
        third = await worker.manual(60)

    assert first == {"ingested": 1, "new": 1, "updated": 0, "duplicate": 0, "failed": 0, "pages": 1, "truncated": False, "failure": None}
    assert second["ingested"] == 1 and second["updated"] == 1 and second["new"] == 0 and second["duplicate"] == 0
    assert third["ingested"] == 1 and third["duplicate"] == 1 and third["new"] == 0 and third["updated"] == 0


@pytest.mark.asyncio
async def test_rest_then_ws_versioning(wired):
    """REST seeds an article; WS update with newer updated_at writes a version row."""
    worker, store, _, _ = wired
    rest_page = {
        "news": [
            {
                "id": 99,
                "headline": "v1",
                "created_at": "2026-04-28T19:00:00Z",
                "updated_at": "2026-04-28T19:00:00Z",
                "content": "<p>v1</p>",
                "symbols": ["AAPL"],
            }
        ],
        "next_page_token": None,
    }
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(return_value=httpx.Response(200, json=rest_page))
        await worker.backfill_startup()

    # Apply WS update via store directly using normalize
    from alpaca_news_mcp.normalize import normalize_news_message

    ws_payload = {
        "T": "n",
        "id": 99,
        "headline": "v2",
        "created_at": "2026-04-28T19:00:00Z",
        "updated_at": "2026-04-28T19:05:00Z",
        "content": "<p>v2</p>",
        "symbols": ["AAPL"],
    }
    res = await store.upsert_article(normalize_news_message(ws_payload), source_kind="ws")
    assert res.was_new is False
    assert res.version_inserted is True
    versions = await store.get_versions(99)
    assert {v.headline for v in versions} == {"v1", "v2"}


@pytest.mark.asyncio
async def test_gap_fill_raises_on_failure_so_retry_path_engages(wired):
    """Reconnect gap-fill must NOT swallow REST failures: partial counts on
    403/429/HTTP errors would make BaseStreamWorker treat the fill as done and
    silently drop the disconnect window."""
    from alpaca_news_mcp.rest_backfill import BackfillError

    worker, _store, state, _ = wired
    state.last_seen_updated_at = "2026-04-28T19:00:00+00:00"
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(return_value=httpx.Response(403, json={"error": "forbidden"}))
        with pytest.raises(BackfillError, match="403"):
            await worker.gap_fill()

    # Startup keeps its best-effort contract (app_setup catches and continues).
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(return_value=httpx.Response(403, json={"error": "forbidden"}))
        result = await worker.backfill_startup()
    assert result["ingested"] == 0


@pytest.mark.asyncio
async def test_gap_fill_raises_when_another_backfill_holds_the_lock(wired):
    from alpaca_news_mcp.rest_backfill import BackfillError

    worker, _store, state, _ = wired
    state.last_seen_updated_at = "2026-04-28T19:00:00+00:00"
    await worker._run_lock.acquire()
    try:
        with pytest.raises(BackfillError, match="already running"):
            await worker.gap_fill()
    finally:
        worker._run_lock.release()


@pytest.mark.asyncio
async def test_gap_fill_raises_when_page_cap_truncates(wired, monkeypatch):
    """A busy window that still has next_page_token at the page cap is an
    incomplete fill — must raise so the retry/alert path engages."""
    import alpaca_news_mcp.rest_backfill as rb

    monkeypatch.setattr(rb, "MAX_BACKFILL_PAGES", 1)
    worker, _store, state, _ = wired
    state.last_seen_updated_at = "2026-04-28T19:00:00+00:00"
    page = {
        "news": [{"id": 900, "headline": "h", "created_at": "2026-04-28T19:01:00Z",
                  "updated_at": "2026-04-28T19:01:00Z"}],
        "next_page_token": "more",
    }
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(return_value=httpx.Response(200, json=page))
        with pytest.raises(rb.BackfillError, match="page cap"):
            await worker.gap_fill()


@pytest.mark.asyncio
async def test_gap_fill_raises_when_items_fail_to_ingest(wired):
    """Item-level ingest failures (bad payloads) leave the window incomplete —
    reconnect gap-fills must raise, not report success over missing articles."""
    from alpaca_news_mcp.rest_backfill import BackfillError

    worker, _store, state, _ = wired
    state.last_seen_updated_at = "2026-04-28T19:00:00+00:00"
    page = {
        "news": [
            {"id": "not-an-int", "headline": "unparseable id"},
            {"id": 901, "headline": "fine", "created_at": "2026-04-28T19:01:00Z",
             "updated_at": "2026-04-28T19:01:00Z"},
        ],
        "next_page_token": None,
    }
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(return_value=httpx.Response(200, json=page))
        with pytest.raises(BackfillError, match="failed to ingest"):
            await worker.gap_fill()
    # The good article still landed (retry is idempotent on the rest).
    assert (await _store.get_article(901)) is not None


@pytest.mark.asyncio
async def test_manual_backfill_surfaces_failure(wired):
    """Non-raising runs must still report incompleteness — the manual MCP tool
    turns this into status=incomplete instead of a false ok."""
    worker, _store, _state, _ = wired
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(return_value=httpx.Response(403, json={"error": "forbidden"}))
        result = await worker.manual(30)
    assert result["failure"] is not None
    assert "403" in result["failure"]

    success_page = {"news": [], "next_page_token": None}
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(return_value=httpx.Response(200, json=success_page))
        result = await worker.manual(30)
    assert result["failure"] is None


@pytest.mark.asyncio
async def test_manual_skipped_by_concurrent_run_reports_failure(wired):
    """A manual run skipped because another backfill holds the lock fetched
    nothing for the requested window — it must carry a failure so the MCP
    tool reports status=incomplete instead of a false ok."""
    worker, _store, _state, _ = wired
    await worker._run_lock.acquire()
    try:
        result = await worker.manual(30)
    finally:
        worker._run_lock.release()
    assert result["skipped"] == 1
    assert result["ingested"] == 0
    assert result["failure"] is not None
    assert "already running" in result["failure"]


@pytest.mark.asyncio
async def test_non_json_body_marks_run_incomplete(wired):
    """A 200 with a non-JSON body must mark the run incomplete, not raise."""
    worker, _store, _state, _ = wired
    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(
            return_value=httpx.Response(200, text="<html>oops</html>")
        )
        result = await worker.manual(30)
    assert result["failure"] == "non-JSON response body"


@pytest.mark.asyncio
async def test_history_fetch_scopes_symbols_and_caps_items(wired):
    """fetch_news_history: symbol-scoped params, item budget stops
    pagination as an intentional truncation (not a failure)."""
    worker, store, _, _ = wired
    seen_params = []

    def responder(request):
        seen_params.append(dict(request.url.params))
        news = [
            {"id": 9000 + i, "headline": f"old story {i}", "symbols": ["XBIO"],
             "created_at": "2026-01-01T00:00:00Z", "updated_at": "2026-01-01T00:00:00Z"}
            for i in range(3)
        ]
        return httpx.Response(200, json={"news": news, "next_page_token": "more"})

    with respx.mock(base_url="https://data.alpaca.example") as mock:
        mock.get("/v1beta1/news").mock(side_effect=responder)
        result = await worker.history(symbols=["xbio"], days=90, max_articles=2)

    assert seen_params[0]["symbols"] == "XBIO"
    assert result["truncated"] is True
    assert result["failure"] is None
    assert result["new"] + result["updated"] + result["duplicate"] == 2
    assert (await store.get_article(9000)) is not None

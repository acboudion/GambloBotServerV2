from __future__ import annotations

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.models import NewsArticle


def _make_article(
    *,
    id: int = 1,
    headline: str = "h",
    summary: str | None = None,
    content_text: str | None = None,
    symbols: list[str] | None = None,
    latency_ms: int | None = None,
):
    return NewsArticle(
        id=id,
        headline=headline,
        summary=summary,
        author="a",
        created_at="2026-04-28T00:00:00Z",
        updated_at="2026-04-28T00:00:01Z",
        content_html=None,
        content_text=content_text,
        url=None,
        source="benzinga",
        symbols=symbols or [],
        first_seen_at="2026-04-28T00:00:01Z",
        last_seen_at="2026-04-28T00:00:01Z",
        last_seen_source="ws",
        update_count=0,
        latency_ms=latency_ms,
        is_content_present=True,
    )


def test_keyword_categorization_mna():
    eng = AlertEngine()
    a = _make_article(headline="Acme acquires Widget Co in $5B deal")
    out = eng.evaluate_article(a, interest_symbols=set())
    cats = {al.category for al in out}
    assert "mna_keyword" in cats


def test_severity_critical_for_bankruptcy():
    eng = AlertEngine()
    a = _make_article(headline="Company files for bankruptcy")
    out = eng.evaluate_article(a, interest_symbols=set())
    sev = {al.severity for al in out}
    assert "critical" in sev


def test_severity_critical_for_halted():
    eng = AlertEngine()
    a = _make_article(headline="Trading halted pending news")
    out = eng.evaluate_article(a, interest_symbols=set())
    cats = {al.category for al in out}
    sev = {al.severity for al in out}
    assert "halt_risk_keyword" in cats
    assert "critical" in sev


def test_interest_symbol_alert():
    eng = AlertEngine()
    a = _make_article(symbols=["AAPL"], headline="Some random news for AAPL")
    out = eng.evaluate_article(a, interest_symbols={"AAPL"})
    cats = {al.category for al in out}
    assert "held_or_interested_symbol" in cats


def test_interest_symbol_elevates_analyst_severity():
    eng = AlertEngine()
    a = _make_article(symbols=["AAPL"], headline="AAPL gets analyst upgrade with new price target")
    out = eng.evaluate_article(a, interest_symbols={"AAPL"})
    analyst_alerts = [al for al in out if al.category == "analyst_keyword"]
    assert analyst_alerts and analyst_alerts[0].severity in ("high", "critical")


def test_breaking_keyword_alert():
    eng = AlertEngine()
    a = _make_article(headline="BREAKING: market wide rally")
    out = eng.evaluate_article(a, interest_symbols=set())
    cats = {al.category for al in out}
    assert "breaking_keyword" in cats


def test_high_latency_alert():
    eng = AlertEngine(high_latency_alert_ms=5000)
    a = _make_article(latency_ms=8000)
    out = eng.evaluate_article(a, interest_symbols=set())
    cats = {al.category for al in out}
    assert "high_latency" in cats


def test_high_latency_alert_suppressed_for_backfill():
    """REST backfill computes latency_ms against `created_at`, so historical
    items would always trip the threshold. `suppress_high_latency=True` lets
    callers opt out without losing other alert categories."""
    eng = AlertEngine(high_latency_alert_ms=5000)
    a = _make_article(
        headline="Acme acquires Widget Co",  # also generates an mna alert
        latency_ms=86_400_000,  # 24h old — would normally trip
    )
    out = eng.evaluate_article(
        a, interest_symbols=set(), suppress_high_latency=True
    )
    cats = {al.category for al in out}
    assert "high_latency" not in cats
    # Other categories (e.g. keyword-based) should still fire.
    assert "mna_keyword" in cats


def test_no_false_positive_for_secured():
    """sec keyword shouldn't match 'secured' due to word boundaries."""
    eng = AlertEngine()
    a = _make_article(headline="Apple secured deal with supplier")
    out = eng.evaluate_article(a, interest_symbols=set())
    # 'deal' will trigger mna_keyword; that's fine. We only want to verify no
    # legal_regulatory match for 'sec' from 'secured'.
    legal = [al for al in out if al.category == "legal_regulatory_keyword"]
    assert legal == []


def test_stream_error_alert_classification():
    eng = AlertEngine()
    a = eng.stream_error_alert(code=406, message="connection limit exceeded")
    assert a.category == "stream_error"
    assert a.severity == "critical"

    b = eng.stream_error_alert(code=409, message="insufficient subscription", entitlement=True)
    assert b.category == "entitlement_error"
    assert b.severity == "critical"


# ---- alert engine upgrades (keywords file, direction, rate limit, dedup) -----

import json as _json  # noqa: E402

from alpaca_news_mcp.alerts import AlertEngine as _Engine  # noqa: E402
from alpaca_news_mcp.models import NewsArticle as _Article  # noqa: E402


def _mk_article(id=1, headline="h", symbols=("AAPL",), summary=None):
    return _Article(
        id=id,
        headline=headline,
        summary=summary,
        first_seen_at="2026-07-02T10:00:00+00:00",
        last_seen_at="2026-07-02T10:00:00+00:00",
        last_seen_source="ws",
        symbols=list(symbols),
    )


def test_corporate_action_group_fires_dead_category_no_more():
    engine = _Engine()
    alerts = engine.evaluate_article(
        _mk_article(headline="Board approves 10-for-1 stock split"),
        interest_symbols=set(),
    )
    cats = {a.category for a in alerts}
    assert "corporate_action_keyword" in cats


def test_direction_tagging_bullish_vs_bearish():
    engine = _Engine()
    beats = engine.evaluate_article(
        _mk_article(headline="MegaCorp beats earnings, raises outlook"),
        interest_symbols=set(),
    )
    earnings = [a for a in beats if a.category == "earnings_keyword"]
    assert earnings[0].direction == "bullish"

    misses = engine.evaluate_article(
        _mk_article(id=2, headline="MegaCorp misses earnings, cuts outlook"),
        interest_symbols=set(),
    )
    earnings = [a for a in misses if a.category == "earnings_keyword"]
    assert earnings[0].direction == "bearish"


def test_keywords_file_merge(tmp_path):
    kw = tmp_path / "kw.json"
    kw.write_text(_json.dumps({
        "mna": {"phrases": ["scheme of arrangement"]},
        "custom_topic": {"phrases": ["quantum widget"], "bullish": ["quantum widget"]},
    }))
    engine = _Engine(keywords_file=str(kw))
    alerts = engine.evaluate_article(
        _mk_article(headline="Target agrees to scheme of arrangement"),
        interest_symbols=set(),
    )
    assert any(a.category == "mna_keyword" for a in alerts)
    # Unknown group maps to breaking_keyword.
    alerts = engine.evaluate_article(
        _mk_article(id=3, headline="Quantum widget shipped"),
        interest_symbols=set(),
    )
    assert any(a.category == "breaking_keyword" for a in alerts)


def test_keywords_file_bad_json_falls_back(tmp_path):
    kw = tmp_path / "kw.json"
    kw.write_text("{not json")
    engine = _Engine(keywords_file=str(kw))
    alerts = engine.evaluate_article(
        _mk_article(headline="Merger announced"), interest_symbols=set()
    )
    assert any(a.category == "mna_keyword" for a in alerts)


def test_rate_limit_suppresses_but_critical_exempt():
    engine = _Engine(rate_limit_per_symbol_hour=2)
    for i in range(5):
        engine.evaluate_article(
            _mk_article(id=i, headline=f"Analyst upgrade #{i}"),
            interest_symbols=set(),
        )
    # Only 2 analyst alerts allowed per hour for the same symbol set.
    total = sum(
        1
        for i in range(5, 10)
        for a in engine.evaluate_article(
            _mk_article(id=i, headline=f"Analyst upgrade #{i}"),
            interest_symbols=set(),
        )
        if a.category == "analyst_keyword"
    )
    assert total == 0
    assert engine.suppressed_alerts >= 3

    # Critical alerts bypass the limiter entirely.
    crit = engine.evaluate_article(
        _mk_article(id=99, headline="Bankruptcy filing"), interest_symbols=set()
    )
    assert any(a.severity == "critical" for a in crit)

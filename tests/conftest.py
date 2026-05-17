"""Shared pytest fixtures."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from alpaca_news_mcp.alerts import AlertEngine
from alpaca_news_mcp.config import Config
from alpaca_news_mcp.state import State
from alpaca_news_mcp.store import Store

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"


@pytest.fixture
def fixture_loader():
    def _load(name: str):
        with open(FIXTURES / name, encoding="utf-8") as f:
            return json.load(f)

    return _load


@pytest.fixture
def env_for_config(monkeypatch, tmp_path):
    monkeypatch.setenv("ALPACA_API_KEY", "test_key_xxxxxxxx")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test_secret_xxxxxxxx")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "test.sqlite"))
    monkeypatch.setenv("ALPACA_DATA_BASE_URL", "https://data.alpaca.markets")
    monkeypatch.setenv("ENABLE_REST_BACKFILL", "false")
    monkeypatch.setenv("BACKFILL_LOOKBACK_MINUTES", "10")
    monkeypatch.setenv("CONNECTION_LIMIT_BACKOFF_SECONDS", "1")
    monkeypatch.setenv("RECONNECT_MIN_SECONDS", "1")
    monkeypatch.setenv("RECONNECT_MAX_SECONDS", "2")
    yield


@pytest.fixture
def config(env_for_config) -> Config:
    return Config.from_env()


@pytest.fixture
async def open_store(tmp_path):
    store = await Store.open(str(tmp_path / "store.sqlite"))
    await store.init_schema()
    yield store
    await store.close()


@pytest.fixture
def alert_engine() -> AlertEngine:
    return AlertEngine(high_latency_alert_ms=10_000)


@pytest.fixture
def state(config) -> State:
    s = State(max_recent_articles=config.max_recent_articles_memory)
    s.set_interest_symbols(["AAPL", "MSFT"], "replace")
    return s

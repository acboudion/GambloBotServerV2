"""MarketDataClient: TTL caching, 429 retry, endpoint parsing (respx-mocked)."""

from __future__ import annotations

import httpx
import pytest
import respx

from alpaca_news_mcp.config import Config
from alpaca_news_mcp.market_client import MarketDataClient


@pytest.fixture
def config(monkeypatch, tmp_path) -> Config:
    monkeypatch.setenv("ALPACA_API_KEY", "k")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "s")
    monkeypatch.setenv("STORAGE_PATH", str(tmp_path / "t.sqlite"))
    return Config.from_env()


@pytest.fixture
async def client(config):
    c = MarketDataClient(config)
    yield c
    await c.close()


@pytest.mark.asyncio
@respx.mock
async def test_clock_is_cached(client):
    route = respx.get("https://paper-api.alpaca.markets/v2/clock").mock(
        return_value=httpx.Response(
            200,
            json={"timestamp": "2026-07-02T10:00:00-04:00", "is_open": True,
                  "next_open": "x", "next_close": "y"},
        )
    )
    first = await client.clock()
    second = await client.clock()
    assert first == second
    assert first["is_open"] is True
    assert route.call_count == 1  # second call served from TTL cache
    assert await client.is_market_open() is True
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_clock_unavailable_returns_none(client):
    respx.get("https://paper-api.alpaca.markets/v2/clock").mock(
        return_value=httpx.Response(500, text="boom")
    )
    assert await client.clock() is None
    assert await client.is_market_open() is None


@pytest.mark.asyncio
@respx.mock
async def test_429_retries_once_with_retry_after(client):
    route = respx.get("https://data.alpaca.markets/v1beta1/screener/stocks/movers")
    route.side_effect = [
        httpx.Response(429, headers={"Retry-After": "0"}),
        httpx.Response(200, json={"gainers": [{"symbol": "UP", "percent_change": 12.0}],
                                  "losers": [], "market_type": "stocks"}),
    ]
    payload = await client.movers(top=5)
    assert payload["gainers"][0]["symbol"] == "UP"
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_most_actives_and_snapshot_paths(client):
    respx.get("https://data.alpaca.markets/v1beta1/screener/stocks/most-actives").mock(
        return_value=httpx.Response(
            200, json={"most_actives": [{"symbol": "TSLA", "volume": 1000}]}
        )
    )
    actives = await client.most_actives(by="volume", top=5)
    assert actives["most_actives"][0]["symbol"] == "TSLA"

    respx.get("https://data.alpaca.markets/v2/stocks/snapshots").mock(
        return_value=httpx.Response(
            200,
            json={"AAPL": {"latestTrade": {"p": 190.5},
                           "latestQuote": {"bp": 190.4, "ap": 190.6},
                           "minuteBar": {"c": 190.5}, "dailyBar": {"c": 190.0},
                           "prevDailyBar": {"c": 188.0}}},
        )
    )
    snaps = await client.snapshots(["aapl"])
    assert snaps["AAPL"]["latestTrade"]["p"] == 190.5


@pytest.mark.asyncio
@respx.mock
async def test_latest_and_corporate_actions(client):
    respx.get("https://data.alpaca.markets/v2/stocks/trades/latest").mock(
        return_value=httpx.Response(200, json={"trades": {"MSFT": {"p": 400.0}}})
    )
    latest = await client.latest("trades", ["MSFT"])
    assert latest["trades"]["MSFT"]["p"] == 400.0
    with pytest.raises(ValueError):
        await client.latest("bogus", ["MSFT"])

    respx.get("https://data.alpaca.markets/v1beta1/corporate-actions").mock(
        return_value=httpx.Response(
            200,
            json={"corporate_actions": {"forward_splits": [{"symbol": "NVDA", "new_rate": 10}]}},
        )
    )
    ca = await client.corporate_actions(symbols=["NVDA"], start="2026-06-01", end="2026-07-01")
    assert ca["corporate_actions"]["forward_splits"][0]["symbol"] == "NVDA"


@pytest.mark.asyncio
@respx.mock
async def test_bars_pagination(client):
    route = respx.get("https://data.alpaca.markets/v2/stocks/bars")
    route.side_effect = [
        httpx.Response(200, json={"bars": {"AAPL": [{"t": "2026-07-02T13:30:00Z", "c": 1.0}]},
                                  "next_page_token": "tok"}),
        httpx.Response(200, json={"bars": {"AAPL": [{"t": "2026-07-02T13:31:00Z", "c": 2.0}]},
                                  "next_page_token": None}),
    ]
    bars = await client.bars(["AAPL"], start_iso="2026-07-02T13:00:00Z")
    assert [b["c"] for b in bars["AAPL"]] == [1.0, 2.0]
    assert route.call_count == 2

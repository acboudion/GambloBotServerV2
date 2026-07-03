# alpaca-news-mcp

Docker-served MCP server that ingests Alpaca's real-time **news** WebSocket and
the **v2 stock market-data** WebSocket (trades, quotes, bars, trading halts,
LULD bands) and exposes everything to MCP clients (Claude Code, Codex) over
Streamable HTTP. It is the data layer for an LLM trading harness: bounded,
token-efficient, delta-pollable tools backed by local SQLite caches.

- News: `wss://stream.data.alpaca.markets/v1beta1/news` → `/data/alpaca_news.sqlite`
- Stocks: `wss://stream.data.alpaca.markets/v2/{feed}` (default `sip`, msgpack) → `/data/alpaca_market.sqlite`
- REST context: screener movers/most-actives, snapshots, market clock/calendar,
  corporate actions, historical bars (gap-fill)

## What this MCP does

- Maintains exactly **one** news WebSocket and **one** stock WebSocket per
  container (Alpaca's connection limit is per endpoint, so the two coexist).
- News: subscribes `news:["*"]`, persists + versions every article, strips HTML
  off the event loop, FTS5 full-text search, REST backfill on startup and
  reconnect gap-fill (with retries + alerts), replay of queue-dropped articles.
- Stocks: full-depth ingestion (trades/quotes/bars/updatedBars/dailyBars/
  statuses/lulds) for a runtime-adjustable watchlist (≤100 symbols); batched
  `executemany` writes; in-memory latest-per-symbol snapshot cache; minute-bar
  gap-fill via REST after reconnects; rolling tick retention (4h default).
- Deterministic alerts: keyword groups (configurable via `ALERT_KEYWORDS_FILE`),
  bullish/bearish direction tagging, cross-source dedup by content hash,
  per-symbol rate limiting, trading-halt/resume/LULD alerts, stream health
  alerts (stale feed, auth failure, gap-fill failure, coverage gaps).
- Delta-polling cursors (`get_news_since`, `get_alerts_since`, `get_bars_since`)
  so a polling harness fetches exactly what's new — no overlapping windows.
- Reliability: idle watchdog (market-clock-gated for stocks), reconnect backoff
  that resets after stable sessions, slow-cadence retry + critical alerts on
  auth failure, `/healthz` reporting both streams.

## What this MCP does NOT do

- It does **not** place trades, cancel orders, or call any order/position
  endpoint. The trading-API host is used ONLY for read-only `GET /v2/clock`
  and `GET /v2/calendar`.
- It does **not** open more than one Alpaca WebSocket per endpoint.
- It does **not** integrate FMP, FRED, SEC EDGAR, or any other data provider.
- It does **not** make trading decisions.

## ⚠️ Alpaca connection-limit warning

Alpaca limits most subscriptions to **one active WebSocket connection per user
per endpoint**. A second connection to the same endpoint returns
`406 connection limit exceeded`. The news and stock streams are different
endpoints — one of each is fine; two containers are not.

- **Run one container at a time.** `docker-compose.yml` enforces `replicas: 1`.
- On `406` the server backs off `CONNECTION_LIMIT_BACKOFF_SECONDS` (default 90s)
  and surfaces the state via the health tools and `/healthz`.

## Required environment variables

Copy `.env.example` to `.env` and edit. Core variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `ALPACA_API_KEY` / `ALPACA_SECRET_KEY` | — (required) | Alpaca credentials |
| `MCP_HOST` / `MCP_PORT` / `MCP_PATH` | `0.0.0.0` / `8000` / `/mcp` | HTTP binding |
| `STORAGE_PATH` | `/data/alpaca_news.sqlite` | News + alerts DB |
| `LOG_LEVEL` | `info` | Logging |

News stream:

| Variable | Default | Purpose |
| --- | --- | --- |
| `ALPACA_NEWS_STREAM_URL` | `wss://stream.data.alpaca.markets/v1beta1/news` | Override for sandbox |
| `NEWS_SUBSCRIPTION_MODE` | `wildcard` | `wildcard` or `fallback` |
| `NEWS_FALLBACK_SYMBOLS` | (empty) | Used if wildcard rejected (409) |
| `NEWS_INTEREST_SYMBOLS` | `AAPL,MSFT,NVDA,TSLA,SPY,QQQ` | Alert escalation filter |
| `NEWS_IDLE_RECONNECT_SECONDS` | `0` (off) | Idle watchdog (news is quiet overnight; ping/pong covers dead sockets) |
| `ENABLE_REST_BACKFILL` | `true` | Startup backfill + reconnect gap-fill |
| `BACKFILL_LOOKBACK_MINUTES` / `REST_BACKFILL_OVERLAP_SECONDS` | `60` / `180` | Backfill windows |
| `EVENT_RETENTION_DAYS` / `RAW_EVENT_RETENTION_DAYS` | `14` / `7` | News retention |
| `RETENTION_INTERVAL_SECONDS` | `3600` | News pruner cadence (also runs at startup) |

Stock stream:

| Variable | Default | Purpose |
| --- | --- | --- |
| `ENABLE_STOCK_STREAM` | `true` | Master switch |
| `ALPACA_STOCK_FEED` | `sip` | `sip` / `iex` / `delayed_sip` |
| `ALPACA_STOCK_STREAM_URL` | (derived) | Override; use `wss://stream.data.alpaca.markets/v2/test` for smoke tests |
| `STOCK_STREAM_CODEC` | `msgpack` | `msgpack` or `json` |
| `STOCK_WATCHLIST_SYMBOLS` | `AAPL,MSFT,NVDA,TSLA,SPY,QQQ` | Initial watchlist (runtime-adjustable, persisted) |
| `STOCK_CHANNELS` | all seven | Subset of `trades,quotes,bars,updatedBars,dailyBars,statuses,lulds` |
| `STOCK_IDLE_RECONNECT_SECONDS` | `120` | Idle watchdog (only fires while market open) |
| `STOCK_QUEUE_MAXSIZE` / `STOCK_BATCH_MAX` | `50000` / `2000` | Ingest queue / batch commit size |
| `STOCK_QUOTE_SAMPLE_MS` | `0` (store all) | First knob to turn if the market DB grows too fast |
| `MARKET_STORAGE_PATH` | `/data/alpaca_market.sqlite` | Tick/bar DB |
| `STOCK_TICK_RETENTION_MINUTES` / `STOCK_BAR_RETENTION_DAYS` | `240` / `30` | Market retention |
| `MARKET_RETENTION_INTERVAL_SECONDS` | `900` | Market pruner cadence |

Reliability & alerts:

| Variable | Default | Purpose |
| --- | --- | --- |
| `STABLE_CONNECTION_SECONDS` | `60` | Session length that resets reconnect backoff |
| `AUTH_RETRY_SECONDS` / `AUTH_ALERT_EVERY_N` | `900` / `4` | 402 retry cadence + re-alert period |
| `RECONNECT_MIN_SECONDS` / `RECONNECT_MAX_SECONDS` | `5` / `120` | Backoff bounds |
| `CONNECTION_LIMIT_BACKOFF_SECONDS` | `90` | Sleep after `406` |
| `ALERT_KEYWORDS_FILE` | (empty) | JSON merged over built-in keyword groups |
| `ALERT_RATE_LIMIT_PER_SYMBOL_HOUR` | `10` | Per-(symbols,category) cap; critical exempt |
| `HALT_ALERT_DEDUP_SECONDS` | `300` | Halt/LULD alert dedup window |

REST context: `ALPACA_DATA_BASE_URL`, `ALPACA_TRADING_BASE_URL` (paper by
default; clock/calendar only), and `MARKET_CACHE_*_TTL` cache tunables.

Secrets are never logged or exposed via tools/resources.

## Docker Compose setup

```bash
cp .env.example .env
# edit .env with your real Alpaca credentials

docker compose up --build -d
curl http://localhost:8000/healthz
```

The compose file mounts a named volume at `/data` so both SQLite files survive
restarts. Existing news databases are migrated in place at startup
(`PRAGMA user_version` migrations: seq cursor, FTS index, alert columns).

## Client setup

```bash
claude mcp add --transport http alpaca-news http://localhost:8000/mcp
# or
codex mcp add alpaca_news --url http://localhost:8000/mcp
```

## Tools

### Delta polling (recommended for a trading-loop harness)
- `get_news_since(cursor, limit, symbols, fields)` — articles newer than the
  cursor, oldest first; updated articles re-surface. `fields=compact` is ~6
  fields per article.
- `get_alerts_since(cursor, limit, severity, categories)`
- `get_bars_since(cursor, symbols, timeframe, limit)` — compact array rows.

Each returns `next_cursor` + `has_more`; pass `next_cursor` back verbatim.

### News
- `get_recent_news(minutes, symbols, sources, limit, include_content, include_raw, fields)`
- `get_news_article(article_id, ...)`, `get_news_versions(article_id, limit)`
- `search_news(query, symbols, since, until, limit, fields)` — FTS5 ranked
  (porter stemming), substring LIKE fallback
- `get_news_for_symbols(symbols, minutes, limit_per_symbol, fields)`
- `get_breaking_news_digest(minutes, symbols, max_articles)`
- `run_news_rest_backfill(minutes)`

### Market data (stream-backed)
- `get_latest_market_data(symbols, include)` — in-memory snapshot cache;
  REST fallback for non-watchlist symbols
- `get_trades_window(symbol, minutes, limit)` / `get_quotes_window(...)`
- `get_stock_bars(symbol, timeframe, start, end, limit)`
- `get_trading_halts(minutes, symbols)` — statuses + LULD
- `set_stock_watchlist(symbols, mode)` / `get_stock_watchlist()` — live
  subscribe/unsubscribe deltas, persisted across restarts

### Market context (REST, TTL-cached)
- `get_market_movers(top)` / `get_most_active_stocks(by, top)` — "what's in
  play today"
- `get_stock_snapshots(symbols)` — trade+quote+bars+prev-day per symbol
- `get_market_clock()` / `get_market_calendar(start, end)`
- `get_corporate_actions(symbols, types, start, end, limit)`

### Symbols, alerts, diagnostics
- `set_interest_symbols(symbols, mode)` / `get_interest_symbols()`
- `get_news_alerts(minutes, severity, categories, symbols, limit)` /
  `ack_news_alert(alert_id)` — alerts carry `direction: bullish|bearish|neutral`
- `get_news_symbol_map`, `get_news_latency_stats`, `get_news_ingestion_stats`,
  `get_raw_news_events`
- `get_news_stream_health()` / `get_news_subscription_state()` /
  `get_stock_stream_health()`

All tools enforce bounded responses and return structured errors
(`{"error":"limit_exceeded",...}`, `{"error":"stock_stream_disabled"}`, ...).

## Resources

`alpaca-news://health|subscription|recent|alerts|symbols|symbols/{s}|article/{id}|stats/latency|stats/ingestion`
and `alpaca-market://watchlist|latest/{symbol}|clock|halts`.

## Architecture in 30 seconds

```
Alpaca news WS ──▶ recv loop ─▶ queue ─▶ batch persister ─▶ news SQLite (WAL, FTS5)
                                             │                    ▲
                                             ├─▶ AlertEngine ─────┘ (one alert feed)
Alpaca stock WS ─▶ recv loop ─▶ queue ─▶ batch persister ─▶ market SQLite (WAL)
   (msgpack)                                 └─▶ snapshot cache (latest per symbol)
REST (TTL-cached): backfill, bars gap-fill, screener, snapshots, clock/calendar

MCP tools/resources read ONLY from the SQLite stores + snapshot cache + REST cache.
```

Both workers share `ws_base.BaseStreamWorker`: bounded queue with backpressure
and visible drops, drain-batched commits (no added latency for single
messages), idle watchdog, backoff reset after stable sessions, and slow-cadence
auth-failure retry with critical alerts. Normalization (HTML stripping) runs in
a thread so bursts never stall the recv loops.

## Troubleshooting

- **`402 auth failed`** — bad credentials. The worker retries every
  `AUTH_RETRY_SECONDS` and records critical alerts; `/healthz` shows
  `auth_failed: true`.
- **`406 connection limit exceeded`** — another process is using your Alpaca
  slot for that endpoint. Stop other clients; the server recovers after the
  backoff.
- **`409 insufficient subscription`** — your plan doesn't grant the feed; the
  server flags `entitlement_error` (news falls back to REST/symbol list).
- **Stale feed** — `stale_reconnects` in health counts watchdog-forced
  reconnects; `stream_stale` alerts fire at most hourly.
- **Market DB too big** — set `STOCK_QUOTE_SAMPLE_MS=250` (or trim
  `STOCK_CHANNELS` / watchlist); snapshot cache stays exact regardless.

## Test commands

```bash
python -m pytest tests -q          # unit + fixture + fake-WS tests (offline)
python -m ruff check src tests
python -m mypy src
docker build -t alpaca-news-mcp .
```

Credential-free live smoke test of the stock pipeline: set
`ALPACA_STOCK_STREAM_URL=wss://stream.data.alpaca.markets/v2/test`,
`STOCK_WATCHLIST_SYMBOLS=FAKEPACA` and watch `get_latest_market_data`
(the test stream emits fake `FAKEPACA` data around the clock).

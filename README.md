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
  Cursors are durable (persisted high-water marks survive pruning/restarts),
  self-describing (`latest_cursor` on every response, `cursor_out_of_range`
  with a recovery hint, `gap=true` when retention pruned past you), and
  bootstrap-friendly (`cursor=-1` = start from now).
- One-call polling: `poll_market` returns news+alerts+bars deltas, a
  watchlist snapshot block, and a loop-context header (market phase,
  seconds to close, suggested next poll interval, data-trust flags) —
  one tool call per bot loop tick instead of five. `get_started` teaches
  the whole contract in one call.
- Runtime news watches: the bot can add/remove alert keyword groups
  (`set_alert_keyword_group`) and follow developing stories
  (`watch_story`) without a restart; both persist across restarts.
- Derived market alerts: `price_move`, `volume_spike`, and
  `day_range_break` triggers computed server-side from minute bars, plus
  market-wide trading-halt coverage via a `statuses:["*"]` subscription
  (non-watchlist halts alert at low severity as discovery signal).
- Server-side analytics: `get_symbol_context` (RSI/EMA/ATR/VWAP-distance/
  relative volume/range position per symbol), `get_tape_stats`
  (microstructure digest), `get_watchlist_pulse`, `get_news_pulse`,
  `get_market_pulse` (movers fused with local news/halt/watchlist state),
  and 5min/15min/1hour bar aggregation — thousands of raw rows distilled
  into bounded scalar answers.
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
| `STATUS_RETENTION_DAYS` | `30` | Halt/LULD retention (day-scale — an active halt must stay visible) |
| `MARKET_RETENTION_INTERVAL_SECONDS` | `900` | Market pruner cadence |
| `MAX_WATCHLIST_SYMBOLS` | `100` (max 500) | Local watchlist cap (Alpaca's paid plan has no stream symbol limit) |
| `STOCK_STATUS_WILDCARD` | `true` | Subscribe `statuses:["*"]` for market-wide halt coverage |
| `WATCHLIST_ADD_BACKFILL_MINUTES` | `240` | REST 1min-bar backfill for newly added watchlist symbols |
| `GAP_FILL_MAX_LOOKBACK_MINUTES` | `1440` | Per-symbol clamp on reconnect bar gap-fill |

Reliability & alerts:

| Variable | Default | Purpose |
| --- | --- | --- |
| `STABLE_CONNECTION_SECONDS` | `60` | Session length that resets reconnect backoff |
| `AUTH_RETRY_SECONDS` / `AUTH_ALERT_EVERY_N` | `900` / `4` | 402 retry cadence + re-alert period |
| `RECONNECT_MIN_SECONDS` / `RECONNECT_MAX_SECONDS` | `5` / `120` | Backoff bounds |
| `CONNECTION_LIMIT_BACKOFF_SECONDS` | `90` | Sleep after `406` |
| `ALERT_KEYWORDS_FILE` | (empty) | JSON merged over built-in keyword groups |
| `ALERT_RATE_LIMIT_PER_SYMBOL_HOUR` | `10` | Per-(symbol,category) cap; critical exempt; co-mentions charge only under-quota symbols |
| `HALT_ALERT_DEDUP_SECONDS` | `300` | Halt/LULD/derived alert dedup window |
| `PRICE_MOVE_ALERT_PCT` / `PRICE_MOVE_WINDOW_MINUTES` | `3.0` / `5` | Derived price_move alert (0 disables) |
| `VOLUME_SPIKE_RATIO` | `5.0` | Derived volume_spike alert (0 disables) |

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

### The trading loop (start here)
- `get_started()` — one-call orientation: loop recipe, cursor contract,
  timestamp units, error schema, current cursors/watchlist, retention.
- `poll_market(news_cursor, alerts_cursor, bars_cursor, symbols, include,
  ..., min_severity)` — ONE call per loop tick: news+alerts+bars deltas,
  watchlist snapshot block (with per-symbol `age_seconds`), and a context
  block (`market_phase`, `seconds_to_close`, `suggested_next_poll_seconds`,
  `degraded` data-trust flags). Sections fail independently.

### Delta polling (individual feeds)
- `get_news_since(cursor, limit, symbols, fields)` — articles newer than the
  cursor, oldest first; updated articles re-surface. `fields=compact` is ~6
  fields per article.
- `get_alerts_since(cursor, limit, severity, min_severity, categories)`
- `get_bars_since(cursor, symbols, timeframe, limit)` — compact array rows.

Each returns `next_cursor` + `latest_cursor` + `has_more`; pass
`next_cursor` back verbatim. `cursor=-1` starts from the tail;
`cursor_out_of_range` errors carry the recovery cursor; `gap=true` +
`oldest_available_cursor` mean retention pruned rows you never saw.

### News
- `get_recent_news(minutes, symbols, sources, limit, include_content, include_raw, fields)`
- `get_news_article(article_id, ...)`, `get_news_versions(article_id, limit)`
- `search_news(query, symbols, since, until, limit, fields)` — FTS5 ranked
  (porter stemming), substring LIKE fallback
- `get_news_for_symbols(symbols, minutes, limit_per_symbol, fields)`
- `get_breaking_news_digest(minutes, symbols, max_articles)` — honest:
  `breaking_matches=false` + empty list when nothing breaking happened
- `get_news_pulse(symbols, minutes, top)` — per-symbol article velocity,
  bullish/bearish skew, alert-category counts, latest headline
- `fetch_news_history(symbols, days, max_articles)` — deep REST fetch
  (years back) for evaluating unfamiliar tickers; lands in the local store
- `run_news_rest_backfill(minutes)`

### News watches (runtime, persisted)
- `set_alert_keyword_group(group, phrases, critical_phrases, bullish,
  bearish)` / `delete_alert_keyword_group(group)` /
  `get_alert_keyword_groups()` — retarget the news radar intraday; new
  groups alert as `custom_keyword` (>= medium), built-in groups keep their
  native category
- `watch_story(article_id)` / `unwatch_story` / `get_watched_stories` —
  follow a developing article; every content update emits a high-severity
  `watched_story_update` alert

### Market data (stream-backed)
- `get_latest_market_data(symbols, include)` — in-memory snapshot cache;
  REST fallback for non-watchlist symbols; per-symbol `age_seconds` +
  `stale` flags so dead prices are never mistaken for live ones
- `get_trades_window(symbol, minutes, limit)` / `get_quotes_window(...)`
- `get_stock_bars(symbol, timeframe, start, end, limit)` — timeframes
  `1min|5min|15min|1hour|1day` (aggregates computed server-side); REST
  fallback (`source: "rest"`) when the local store is empty
- `get_trading_halts(minutes, symbols)` — statuses + LULD, retained
  `STATUS_RETENTION_DAYS`
- `set_stock_watchlist(symbols, mode)` / `get_stock_watchlist()` — live
  subscribe/unsubscribe deltas, persisted across restarts; newly added
  symbols get an automatic REST bar backfill

### Market analytics (server-computed, local data)
- `get_symbol_context(symbols)` — ~15 scalars per symbol: last/change/gap,
  session-VWAP distance, RSI-14, EMA-9/21, ATR-14, range position,
  time-adjusted relative volume, halted flag
- `get_tape_stats(symbol, minutes, bucket_seconds, block_size)` — tape
  speed, uptick/downtick, block trades, spread bps, bid-size imbalance
- `get_watchlist_pulse(minutes, symbols)` — one row per symbol:
  last/chg%/volume/trades/spread/halted
- `get_market_pulse(top, news_minutes)` — movers + most-actives fused with
  local news counts, halt state, watchlist membership (discovery)

### Market context (REST, TTL-cached)
- `get_market_movers(top)` / `get_most_active_stocks(by, top)` — "what's in
  play today"
- `get_stock_snapshots(symbols)` — trade+quote+bars+prev-day per symbol
- `get_market_clock()` / `get_market_calendar(start, end)`
- `get_corporate_actions(symbols, types, start, end, limit)`

### Symbols, alerts, diagnostics
- `set_interest_symbols(symbols, mode)` / `get_interest_symbols()`
- `get_news_alerts(minutes, severity, categories, symbols, limit)` /
  `ack_news_alert(alert_id)` — alerts carry `direction: bullish|bearish|neutral`;
  categories now include `custom_keyword`, `watched_story_update`,
  `price_move`, `volume_spike`, `day_range_break`, `persist_failure`
- `get_news_symbol_map`, `get_news_latency_stats`, `get_news_ingestion_stats`,
  `get_raw_news_events`
- `get_news_stream_health()` / `get_news_subscription_state()` /
  `get_stock_stream_health()`

All tools enforce bounded responses and return structured errors with
recovery hints (`{"error":"limit_exceeded","hint":...}`); transient
upstream failures carry `retryable: true`. Timestamp units: news =
ISO-8601, ticks = `ts_us` (epoch microseconds UTC), bars = `ts` (epoch
seconds UTC).

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

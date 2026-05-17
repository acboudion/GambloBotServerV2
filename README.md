# alpaca-news-mcp

Docker-served MCP server that ingests Alpaca's real-time news WebSocket and exposes
the data to MCP clients (Claude Code, Codex) over Streamable HTTP.

The server maintains exactly **one** Alpaca news WebSocket connection per running
container, normalizes and persists every news article to a local SQLite database,
and serves bounded read-only MCP tools and resources backed by that local cache.

## What this MCP does

- Connects to `wss://stream.data.alpaca.markets/v1beta1/news` once.
- Authenticates with header-based auth, falling back to message auth.
- Subscribes with `news:["*"]` for maximum coverage.
- Persists every `T="n"` news article (normalized + raw payload).
- Indexes articles by symbol and tracks per-article version history when
  `headline`, `summary`, `content`, or `updated_at` changes.
- Generates deterministic alerts (M&A, financing, earnings, analyst, legal,
  halt risk, breaking, interest-symbol matches, high latency, stream errors).
- Optionally backfills on startup and after reconnects via Alpaca's REST news API.
- Exposes everything through MCP tools and resources at `http://localhost:8000/mcp`.
- Exposes a plain `/healthz` HTTP endpoint for Docker healthchecks.

## What this MCP does NOT do

- It does **not** place trades, cancel orders, or call any trading endpoints.
- It does **not** open more than one Alpaca news WebSocket.
- It does **not** integrate FMP, FRED, SEC EDGAR, or any other data provider.
- It does **not** subscribe to stock trades/quotes/bars/statuses/LULDs, options, or
  crypto streams.
- It does **not** make trading decisions from headlines.

## ⚠️ Alpaca connection-limit warning

Alpaca limits Algo Trader Plus and most other subscriptions to **one active
WebSocket connection per user per endpoint**. A second connection returns
`406 connection limit exceeded`.

To stay safe:

- **Run one container at a time.** `docker-compose.yml` enforces `replicas: 1`.
- **Do not run a second copy of the server** with the same Alpaca credentials.
- **Prefer Streamable HTTP MCP transport** (this repo's default). Many Claude/Codex
  clients can share one running container that owns one Alpaca WebSocket. stdio
  mode is risky because it can launch a new container per client.
- On `406` the server backs off `CONNECTION_LIMIT_BACKOFF_SECONDS` (default 90s)
  and surfaces the state via `get_news_stream_health()` and `/healthz`.

## Required environment variables

Copy `.env.example` to `.env` and edit:

| Variable | Required | Default | Purpose |
| --- | --- | --- | --- |
| `ALPACA_API_KEY` | yes | — | Alpaca API key id |
| `ALPACA_SECRET_KEY` | yes | — | Alpaca API secret |
| `ALPACA_NEWS_STREAM_URL` | no | `wss://stream.data.alpaca.markets/v1beta1/news` | Override for sandbox |
| `ALPACA_DATA_BASE_URL` | no | `https://data.alpaca.markets` | REST base URL |
| `MCP_HOST` / `MCP_PORT` / `MCP_PATH` | no | `0.0.0.0` / `8000` / `/mcp` | HTTP binding |
| `STORAGE_PATH` | no | `/data/alpaca_news.sqlite` | SQLite DB path |
| `LOG_LEVEL` | no | `info` | `debug`/`info`/`warning`/`error` |
| `NEWS_SUBSCRIPTION_MODE` | no | `wildcard` | `wildcard` or `fallback` |
| `NEWS_FALLBACK_SYMBOLS` | no | (empty) | Comma list used if wildcard rejected |
| `NEWS_INTEREST_SYMBOLS` | no | `AAPL,MSFT,NVDA,TSLA,SPY,QQQ` | Local-only filter for alerts |
| `ENABLE_REST_BACKFILL` | no | `true` | Backfill on startup + gap-fill on reconnect |
| `BACKFILL_LOOKBACK_MINUTES` | no | `60` | Startup window |
| `REST_BACKFILL_OVERLAP_SECONDS` | no | `180` | Reconnect overlap |
| `REST_INCLUDE_CONTENT` | no | `true` | Pass `include_content=true` to REST |
| `REST_EXCLUDE_CONTENTLESS` | no | `false` | Pass `exclude_contentless=true` to REST |
| `MAX_RECENT_ARTICLES_MEMORY` | no | `5000` | In-memory deque bound |
| `EVENT_RETENTION_DAYS` | no | `14` | Article retention |
| `RAW_EVENT_RETENTION_DAYS` | no | `7` | Raw-event retention |
| `RECONNECT_MIN_SECONDS` / `RECONNECT_MAX_SECONDS` | no | `5` / `120` | Backoff bounds |
| `CONNECTION_LIMIT_BACKOFF_SECONDS` | no | `90` | Sleep after `406` |
| `QUEUE_MAXSIZE` | no | `10000` | Bounded WS-to-persister queue |
| `ENABLE_MANUAL_REST_BACKFILL` | no | `true` | Exposes `run_news_rest_backfill` tool |
| `MCP_READ_ONLY` | no | `true` | Reserved for future use |

Secrets are never logged or exposed via tools/resources.

## Docker Compose setup

```bash
cp .env.example .env
# edit .env with your real Alpaca credentials

docker compose up --build -d

# verify health
curl http://localhost:8000/healthz
```

The compose file mounts a named volume `alpaca-news-data` at `/data` so the SQLite
file (and its WAL/SHM siblings) survive container restarts.

## Claude Code setup

```bash
claude mcp add --transport http alpaca-news http://localhost:8000/mcp
```

After this, the alpaca-news MCP appears in Claude Code's tool list.

## Codex setup

CLI:

```bash
codex mcp add alpaca_news --url http://localhost:8000/mcp
codex mcp list
```

Or in `~/.codex/config.toml`:

```toml
[mcp_servers.alpaca_news]
url = "http://localhost:8000/mcp"
startup_timeout_sec = 20
tool_timeout_sec = 60
enabled = true
```

## Tools

### Health and status
- `get_news_stream_health` — connection state, subscription state, counters, last-message timestamps.
- `get_news_subscription_state` — requested vs Alpaca-acknowledged subscription.

### Article retrieval
- `get_recent_news(minutes, symbols, sources, limit, include_content, include_raw)`
- `get_news_article(article_id, include_content, include_raw, include_versions, version_limit)`
- `search_news(query, symbols, since, until, limit, include_content)`
- `get_news_for_symbols(symbols, minutes, limit_per_symbol, include_content)`
- `get_breaking_news_digest(minutes, symbols, max_articles)`

### Symbols & alerts
- `set_interest_symbols(symbols, mode=replace|add|remove|clear)`
- `get_interest_symbols`
- `get_news_alerts(minutes, severity, categories, symbols, limit)`
- `ack_news_alert(alert_id)`
- `get_news_symbol_map(minutes, min_articles)`

### Diagnostics
- `get_news_latency_stats(minutes)`
- `get_news_ingestion_stats(minutes)`
- `get_raw_news_events(minutes, limit)`
- `get_news_versions(article_id, limit)`
- `run_news_rest_backfill(minutes)` (returns `{"error":"disabled"}` when
  `ENABLE_MANUAL_REST_BACKFILL=false`)

All tools enforce response limits (default 50 articles, 100 raw events,
50 versions, 4000 chars per content_text; hard caps 200 articles, 500 raw
events, 200 versions, 500 alerts) and return
`{"error":"limit_exceeded","max_allowed":N,"requested":M}` when exceeded.
`get_news_versions` and `get_news_article(include_versions=True)` also
report `versions_total` so callers can detect truncation.

## Resources

- `alpaca-news://health`
- `alpaca-news://subscription`
- `alpaca-news://recent`
- `alpaca-news://alerts`
- `alpaca-news://symbols`
- `alpaca-news://symbols/{symbol}`
- `alpaca-news://article/{article_id}`
- `alpaca-news://stats/latency`
- `alpaca-news://stats/ingestion`

## REST backfill behavior

- **Startup**: when `ENABLE_REST_BACKFILL=true`, the server queries Alpaca News REST
  for the last `BACKFILL_LOOKBACK_MINUTES` minutes (sort=asc, limit=50, paginated
  by `next_page_token`) before opening the WebSocket.
- **Reconnect gap-fill**: after a successful reconnect, the server re-queries from
  `last_seen_updated_at - REST_BACKFILL_OVERLAP_SECONDS` to recover anything missed
  during the disconnect.
- **Manual**: `run_news_rest_backfill(minutes=N)` runs an ad-hoc backfill (subject
  to `ENABLE_MANUAL_REST_BACKFILL`).
- **Rate limits**: `429` honors `Retry-After`; `403` marks `entitlement_error`.

## Troubleshooting

- **`402 auth failed`** — check `ALPACA_API_KEY`/`ALPACA_SECRET_KEY`. The server
  marks this fatal and stops reconnecting until restarted with new credentials.
- **`406 connection limit exceeded`** — another process is using your Alpaca slot.
  Stop any other Alpaca WebSocket clients (including older containers, paper
  scripts, etc.) and the server will recover after `CONNECTION_LIMIT_BACKOFF_SECONDS`.
- **`407 slow client`** — the server should never trigger this in practice
  thanks to its bounded queue + persister coroutine. If you see it, increase
  `QUEUE_MAXSIZE` or check for I/O stalls.
- **`409 insufficient subscription`** — your Alpaca plan doesn't grant the
  requested feed. The server marks `entitlement_error=true` and falls back to
  REST if enabled.
- **No articles received** — confirm `/healthz` shows `stream_connected=true`
  and `authenticated=true`, then check `get_news_subscription_state` for the
  Alpaca-acknowledged subscription.

## Security notes

- Never commit `.env`. The `.dockerignore` and `.gitignore` exclude it.
- API keys are read from environment, never logged. The logger has a
  redaction filter that scrubs them from any log line where they appear.
- Article HTML is treated as untrusted and stripped to plain text before being
  returned via tools/resources. Raw HTML remains in storage for fidelity but is
  only returned when explicitly requested with `include_raw=true`.
- No tool calls non-Alpaca APIs. No tool places trades.

## Test commands

```bash
# Unit + fixture + WebSocket fixture tests (offline)
python -m pytest tests -q

# Lint & types
python -m ruff check src tests
python -m mypy src

# Build the Docker image
docker build -t alpaca-news-mcp .
```

## Architecture in 30 seconds

```
Alpaca WS  →  recv loop  →  bounded asyncio.Queue  →  persister
                                                          ↓
                                          normalize → SQLite (WAL) + caches
                                                          ↓
                                                   AlertEngine (deterministic)
                                                          ↑
                                MCP tools/resources read here only
```

The recv loop only does `orjson.loads` + `queue.put_nowait`. All persistence,
versioning, alerting, and retention pruning happen on the persister coroutine
or a background retention task.

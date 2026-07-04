# AGENTS.md

You are coding an Alpaca market-data MCP server (news + stock streams).

Do not integrate FMP, FRED, SEC EDGAR, or any non-Alpaca data provider.
Never place, modify, or cancel orders — no trading endpoints. The only
trading-API host usage allowed is read-only GET /v2/clock and /v2/calendar.

Primary objective:
- A Docker-runnable MCP server exposing Alpaca real-time news AND the v2
  stock market-data stream (trades/quotes/bars/statuses/LULD) through
  Streamable HTTP MCP, as the data layer for an LLM trading harness.

Important Alpaca rules:
- Connection limits are per data endpoint: open at most ONE news WebSocket
  and at most ONE stock WebSocket per process (BaseStreamWorker enforces a
  per-subclass singleton).
- The default news subscription is {"action":"subscribe","news":["*"]}.
- The stock stream additionally subscribes statuses:["*"] as a SEPARATE
  frame when STOCK_STATUS_WILDCARD is on (market-wide halt coverage;
  statuses are a few tiny events per day). Watchlist deltas must not touch
  the statuses channel while the wildcard is live.
- Trust Alpaca subscription acknowledgements, not requested subscriptions.
- On 406 connection limit exceeded, do not busy-loop. Surface the error and back off.
- On 402 auth failure, retry on the slow AUTH_RETRY_SECONDS cadence with alerts.

Implementation requirements:
- Python 3.12+, official MCP Python SDK / FastMCP.
- Two SQLite (WAL) databases: news (+alerts) and market ticks/bars — keep them
  separate so quote bursts never contend with news writes.
- Async WebSocket ingestion through the shared ws_base.BaseStreamWorker:
  bounded queues, drain-batched persistence (executemany + single commit),
  idle watchdog, backoff reset after stable sessions.
- Bounded MCP tool responses with structured errors ({"error": ...}).
- Preserve raw Alpaca messages for news and for low-volume stock messages
  (statuses/LULDs, corrections, cancelErrors, unknown types). High-volume
  ticks (trades/quotes/bars) are flattened losslessly into columns instead —
  raw copies at full quote depth would multiply write volume for no signal.
- Strip/sanitize article HTML off the event loop.
- Schema changes go through migrations.py (PRAGMA user_version); schema.sql /
  market_schema.sql describe the final shape with IF NOT EXISTS only.
- Never log or expose Alpaca credentials.

Testing requirements:
- pytest for normalization, storage (both DBs), alerts, backfill, migrations,
  MCP tools/resources, and WebSocket behavior against a local fake server
  (fixtures cover both JSON and msgpack codecs).
- Run: python -m pytest tests -q && python -m ruff check src tests && python -m mypy src
- Build and run the Docker container before claiming completion.

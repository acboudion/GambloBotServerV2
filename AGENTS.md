# AGENTS.md

You are coding an Alpaca-only News WebSocket MCP server.

Do not integrate FMP, FRED, SEC EDGAR, order placement, trading WebSockets, stock streams, option streams, or crypto streams.

Primary objective:
- Build a Docker-runnable MCP server that exposes Alpaca real-time news through Streamable HTTP MCP.

Important Alpaca rule:
- The server must open only one Alpaca News WebSocket connection.
- The default subscription is {"action":"subscribe","news":["*"]}.
- Trust Alpaca subscription acknowledgements, not requested subscriptions.
- On 406 connection limit exceeded, do not busy-loop. Surface the error and back off.

Implementation requirements:
- Use Python 3.12+.
- Use the official MCP Python SDK / FastMCP.
- Use SQLite WAL for persistence.
- Use async WebSocket ingestion.
- Use bounded MCP tool responses.
- Preserve raw Alpaca messages.
- Strip/sanitize article HTML before returning readable text.
- Optional Alpaca REST backfill is allowed only for startup and reconnect gap-fill.
- Never log or expose Alpaca credentials.
- Never place trades.

Testing requirements:
- Write pytest tests for normalization, storage, alerts, backfill, MCP tools, and WebSocket error handling.
- Build and run the Docker container before claiming completion.

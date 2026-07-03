"""Console-script entrypoint: starts uvicorn with the FastMCP + /healthz Starlette app."""

from __future__ import annotations

import uvicorn

from .config import Config
from .logging_utils import configure_logging
from .server import build_mcp, build_starlette_app


def main() -> None:
    cfg = Config.from_env()
    configure_logging(cfg.log_level)
    mcp = build_mcp(mcp_path=cfg.mcp_path)
    app = build_starlette_app(mcp, cfg)

    uvicorn.run(
        app,
        host=cfg.mcp_host,
        port=cfg.mcp_port,
        log_level=cfg.log_level,
        access_log=False,
    )


if __name__ == "__main__":
    main()

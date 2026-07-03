"""Logging with mandatory secret redaction."""

from __future__ import annotations

import logging
import os
import sys


class SecretRedactingFilter(logging.Filter):
    """Strips Alpaca credentials from any log record's message and args."""

    def __init__(self) -> None:
        super().__init__()
        self._secrets: list[str] = []
        self._refresh()

    def _refresh(self) -> None:
        secrets: list[str] = []
        for name in ("ALPACA_API_KEY", "ALPACA_SECRET_KEY"):
            v = os.getenv(name)
            if v and len(v) >= 4:
                secrets.append(v)
        self._secrets = secrets

    def filter(self, record: logging.LogRecord) -> bool:
        if not self._secrets:
            return True
        try:
            msg = record.getMessage()
        except Exception:
            return True
        redacted = msg
        for s in self._secrets:
            if s in redacted:
                redacted = redacted.replace(s, "***REDACTED***")
        if redacted != msg:
            record.msg = redacted
            record.args = ()
        return True


def configure_logging(level: str = "info") -> None:
    log_level = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    handler.addFilter(SecretRedactingFilter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(log_level)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

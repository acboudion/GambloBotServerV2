"""Process-wide AppState container shared across MCP tools, resources, and lifespan."""

from __future__ import annotations

from dataclasses import dataclass

from .alerts import AlertEngine
from .config import Config
from .rest_backfill import RestBackfillWorker
from .state import State
from .store import Store
from .stream import NewsStreamWorker


@dataclass
class AppState:
    config: Config
    store: Store
    state: State
    alerts: AlertEngine
    stream: NewsStreamWorker
    rest_backfill: RestBackfillWorker


_app_state: AppState | None = None


def set_app_state(app_state: AppState) -> None:
    global _app_state
    _app_state = app_state


def get_app_state() -> AppState:
    if _app_state is None:
        raise RuntimeError("AppState has not been initialized")
    return _app_state


def clear_app_state() -> None:
    global _app_state
    _app_state = None

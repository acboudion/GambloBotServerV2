"""Shared MCP tool response helpers.

One error vocabulary for every tool: the legacy keys (error, max_allowed,
requested, valid, ...) stay byte-compatible, and every error additionally
carries a `hint` — the concrete recovery action an LLM caller should take —
plus `retryable` where a plain retry is the right move.
"""

from __future__ import annotations

from typing import Any, get_args

from .models import AlertCategory, Severity

VALID_FIELD_MODES = ("compact", "standard", "full")
SEVERITIES: tuple[str, ...] = get_args(Severity)
ALERT_CATEGORIES: tuple[str, ...] = get_args(AlertCategory)
# Ascending significance, for min_severity filtering.
SEVERITY_ORDER = ("low", "medium", "high", "critical")


def err(code: str, hint: str | None = None, **fields: Any) -> dict[str, Any]:
    out: dict[str, Any] = {"error": code, **fields}
    if hint:
        out["hint"] = hint
    return out


def limit_exceeded(max_allowed: int, requested: int) -> dict[str, Any]:
    return err(
        "limit_exceeded",
        f"retry with a value <= {max_allowed}",
        max_allowed=max_allowed,
        requested=requested,
    )


def filter_over_cap(values: list[str] | None, cap: int) -> dict[str, Any] | None:
    """Optional list filters (symbols/categories/sources) become one SQL
    placeholder each — reject oversized lists structurally instead of
    exceeding SQLite's variable limit and raising out of the tool."""
    if values and len(values) > cap:
        return limit_exceeded(cap, len(values))
    return None


def invalid_fields(fields: str) -> dict[str, Any]:
    return err(
        "invalid_fields",
        f"use one of: {', '.join(VALID_FIELD_MODES)}",
        fields=fields,
        valid=list(VALID_FIELD_MODES),
    )


def invalid_severity(value: str) -> dict[str, Any]:
    return err(
        "invalid_severity",
        f"use one of: {', '.join(SEVERITIES)} (filters are exact-match; "
        "use min_severity for 'this level and above')",
        severity=value,
        valid=list(SEVERITIES),
    )


def invalid_categories(invalid: list[str]) -> dict[str, Any]:
    return err(
        "invalid_categories",
        "remove the invalid entries; category names are exact-match",
        invalid=invalid,
        valid=list(ALERT_CATEGORIES),
    )


def invalid_date(field: str, value: str) -> dict[str, Any]:
    """A caller-provided date that failed to parse must be a structured error
    — silently falling back to a default window would return valid-looking
    data for the wrong dates."""
    return err(
        "invalid_date",
        "pass an ISO-8601 timestamp like 2026-07-04T14:30:00Z (or YYYY-MM-DD)",
        field=field,
        value=value,
    )


def cursor_out_of_range(cursor: int, latest: int) -> dict[str, Any]:
    """A cursor above the allocation high-water mark can never return data
    (stale bot state, or a swapped database) — starving silently would look
    identical to 'no news', so it must be a structured, recoverable error."""
    return err(
        "cursor_out_of_range",
        "pass latest_cursor back (or cursor=-1) to resume from the tail",
        cursor=cursor,
        latest_cursor=latest,
    )


def tail_response(items_key: str, latest: int) -> dict[str, Any]:
    """cursor=-1 bootstrap: subscribe from now without replaying history."""
    return {
        "count": 0,
        items_key: [],
        "next_cursor": latest,
        "latest_cursor": latest,
        "has_more": False,
        "note": "started_from_tail",
    }


def stream_disabled() -> dict[str, Any]:
    return err(
        "stock_stream_disabled",
        "the stock stream is off (ENABLE_STOCK_STREAM=false); news tools and "
        "REST market-context tools (snapshots, movers, clock) still work",
    )


def client_unavailable() -> dict[str, Any]:
    return err(
        "market_client_unavailable",
        "the REST market client is not configured; stream-backed tools may "
        "still work",
    )


def upstream_error() -> dict[str, Any]:
    return err(
        "alpaca_request_failed",
        "transient upstream failure; retry shortly",
        retryable=True,
    )


def severities_at_or_above(min_severity: str) -> list[str]:
    """['high'] -> ['high', 'critical'] etc. Caller validates membership."""
    idx = SEVERITY_ORDER.index(min_severity)
    return list(SEVERITY_ORDER[idx:])


__all__ = [
    "ALERT_CATEGORIES",
    "SEVERITIES",
    "SEVERITY_ORDER",
    "VALID_FIELD_MODES",
    "client_unavailable",
    "cursor_out_of_range",
    "err",
    "filter_over_cap",
    "invalid_categories",
    "invalid_date",
    "invalid_fields",
    "invalid_severity",
    "limit_exceeded",
    "severities_at_or_above",
    "stream_disabled",
    "tail_response",
    "upstream_error",
]

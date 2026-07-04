"""Pure indicator math over locally stored bars.

Deterministic, dependency-free helpers used by get_symbol_context: an LLM
doing RSI/ATR arithmetic over 390-row bar arrays is slow, token-hungry, and
error-prone — the server computes ~15 scalars per symbol instead. All
functions accept bar dicts shaped like MarketStore.bars_window rows
(ts/open/high/low/close/volume/trade_count/vwap) and return None when the
input is insufficient rather than guessing.
"""

from __future__ import annotations

from itertools import pairwise
from typing import Any

Bar = dict[str, Any]


def _closes(bars: list[Bar]) -> list[float]:
    return [float(b["close"]) for b in bars if b.get("close") is not None]


def rsi(bars: list[Bar], period: int = 14) -> float | None:
    """Wilder-smoothed RSI over closing prices."""
    closes = _closes(bars)
    if len(closes) < period + 1:
        return None
    gains: list[float] = []
    losses: list[float] = []
    for prev, cur in pairwise(closes):
        delta = cur - prev
        gains.append(max(delta, 0.0))
        losses.append(max(-delta, 0.0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for gain, loss in zip(gains[period:], losses[period:], strict=True):
        avg_gain = (avg_gain * (period - 1) + gain) / period
        avg_loss = (avg_loss * (period - 1) + loss) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - 100.0 / (1.0 + rs), 2)


def ema(bars: list[Bar], span: int) -> float | None:
    closes = _closes(bars)
    if len(closes) < span:
        return None
    alpha = 2.0 / (span + 1.0)
    value = sum(closes[:span]) / span  # SMA seed
    for close in closes[span:]:
        value = alpha * close + (1.0 - alpha) * value
    return round(value, 4)


def atr(bars: list[Bar], period: int = 14) -> float | None:
    """Wilder-smoothed average true range."""
    rows = [
        b
        for b in bars
        if b.get("high") is not None
        and b.get("low") is not None
        and b.get("close") is not None
    ]
    if len(rows) < period + 1:
        return None
    trs: list[float] = []
    for prev, cur in pairwise(rows):
        prev_close = float(prev["close"])
        high = float(cur["high"])
        low = float(cur["low"])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    value = sum(trs[:period]) / period
    for tr in trs[period:]:
        value = (value * (period - 1) + tr) / period
    return round(value, 4)


def session_vwap(bars: list[Bar]) -> float | None:
    """Volume-weighted average price across the given bars (each bar's own
    vwap weighted by its volume)."""
    num = 0.0
    den = 0.0
    for b in bars:
        vwap = b.get("vwap")
        volume = b.get("volume")
        if vwap is None or not volume:
            continue
        num += float(vwap) * float(volume)
        den += float(volume)
    if den <= 0:
        return None
    return round(num / den, 4)


def day_range_position(last: float | None, high: float | None, low: float | None) -> float | None:
    """0.0 = at the low, 1.0 = at the high."""
    if last is None or high is None or low is None or high <= low:
        return None
    return round((last - low) / (high - low), 3)


def gap_pct(today_open: float | None, prev_close: float | None) -> float | None:
    if not today_open or not prev_close:
        return None
    return round((today_open - prev_close) / prev_close * 100.0, 2)


def pct_change(last: float | None, reference: float | None) -> float | None:
    if last is None or not reference:
        return None
    return round((last - reference) / reference * 100.0, 2)


def relative_volume(
    today_volume: float,
    prior_daily_volumes: list[float],
    elapsed_session_fraction: float,
) -> float | None:
    """Today's cumulative volume vs the prior-day average scaled to the same
    elapsed fraction of the session (a crude but honest time adjustment)."""
    prior = [v for v in prior_daily_volumes if v]
    if not prior or today_volume <= 0:
        return None
    fraction = min(max(elapsed_session_fraction, 1.0 / 390.0), 1.0)
    expected = (sum(prior) / len(prior)) * fraction
    if expected <= 0:
        return None
    return round(today_volume / expected, 2)


__all__ = [
    "atr",
    "day_range_position",
    "ema",
    "gap_pct",
    "pct_change",
    "relative_volume",
    "rsi",
    "session_vwap",
]

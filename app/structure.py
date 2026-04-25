from __future__ import annotations

import logging
from typing import Any

from app.models import Candle


def _closed_candles(candles: list[Candle]) -> list[Candle]:
    if len(candles) <= 1:
        return []
    return candles[:-1]


def _calculate_atr(candles: list[Candle], period: int = 14) -> float | None:
    if len(candles) < period + 1:
        return None

    tr_values: list[float] = []
    for index in range(1, len(candles)):
        current = candles[index]
        previous = candles[index - 1]
        tr_values.append(
            max(
                current.high - current.low,
                abs(current.high - previous.close),
                abs(current.low - previous.close),
            )
        )
    if len(tr_values) < period:
        return None
    window = tr_values[-period:]
    return sum(window) / len(window)


def detect_confirmed_pivots(candles: list[Candle], pivot_window: int = 3) -> dict[str, list[dict[str, float | int]]]:
    if pivot_window <= 0:
        raise ValueError("pivot_window debe ser mayor que cero.")

    closed = _closed_candles(candles)
    if len(closed) < (pivot_window * 2) + 1:
        return {"highs": [], "lows": []}

    pivot_highs: list[dict[str, float | int]] = []
    pivot_lows: list[dict[str, float | int]] = []

    for index in range(pivot_window, len(closed) - pivot_window):
        current = closed[index]
        left = closed[index - pivot_window : index]
        right = closed[index + 1 : index + 1 + pivot_window]

        if all(current.high > candle.high for candle in left + right):
            pivot_highs.append(
                {
                    "index": index,
                    "price": current.high,
                    "close_time": current.close_time,
                }
            )
        if all(current.low < candle.low for candle in left + right):
            pivot_lows.append(
                {
                    "index": index,
                    "price": current.low,
                    "close_time": current.close_time,
                }
            )

    return {"highs": pivot_highs, "lows": pivot_lows}


def detect_swing_high(candles: list[Candle], left: int = 2, right: int = 2) -> dict[str, float | int] | None:
    pivots = detect_confirmed_pivots(candles, pivot_window=max(left, right))
    if not pivots["highs"]:
        return None
    return pivots["highs"][-1]


def detect_swing_low(candles: list[Candle], left: int = 2, right: int = 2) -> dict[str, float | int] | None:
    pivots = detect_confirmed_pivots(candles, pivot_window=max(left, right))
    if not pivots["lows"]:
        return None
    return pivots["lows"][-1]


def detect_bos_bullish(candles: list[Candle], pivot_window: int = 3) -> bool:
    analysis = analyze_structure(candles, pivot_window=pivot_window)
    return str(analysis.get("bos", "NONE")) == "BULL"


def detect_bos_bearish(candles: list[Candle], pivot_window: int = 3) -> bool:
    analysis = analyze_structure(candles, pivot_window=pivot_window)
    return str(analysis.get("bos", "NONE")) == "BEAR"


def detect_higher_low(candles: list[Candle], pivot_window: int = 3) -> bool:
    analysis = analyze_structure(candles, pivot_window=pivot_window)
    return bool(analysis.get("hl", False))


def detect_lower_high(candles: list[Candle], pivot_window: int = 3) -> bool:
    analysis = analyze_structure(candles, pivot_window=pivot_window)
    return bool(analysis.get("lh", False))


def _structure_flags(pivot_highs: list[dict[str, float | int]], pivot_lows: list[dict[str, float | int]]) -> dict[str, Any]:
    last_high = float(pivot_highs[-1]["price"]) if pivot_highs else None
    prev_high = float(pivot_highs[-2]["price"]) if len(pivot_highs) >= 2 else None
    last_low = float(pivot_lows[-1]["price"]) if pivot_lows else None
    prev_low = float(pivot_lows[-2]["price"]) if len(pivot_lows) >= 2 else None

    hh = last_high is not None and prev_high is not None and last_high > prev_high
    lh = last_high is not None and prev_high is not None and last_high < prev_high
    hl = last_low is not None and prev_low is not None and last_low > prev_low
    ll = last_low is not None and prev_low is not None and last_low < prev_low

    bias = "MIX"
    if hh and hl:
        bias = "BULL"
    elif lh and ll:
        bias = "BEAR"

    return {
        "last_high": last_high,
        "prev_high": prev_high,
        "last_low": last_low,
        "prev_low": prev_low,
        "hh": hh,
        "lh": lh,
        "hl": hl,
        "ll": ll,
        "bias": bias,
    }


def _infer_choch(
    *,
    closed: list[Candle],
    pivot_highs: list[dict[str, float | int]],
    pivot_lows: list[dict[str, float | int]],
    previous_bias: str,
) -> str:
    if not closed:
        return "NONE"
    last_close = closed[-1].close

    if previous_bias == "BEAR":
        for index in range(len(pivot_highs) - 1, 0, -1):
            current = float(pivot_highs[index]["price"])
            previous = float(pivot_highs[index - 1]["price"])
            if current < previous and last_close > current:
                return "BULL"
    if previous_bias == "BULL":
        for index in range(len(pivot_lows) - 1, 0, -1):
            current = float(pivot_lows[index]["price"])
            previous = float(pivot_lows[index - 1]["price"])
            if current > previous and last_close < current:
                return "BEAR"

    return "NONE"


def analyze_structure(
    candles: list[Candle],
    *,
    pivot_window: int = 3,
    pullback_tolerance_mode: str = "atr",
    pullback_atr_mult: float = 0.25,
    pullback_pct: float = 0.15,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    closed = _closed_candles(candles)
    if not closed:
        return {
            "bias": "MIX",
            "bos": "NONE",
            "choch": "NONE",
            "pullback": "NONE",
            "pullback_clean": False,
            "summary": "sin velas cerradas",
            "pivots": {"highs": [], "lows": []},
            "hh": False,
            "hl": False,
            "lh": False,
            "ll": False,
            "sweep": "NONE",
            "price": None,
        }

    pivots = detect_confirmed_pivots(candles, pivot_window=pivot_window)
    pivot_highs = pivots["highs"]
    pivot_lows = pivots["lows"]
    flags = _structure_flags(pivot_highs, pivot_lows)

    last_close_candle = closed[-1]
    previous_close_candle = closed[-2] if len(closed) >= 2 else closed[-1]
    last_close = last_close_candle.close

    previous_bias = "MIX"
    if len(pivot_highs) >= 3 and len(pivot_lows) >= 3:
        prev_flags = _structure_flags(pivot_highs[:-1], pivot_lows[:-1])
        previous_bias = str(prev_flags["bias"])

    bos = "NONE"
    broken_level = None
    if flags["last_high"] is not None and last_close > float(flags["last_high"]):
        bos = "BULL"
        broken_level = float(flags["last_high"])
    elif flags["last_low"] is not None and last_close < float(flags["last_low"]):
        bos = "BEAR"
        broken_level = float(flags["last_low"])

    choch = _infer_choch(
        closed=closed,
        pivot_highs=pivot_highs,
        pivot_lows=pivot_lows,
        previous_bias=previous_bias,
    )

    atr_value = _calculate_atr(closed, period=14)
    tolerance_mode = pullback_tolerance_mode.strip().lower() or "atr"
    tolerance = (atr_value * pullback_atr_mult) if (tolerance_mode == "atr" and atr_value is not None) else (last_close * (pullback_pct / 100))

    pullback = "NONE"
    pullback_clean = False
    if broken_level is not None and tolerance is not None:
        near_zone = abs(last_close - broken_level) <= tolerance
        if bos == "BULL" and near_zone:
            pullback = "LONG"
            pullback_clean = last_close_candle.close > last_close_candle.open or last_close >= broken_level
        elif bos == "BEAR" and near_zone:
            pullback = "SHORT"
            pullback_clean = last_close_candle.close < last_close_candle.open or last_close <= broken_level

    sweep = "NONE"
    if len(closed) >= 2:
        prior = previous_close_candle
        latest = last_close_candle
        if latest.high > prior.high and latest.close < prior.high:
            sweep = "BEAR"
        elif latest.low < prior.low and latest.close > prior.low:
            sweep = "BULL"

    if logger is not None:
        logger.info(
            "Structure pivots | highs=%s lows=%s window=%s",
            len(pivot_highs),
            len(pivot_lows),
            pivot_window,
        )
        if bos != "NONE":
            logger.info("Structure BOS detected | bos=%s level=%s", bos, broken_level)
        if choch != "NONE":
            logger.info("Structure CHoCH detected | choch=%s prev_bias=%s", choch, previous_bias)
        if pullback != "NONE":
            logger.info(
                "Structure pullback detected | dir=%s clean=%s tol=%.8f",
                pullback,
                pullback_clean,
                tolerance,
            )

    summary = "estructura mixta"
    if flags["bias"] == "BULL":
        summary = "fuerza alcista"
    elif flags["bias"] == "BEAR":
        summary = "fuerza bajista"

    return {
        "bias": flags["bias"],
        "bos": bos,
        "choch": choch,
        "pullback": pullback,
        "pullback_clean": pullback_clean,
        "summary": summary,
        "pivots": pivots,
        "last_high": flags["last_high"],
        "prev_high": flags["prev_high"],
        "last_low": flags["last_low"],
        "prev_low": flags["prev_low"],
        "hh": flags["hh"],
        "lh": flags["lh"],
        "hl": flags["hl"],
        "ll": flags["ll"],
        "price": last_close,
        "previous_bias": previous_bias,
        "broken_level": broken_level,
        "tolerance": tolerance,
        "tolerance_mode": "atr" if (tolerance_mode == "atr" and atr_value is not None) else "pct",
        "atr": atr_value,
        "sweep": sweep,
    }

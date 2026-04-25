from app.models import Candle
from app.structure import (
    analyze_structure,
    detect_bos_bullish,
    detect_confirmed_pivots,
    detect_higher_low,
    detect_lower_high,
    detect_swing_high,
    detect_swing_low,
)


def _make_candles(closes: list[float]) -> list[Candle]:
    candles: list[Candle] = []
    for index, close in enumerate(closes):
        candles.append(
            Candle(
                open_time=1710000000000 + (index * 60000),
                open=close - 0.1,
                high=close + 0.3,
                low=close - 0.3,
                close=close,
                close_time=1710000059999 + (index * 60000),
                volume=100,
            )
        )
    return candles


def test_detect_confirmed_pivots_ignores_open_candle() -> None:
    candles = _make_candles([1.0, 1.2, 1.5, 1.1, 0.9, 1.0, 1.4, 1.3, 1.2])
    pivots = detect_confirmed_pivots(candles, pivot_window=2)
    assert len(pivots["highs"]) >= 1
    assert all(int(item["index"]) < len(candles) - 1 for item in pivots["highs"])
    assert all(int(item["index"]) < len(candles) - 1 for item in pivots["lows"])


def test_analyze_structure_returns_bias_flags() -> None:
    candles = _make_candles([1.00, 1.20, 1.40, 1.15, 1.35, 1.55, 1.30, 1.50, 1.70, 1.60, 1.65])
    result = analyze_structure(candles, pivot_window=2)
    assert result["bias"] in {"BULL", "BEAR", "MIX"}
    assert result["bos"] in {"BULL", "BEAR", "NONE"}
    assert result["choch"] in {"BULL", "BEAR", "NONE"}
    assert result["pullback"] in {"LONG", "SHORT", "NONE"}


def test_structure_helper_functions() -> None:
    candles = _make_candles([1.00, 1.20, 1.40, 1.15, 1.35, 1.55, 1.30, 1.50, 1.70, 1.60, 1.65])
    swing_high = detect_swing_high(candles, left=2, right=2)
    swing_low = detect_swing_low(candles, left=2, right=2)
    assert swing_high is None or "price" in swing_high
    assert swing_low is None or "price" in swing_low
    assert isinstance(detect_bos_bullish(candles, pivot_window=2), bool)
    assert isinstance(detect_higher_low(candles, pivot_window=2), bool)
    assert isinstance(detect_lower_high(candles, pivot_window=2), bool)

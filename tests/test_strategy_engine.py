from app.models import AddZone, Candle, TradeSide
from app.strategy_engine import (
    analyze_adx_dmi,
    analyze_ema_signal,
    analyze_koncorde_lite,
    analyze_macd,
    analyze_sqzmom,
    approximate_pnl,
    calculate_distance_to_stop_loss,
    classify_bias,
    detect_structure,
    detect_touched_add_zone,
    evaluate_signal_quality,
    should_alert_break_even,
)


def test_calculate_distance_to_stop_loss_returns_pct() -> None:
    assert round(calculate_distance_to_stop_loss(100, 98), 2) == 2.00


def test_approximate_pnl_short_with_quantity() -> None:
    pnl = approximate_pnl(TradeSide.SHORT, entry_price=100, current_price=95, quantity=2)
    assert round(pnl.raw_pct, 2) == 5.00
    assert round(pnl.pnl_usdt or 0, 2) == 10.00


def test_should_alert_break_even_when_trade_revisits_entry() -> None:
    should_alert = should_alert_break_even(
        TradeSide.SHORT,
        entry_price=100,
        stop_loss=105,
        current_price=100.02,
        best_price=96,
        min_rr=0.5,
        entry_tolerance_pct=0.05,
    )
    assert should_alert is True


def test_detect_structure_bullish_and_bias() -> None:
    candles = [
        Candle(0, 0, 10, 7, 8, 1),
        Candle(1, 0, 12, 8, 9, 2),
        Candle(2, 0, 9, 6, 7, 3),
        Candle(3, 0, 13, 9, 12, 4),
        Candle(4, 0, 10, 7, 8, 5),
        Candle(5, 0, 14, 10, 13, 6),
        Candle(6, 0, 11, 8, 9, 7),
        Candle(7, 0, 15, 11, 14, 8),
        Candle(8, 0, 12, 9, 10, 9),
    ]
    structure = detect_structure(candles, pivot_length=1)
    assert structure.trend == "bullish"
    assert classify_bias(110, ema_fast=105, ema_slow=100) == "bullish"


def test_detect_touched_add_zone() -> None:
    zone = detect_touched_add_zone(
        previous_price=100.5,
        current_price=99.9,
        add_zones=[AddZone(price=100, size_usdt=10, note="test")],
        tolerance_pct=0.1,
    )
    assert zone is not None
    assert zone.price == 100


def test_analyze_ema_signal_uses_closed_candle_and_detects_bear_cross() -> None:
    closes = ([150.0] * 200) + [1.0, 9999.0]
    candles = [
        Candle(index, 0, close, close, close, index + 1)
        for index, close in enumerate(closes)
    ]
    result = analyze_ema_signal(candles)
    assert result["cross"] == "bear_cross"
    assert result["trend"] == "BEAR"


def test_analyze_koncorde_lite_detects_long_strong() -> None:
    closes = [100.0 + (index * 0.2) for index in range(30)] + [110.0, 9999.0]
    candles = []
    for index, close in enumerate(closes):
        volume = 100.0
        if index == len(closes) - 2:
            volume = 150.0
        candles.append(Candle(index, 0, close, close, close, index + 1, volume))

    result = analyze_koncorde_lite(candles)
    assert result["flow"] == "LONG_STRONG"
    assert result["icon"] == "🟢"


def test_evaluate_signal_quality_returns_long_when_sync_ok() -> None:
    closes = [100.0 + (index * 0.4) for index in range(210)] + [9999.0]
    candles = [
        Candle(index, 0, close, close + 0.2, close - 0.2, index + 1, 100.0)
        for index, close in enumerate(closes)
    ]
    candles[-2].volume = 150.0

    ema_result = analyze_ema_signal(candles)
    koncorde = analyze_koncorde_lite(candles)
    adx = analyze_adx_dmi(candles)
    macd = analyze_macd([candle.close for candle in candles[:-1]])
    sqz = analyze_sqzmom(candles)
    result = evaluate_signal_quality(
        tf_directions={"15m": "LONG", "5m": "LONG", "3m": "LONG", "1m": "LONG"},
        m15_ema=ema_result,
        koncorde_m15=koncorde,
        adx_m15=adx,
        macd_m15=macd,
        sqzmom_m15=sqz,
    )
    assert result["result"] == "LONG"
    assert result["score"] >= 11

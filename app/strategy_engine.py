from __future__ import annotations

from datetime import datetime, timezone
import math

from app.models import (
    AddZone,
    Bias,
    Candle,
    MarketContext,
    PnlEstimate,
    StructureState,
    TradeSide,
)


def ema(values: list[float], period: int) -> float | None:
    return calculate_ema(values, period)


def rsi(values: list[float], period: int = 14) -> float | None:
    if period <= 0:
        raise ValueError("period debe ser mayor que cero.")
    if len(values) < period + 1:
        return None

    gains: list[float] = []
    losses: list[float] = []
    for index in range(1, period + 1):
        change = values[index] - values[index - 1]
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    for index in range(period + 1, len(values)):
        change = values[index] - values[index - 1]
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        avg_gain = ((avg_gain * (period - 1)) + gain) / period
        avg_loss = ((avg_loss * (period - 1)) + loss) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def volume_ma(values: list[float], period: int = 20) -> float | None:
    if period <= 0:
        raise ValueError("period debe ser mayor que cero.")
    if len(values) < period:
        return None
    return sum(values[-period:]) / period


def _simple_mean(values: list[float]) -> float:
    return sum(values) / len(values)


def _simple_std(values: list[float]) -> float:
    mean = _simple_mean(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def calculate_distance_to_stop_loss(current_price: float, stop_loss: float) -> float:
    if current_price <= 0:
        raise ValueError("current_price debe ser mayor que cero.")
    return abs((stop_loss - current_price) / current_price) * 100


def calculate_take_profit_distances(current_price: float, take_profits: list[float]) -> list[float]:
    if current_price <= 0:
        raise ValueError("current_price debe ser mayor que cero.")
    return [abs((target - current_price) / current_price) * 100 for target in take_profits]


def is_price_in_invalidation_zone(current_price: float, zone_min: float, zone_max: float) -> bool:
    return zone_min <= current_price <= zone_max


def touched_price_level(
    previous_price: float | None,
    current_price: float,
    level: float,
    tolerance_pct: float = 0.0,
) -> bool:
    tolerance = abs(level) * (tolerance_pct / 100)
    if abs(current_price - level) <= tolerance:
        return True
    if previous_price is None:
        return False
    low = min(previous_price, current_price) - tolerance
    high = max(previous_price, current_price) + tolerance
    return low <= level <= high


def detect_touched_add_zone(
    previous_price: float | None,
    current_price: float,
    add_zones: list[AddZone],
    tolerance_pct: float,
) -> AddZone | None:
    for zone in add_zones:
        if touched_price_level(previous_price, current_price, zone.price, tolerance_pct):
            return zone
    return None


def has_price_reached_level(side: TradeSide, current_price: float, level: float) -> bool:
    if side == TradeSide.LONG:
        return current_price >= level
    return current_price <= level


def approximate_pnl(
    side: TradeSide,
    entry_price: float,
    current_price: float,
    quantity: float | None = None,
    leverage: int | None = None,
) -> PnlEstimate:
    if entry_price <= 0:
        raise ValueError("entry_price debe ser mayor que cero.")

    if side == TradeSide.LONG:
        raw_pct = ((current_price - entry_price) / entry_price) * 100
        pnl_per_unit = current_price - entry_price
    else:
        raw_pct = ((entry_price - current_price) / entry_price) * 100
        pnl_per_unit = entry_price - current_price

    leveraged_pct = raw_pct * leverage if leverage else raw_pct
    pnl_usdt = pnl_per_unit * abs(quantity) if quantity is not None else None
    return PnlEstimate(raw_pct=raw_pct, leveraged_pct=leveraged_pct, pnl_usdt=pnl_usdt)


def should_alert_break_even(
    side: TradeSide,
    entry_price: float,
    stop_loss: float,
    current_price: float,
    best_price: float | None,
    min_rr: float,
    entry_tolerance_pct: float,
) -> bool:
    if best_price is None:
        return False

    risk = abs(entry_price - stop_loss)
    if risk == 0:
        return False

    favourable_move = (
        best_price - entry_price if side == TradeSide.LONG else entry_price - best_price
    )
    if favourable_move < risk * min_rr:
        return False

    tolerance = entry_price * (entry_tolerance_pct / 100)
    return abs(current_price - entry_price) <= tolerance


def calculate_ema(values: list[float], period: int) -> float | None:
    if period <= 0:
        raise ValueError("period debe ser mayor que cero.")
    if len(values) < period:
        return None

    multiplier = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for value in values[period:]:
        ema = (value - ema) * multiplier + ema
    return ema


def detect_structure(candles: list[Candle], pivot_length: int) -> StructureState:
    if pivot_length <= 0:
        raise ValueError("pivot_length debe ser mayor que cero.")
    if len(candles) < (pivot_length * 2) + 1:
        return StructureState(trend="neutral")

    pivot_highs: list[tuple[int, float]] = []
    pivot_lows: list[tuple[int, float]] = []

    for index in range(pivot_length, len(candles) - pivot_length):
        current = candles[index]
        left = candles[index - pivot_length : index]
        right = candles[index + 1 : index + 1 + pivot_length]

        if all(current.high > candle.high for candle in left + right):
            pivot_highs.append((index, current.high))
        if all(current.low < candle.low for candle in left + right):
            pivot_lows.append((index, current.low))

    trend = "neutral"
    if len(pivot_highs) >= 2 and len(pivot_lows) >= 2:
        last_two_highs = pivot_highs[-2:]
        last_two_lows = pivot_lows[-2:]
        if (
            last_two_highs[1][1] > last_two_highs[0][1]
            and last_two_lows[1][1] > last_two_lows[0][1]
        ):
            trend = "bullish"
        elif (
            last_two_highs[1][1] < last_two_highs[0][1]
            and last_two_lows[1][1] < last_two_lows[0][1]
        ):
            trend = "bearish"

    return StructureState(
        trend=trend,
        last_swing_high=pivot_highs[-1][1] if pivot_highs else None,
        last_swing_low=pivot_lows[-1][1] if pivot_lows else None,
        pivot_highs=pivot_highs,
        pivot_lows=pivot_lows,
    )


def classify_bias(price: float, ema_fast: float | None, ema_slow: float | None) -> Bias:
    if ema_fast is None or ema_slow is None:
        return Bias.NEUTRAL
    if price < ema_slow and ema_fast < ema_slow:
        return Bias.BEARISH
    if price > ema_slow and ema_fast > ema_slow:
        return Bias.BULLISH
    return Bias.NEUTRAL


def build_market_context(
    symbol: str,
    candles: list[Candle],
    current_price: float,
    ema_fast_period: int,
    ema_slow_period: int,
    pivot_length: int,
) -> MarketContext:
    closes = [candle.close for candle in candles]
    ema_fast = calculate_ema(closes, ema_fast_period)
    ema_slow = calculate_ema(closes, ema_slow_period)
    structure = detect_structure(candles, pivot_length)
    return MarketContext(
        symbol=symbol,
        ema_fast=ema_fast,
        ema_slow=ema_slow,
        bias=classify_bias(current_price, ema_fast, ema_slow),
        structure=structure.trend,
        refreshed_at=datetime.now(tz=timezone.utc),
    )


def analyze_ema_signal(
    candles: list[Candle],
    *,
    fast_period: int = 55,
    slow_period: int = 200,
) -> dict[str, float | str | None]:
    if len(candles) < slow_period + 2:
        raise ValueError("No hay suficientes velas para EMA200 cerrada.")

    closed_now = candles[:-1]
    closed_prev = candles[:-2]
    if len(closed_now) < slow_period or len(closed_prev) < slow_period:
        raise ValueError("No hay suficientes velas cerradas para calcular cruces.")

    closes_now = [candle.close for candle in closed_now]
    closes_prev = [candle.close for candle in closed_prev]

    ema55_prev = calculate_ema(closes_prev, fast_period)
    ema200_prev = calculate_ema(closes_prev, slow_period)
    ema55_now = calculate_ema(closes_now, fast_period)
    ema200_now = calculate_ema(closes_now, slow_period)
    close_now = closed_now[-1].close

    if None in {ema55_prev, ema200_prev, ema55_now, ema200_now}:
        raise ValueError("No se pudieron calcular EMA55/EMA200.")

    cross = "none"
    if ema55_prev <= ema200_prev and ema55_now > ema200_now:
        cross = "bull_cross"
    elif ema55_prev >= ema200_prev and ema55_now < ema200_now:
        cross = "bear_cross"

    if close_now > ema200_now and ema55_now > ema200_now:
        trend = "BULL"
        icon = "🟢"
        relation = "close>EMA200 | EMA55>EMA200"
    elif close_now < ema200_now and ema55_now < ema200_now:
        trend = "BEAR"
        icon = "🔴"
        relation = "close<EMA200 | EMA55<EMA200"
    else:
        trend = "MIX"
        icon = "🟡"
        relation = "mixto"

    return {
        "close": close_now,
        "ema55": ema55_now,
        "ema200": ema200_now,
        "ema55_prev": ema55_prev,
        "ema200_prev": ema200_prev,
        "cross": cross,
        "trend": trend,
        "icon": icon,
        "relation": relation,
    }


def analyze_koncorde_lite(candles: list[Candle]) -> dict[str, float | str]:
    if len(candles) < 22:
        raise ValueError("No hay suficientes velas cerradas para Koncorde Lite M15.")

    closed = candles[:-1]
    if len(closed) < 21:
        raise ValueError("No hay suficientes velas cerradas para Koncorde Lite M15.")

    close_values = [candle.close for candle in closed]
    current_candle = closed[-1]
    previous_candle = closed[-2]
    historical_volumes = [candle.volume for candle in closed[:-1]]

    current_rsi = rsi(close_values, period=14)
    current_volume_ma20 = volume_ma(historical_volumes, period=20)
    if current_rsi is None or current_volume_ma20 is None or current_volume_ma20 == 0:
        raise ValueError("No hay suficientes datos para RSI14/volume MA20 en M15.")

    volume_ratio = current_candle.volume / current_volume_ma20
    close_up = current_candle.close > previous_candle.close
    close_down = current_candle.close < previous_candle.close

    flow = "NEUTRAL"
    text = "flujo neutral"
    icon = "🟡"

    if current_rsi > 55 and volume_ratio >= 1.2 and close_up:
        flow = "LONG_STRONG"
        text = "flujo LONG fuerte"
        icon = "🟢"
    elif current_rsi > 50 and current_candle.volume >= current_volume_ma20 and close_up:
        flow = "LONG_OK"
        text = "flujo LONG ok"
        icon = "🟢"
    elif current_rsi < 45 and volume_ratio >= 1.2 and close_down:
        flow = "SHORT_STRONG"
        text = "flujo SHORT fuerte"
        icon = "🔴"
    elif current_rsi < 50 and current_candle.volume >= current_volume_ma20 and close_down:
        flow = "SHORT_OK"
        text = "flujo SHORT ok"
        icon = "🔴"

    return {
        "flow": flow,
        "text": text,
        "icon": icon,
        "rsi": current_rsi,
        "volume_ratio": volume_ratio,
        "close_up": close_up,
        "close_down": close_down,
    }


def analyze_adx_dmi(candles: list[Candle], period: int = 14) -> dict[str, float]:
    closed = candles[:-1]
    if len(closed) < period + 2:
        raise ValueError("No hay suficientes velas cerradas para ADX/DMI.")

    tr_values: list[float] = []
    plus_dm_values: list[float] = []
    minus_dm_values: list[float] = []

    for index in range(1, len(closed)):
        current = closed[index]
        previous = closed[index - 1]
        up_move = current.high - previous.high
        down_move = previous.low - current.low
        plus_dm = up_move if up_move > down_move and up_move > 0 else 0.0
        minus_dm = down_move if down_move > up_move and down_move > 0 else 0.0
        true_range = max(
            current.high - current.low,
            abs(current.high - previous.close),
            abs(current.low - previous.close),
        )
        tr_values.append(true_range)
        plus_dm_values.append(plus_dm)
        minus_dm_values.append(minus_dm)

    if len(tr_values) < period:
        raise ValueError("No hay suficientes datos para ADX/DMI.")

    smoothed_tr = sum(tr_values[:period])
    smoothed_plus_dm = sum(plus_dm_values[:period])
    smoothed_minus_dm = sum(minus_dm_values[:period])

    plus_di = 100 * (smoothed_plus_dm / smoothed_tr) if smoothed_tr else 0.0
    minus_di = 100 * (smoothed_minus_dm / smoothed_tr) if smoothed_tr else 0.0
    denominator = plus_di + minus_di
    dx_values = [100 * abs(plus_di - minus_di) / denominator if denominator else 0.0]

    for index in range(period, len(tr_values)):
        smoothed_tr = smoothed_tr - (smoothed_tr / period) + tr_values[index]
        smoothed_plus_dm = smoothed_plus_dm - (smoothed_plus_dm / period) + plus_dm_values[index]
        smoothed_minus_dm = smoothed_minus_dm - (smoothed_minus_dm / period) + minus_dm_values[index]
        plus_di = 100 * (smoothed_plus_dm / smoothed_tr) if smoothed_tr else 0.0
        minus_di = 100 * (smoothed_minus_dm / smoothed_tr) if smoothed_tr else 0.0
        denominator = plus_di + minus_di
        dx_values.append(100 * abs(plus_di - minus_di) / denominator if denominator else 0.0)

    adx = _simple_mean(dx_values[:period]) if len(dx_values) >= period else _simple_mean(dx_values)
    for dx_value in dx_values[period:]:
        adx = ((adx * (period - 1)) + dx_value) / period

    return {"adx": adx, "plus_di": plus_di, "minus_di": minus_di}


def _ema_series(values: list[float], period: int) -> list[float]:
    if period <= 0:
        raise ValueError("period debe ser mayor que cero.")
    if len(values) < period:
        return []
    multiplier = 2 / (period + 1)
    ema_value = sum(values[:period]) / period
    series = [ema_value]
    for value in values[period:]:
        ema_value = ((value - ema_value) * multiplier) + ema_value
        series.append(ema_value)
    return series


def analyze_macd(values: list[float], fast: int = 12, slow: int = 26, signal: int = 9) -> dict[str, float]:
    fast_series = _ema_series(values, fast)
    slow_series = _ema_series(values, slow)
    if not fast_series or not slow_series:
        raise ValueError("No hay suficientes datos para MACD.")

    aligned_fast = fast_series[-len(slow_series) :]
    macd_series = [fast_value - slow_value for fast_value, slow_value in zip(aligned_fast, slow_series)]
    signal_series = _ema_series(macd_series, signal)
    if not signal_series:
        raise ValueError("No hay suficientes datos para señal MACD.")

    macd_now = macd_series[-1]
    signal_now = signal_series[-1]
    histogram_now = macd_now - signal_now
    return {"macd": macd_now, "signal": signal_now, "histogram": histogram_now}


def analyze_sqzmom(candles: list[Candle], period: int = 20) -> dict[str, float | bool]:
    closed = candles[:-1]
    if len(closed) < period + 2:
        raise ValueError("No hay suficientes velas cerradas para SQZMOM.")

    current_window = closed[-period:]
    previous_window = closed[-period - 1 : -1]
    closes_now = [candle.close for candle in current_window]
    closes_prev = [candle.close for candle in previous_window]
    highs_now = [candle.high for candle in current_window]
    highs_prev = [candle.high for candle in previous_window]
    lows_now = [candle.low for candle in current_window]
    lows_prev = [candle.low for candle in previous_window]

    sma_now = _simple_mean(closes_now)
    sma_prev = _simple_mean(closes_prev)
    bb_now = _simple_std(closes_now) * 2.0
    bb_prev = _simple_std(closes_prev) * 2.0

    tr_now = []
    tr_prev = []
    for index in range(1, len(current_window)):
        current = current_window[index]
        previous = current_window[index - 1]
        tr_now.append(max(current.high - current.low, abs(current.high - previous.close), abs(current.low - previous.close)))
    for index in range(1, len(previous_window)):
        current = previous_window[index]
        previous = previous_window[index - 1]
        tr_prev.append(max(current.high - current.low, abs(current.high - previous.close), abs(current.low - previous.close)))

    atr_now = _simple_mean(tr_now)
    atr_prev = _simple_mean(tr_prev)
    kc_now = atr_now * 1.5
    kc_prev = atr_prev * 1.5

    momentum_now = closes_now[-1] - (((max(highs_now) + min(lows_now)) / 2 + sma_now) / 2)
    momentum_prev = closes_prev[-1] - (((max(highs_prev) + min(lows_prev)) / 2 + sma_prev) / 2)

    squeeze_on = (sma_now + bb_now) < (sma_now + kc_now) and (sma_now - bb_now) > (sma_now - kc_now)
    return {
        "momentum": momentum_now,
        "previous_momentum": momentum_prev,
        "squeeze_on": squeeze_on,
        "strong_bull": momentum_now > 0 and abs(momentum_now) >= abs(momentum_prev),
        "strong_bear": momentum_now < 0 and abs(momentum_now) >= abs(momentum_prev),
    }


def evaluate_signal_quality(
    *,
    tf_directions: dict[str, str],
    m15_ema: dict[str, float | str | None],
    koncorde_m15: dict[str, float | str],
    adx_m15: dict[str, float],
    macd_m15: dict[str, float],
    sqzmom_m15: dict[str, float | bool],
) -> dict[str, int | str]:
    long_valid = (
        tf_directions["15m"] == "LONG"
        and tf_directions["5m"] == "LONG"
        and tf_directions["3m"] == "LONG"
    )
    short_valid = (
        tf_directions["15m"] == "SHORT"
        and tf_directions["5m"] == "SHORT"
        and tf_directions["3m"] == "SHORT"
    )

    if not long_valid and not short_valid:
        return {"result": "SIN_SEÑAL", "score": 0, "quality": "BAJA"}

    direction = "LONG" if long_valid else "SHORT"
    score = 7
    if tf_directions["1m"] == direction:
        score += 1

    adx_value = adx_m15["adx"]
    if adx_value >= 20:
        score += 1
    if adx_value >= 25:
        score += 1

    if direction == "LONG":
        if macd_m15["histogram"] > 0:
            score += 1
        if sqzmom_m15["strong_bull"]:
            score += 1
        if m15_ema["close"] > m15_ema["ema200"]:
            score += 2
        if m15_ema["close"] > m15_ema["ema55"]:
            score += 1
        if m15_ema["ema55"] > m15_ema["ema200"]:
            score += 2
        if m15_ema["cross"] == "bull_cross":
            score += 1
        elif m15_ema["cross"] == "bear_cross":
            score -= 1
        if koncorde_m15["rsi"] > 50:
            score += 1
        if koncorde_m15["volume_ratio"] >= 1.0:
            score += 1
        if koncorde_m15["volume_ratio"] >= 1.2:
            score += 1
        if koncorde_m15["close_up"]:
            score += 1
        if str(koncorde_m15["flow"]).startswith("SHORT"):
            score -= 2
    else:
        if macd_m15["histogram"] < 0:
            score += 1
        if sqzmom_m15["strong_bear"]:
            score += 1
        if m15_ema["close"] < m15_ema["ema200"]:
            score += 2
        if m15_ema["close"] < m15_ema["ema55"]:
            score += 1
        if m15_ema["ema55"] < m15_ema["ema200"]:
            score += 2
        if m15_ema["cross"] == "bear_cross":
            score += 1
        elif m15_ema["cross"] == "bull_cross":
            score -= 1
        if koncorde_m15["rsi"] < 50:
            score += 1
        if koncorde_m15["volume_ratio"] >= 1.0:
            score += 1
        if koncorde_m15["volume_ratio"] >= 1.2:
            score += 1
        if koncorde_m15["close_down"]:
            score += 1
        if str(koncorde_m15["flow"]).startswith("LONG"):
            score -= 2

    if score <= 6:
        quality = "BAJA"
    elif score <= 10:
        quality = "MEDIA"
    elif score <= 14:
        quality = "ALTA"
    else:
        quality = "MUY_ALTA"

    return {"result": direction, "score": score, "quality": quality}

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


def analyze_koncorde_lite(candles: list[Candle]) -> dict[str, float | str | bool | None]:
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
    previous_rsi = rsi(close_values[:-1], period=14) if len(close_values) >= 15 else None
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
        "rsi_prev": previous_rsi,
        "volume": current_candle.volume,
        "volume_ma20": current_volume_ma20,
        "open": current_candle.open,
        "high": current_candle.high,
        "low": current_candle.low,
        "close": current_candle.close,
        "close_prev": previous_candle.close,
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

    adx_series: list[float] = []
    adx = _simple_mean(dx_values[:period]) if len(dx_values) >= period else _simple_mean(dx_values)
    adx_series.append(adx)
    for dx_value in dx_values[period:]:
        adx = ((adx * (period - 1)) + dx_value) / period
        adx_series.append(adx)

    adx_prev = adx_series[-2] if len(adx_series) >= 2 else adx_series[-1]
    return {"adx": adx, "adx_prev": adx_prev, "plus_di": plus_di, "minus_di": minus_di}


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


def _quality_from_score(score: int) -> str:
    if score <= 6:
        return "BAJA"
    if score <= 10:
        return "MEDIA"
    if score <= 14:
        return "ALTA"
    return "MUY_ALTA"


def explain_adx_dmi(
    *,
    adx: float,
    adx_prev: float,
    plus_di: float,
    minus_di: float,
    signal_direction: str,
    sqz_direction: str,
    macd_direction: str,
    ema_trend: str,
    structure_bias: str,
    bos: str,
    choch: str,
    pullback: str,
    chase_risk: str,
) -> dict[str, str | int | float | bool]:
    slope = "↗" if adx > adx_prev else ("↘" if adx < adx_prev else "→")
    dmi_direction = "NEUTRAL"
    if plus_di > minus_di:
        dmi_direction = "LONG"
    elif minus_di > plus_di:
        dmi_direction = "SHORT"

    state = "ADX_FAVOR_OK"
    status = "🟡 Tendencia válida débil"
    reading = "ADX sostiene una dirección, pero con fuerza moderada."
    impact = 0
    filtro = "neutral"

    if adx < 20:
        state = "ADX_WEAK"
        status = "⚪ Tendencia débil/rango"
        reading = "ADX bajo. Riesgo de falso movimiento/lateralización."
        impact = -1
        filtro = "ruido"
    elif dmi_direction not in {"NEUTRAL", signal_direction}:
        state = "ADX_CONTRARY"
        status = "⚠️ DMI contrario"
        reading = "DMI contradice la dirección principal. Precaución por ruido o giro."
        impact = -2
        filtro = "ruido"
    elif adx >= 45:
        state = "ADX_EXTENDED"
        status = "⚠️ Tendencia extendida"
        reading = "Hay fuerza, pero puede estar tarde. Mejor pullback/pivote."
        if pullback == signal_direction:
            impact = 1
            filtro = "favorable"
        else:
            impact = -1
            filtro = "ruido"
        if chase_risk == "high":
            reading += " Riesgo alto por chase."
    elif adx >= 25 and dmi_direction == signal_direction and slope == "↗":
        state = "ADX_FAVOR_STRONG"
        status = "🟢 Tendencia alcista fuerte" if signal_direction == "LONG" else "🔴 Tendencia bajista fuerte"
        reading = (
            "ADX confirma fuerza y +DI domina. Presión direccional acompaña LONG."
            if signal_direction == "LONG"
            else "ADX confirma fuerza y -DI domina. Presión direccional acompaña SHORT."
        )
        impact = 2
        filtro = "favorable"
    elif adx >= 22 and dmi_direction == signal_direction:
        state = "ADX_FAVOR_OK"
        status = "🟢 Tendencia alcista moderada" if signal_direction == "LONG" else "🔴 Tendencia bajista moderada"
        reading = "DMI acompaña y ADX valida dirección con fuerza inicial."
        impact = 1
        filtro = "favorable"
    elif adx < 18:
        status = "⚪ Tendencia débil/rango"
        reading = "Mercado en rango. ADX no confirma presión direccional."

    return {
        "state": state,
        "status": status,
        "reading": reading,
        "impact_score": impact,
        "filtro": filtro,
        "data": f"ADX {adx:.1f} {slope} | +DI {plus_di:.1f} | -DI {minus_di:.1f}",
        "slope": slope,
        "dmi_direction": dmi_direction,
        "is_weak": state == "ADX_WEAK",
        "is_contrary": state == "ADX_CONTRARY",
        "is_extended": state == "ADX_EXTENDED",
    }


def explain_koncorde_lite(
    *,
    rsi: float,
    rsi_prev: float | None,
    volume: float,
    volume_ma20: float,
    vol_ratio: float,
    open_price: float,
    high: float,
    low: float,
    close: float,
    close_prev: float,
    signal_direction: str,
    structure_bias: str,
    pullback: str,
    chase_risk: str,
) -> dict[str, str | int | float | bool]:
    candle_up = close > close_prev and close > open_price
    candle_down = close < close_prev and close < open_price
    candle_bias = "alcista" if close > open_price else "bajista"

    body = abs(close - open_price)
    spread = max(1e-9, high - low)
    body_ratio = body / spread

    flow_direction = "NEUTRAL"
    state = "NEUTRAL"
    status = "🟡 Flujo neutral"
    reading = "KoncordeLite no confirma dirección."
    impact = 0
    filtro = "neutral"

    if rsi > 55 and vol_ratio >= 1.2 and close > close_prev and close > open_price:
        state = "LONG_STRONG"
        flow_direction = "LONG"
        status = "🟢 Flujo comprador fuerte"
        reading = "Volumen y RSI acompañan subida. Presión compradora real."
        impact = 3
        filtro = "favorable"
    elif rsi > 50 and vol_ratio >= 1.0 and close > close_prev:
        state = "LONG_OK"
        flow_direction = "LONG"
        status = "🟢 Flujo comprador moderado"
        reading = "Flujo acompaña LONG, pero sin fuerza extrema."
        impact = 1
        filtro = "favorable"
    elif rsi < 45 and vol_ratio >= 1.2 and close < close_prev and close < open_price:
        state = "SHORT_STRONG"
        flow_direction = "SHORT"
        status = "🔴 Flujo vendedor fuerte"
        reading = "Volumen y RSI acompañan caída. Presión vendedora real."
        impact = 3
        filtro = "favorable"
    elif rsi < 50 and vol_ratio >= 1.0 and close < close_prev:
        state = "SHORT_OK"
        flow_direction = "SHORT"
        status = "🔴 Flujo vendedor moderado"
        reading = "Flujo acompaña SHORT, pero sin fuerza extrema."
        impact = 1
        filtro = "favorable"

    if vol_ratio >= 1.3 and (body_ratio <= 0.25 or abs(close - close_prev) <= (spread * 0.15)):
        state = "ABSORPTION"
        flow_direction = "NEUTRAL"
        status = "🟣 Posible absorción"
        reading = "Volumen alto, poco avance. No perseguir sin confirmación."
        impact = -1 if chase_risk == "high" else 0
        filtro = "neutral" if impact == 0 else "ruido"

    if vol_ratio < 0.8:
        state = "DRY_VOLUME"
        flow_direction = "NEUTRAL"
        status = "🧊 Volumen seco"
        reading = "Momentum con baja participación. Señal menos confiable."
        impact = -1
        filtro = "ruido"

    if flow_direction in {"LONG", "SHORT"} and flow_direction != signal_direction:
        state = "CONTRARY"
        status = "⚠️ Flujo contrario"
        reading = "Momentum y flujo M15 no coinciden. Riesgo ruido."
        impact = -2
        filtro = "ruido"

    return {
        "state": state,
        "status": status,
        "reading": reading,
        "impact_score": impact,
        "filtro": filtro,
        "flow_direction": flow_direction,
        "data": f"RSI {rsi:.1f} | Vol {vol_ratio:.2f}x | cierre {candle_bias}",
        "is_contrary": state == "CONTRARY",
    }


def build_human_market_report(
    *,
    signal_direction: str,
    quality: str,
    sync_label: str,
    ema_trend: str,
    structure_bias: str,
    bos: str,
    choch: str,
    pullback: str,
    structure_notes: str,
    chase_risk: str,
    adx_human: dict[str, str | int | float | bool],
    koncorde_human: dict[str, str | int | float | bool],
) -> list[str]:
    lines: list[str] = []
    strong_setup = (
        signal_direction in {"LONG", "SHORT"}
        and quality in {"ALTA", "MUY_ALTA"}
        and structure_bias == ("BULL" if signal_direction == "LONG" else "BEAR")
        and bos == ("BULL" if signal_direction == "LONG" else "BEAR")
        and pullback == signal_direction
    )

    if strong_setup:
        if signal_direction == "LONG":
            lines.append("Presión alcista alineada con estructura M15.")
            lines.append("El último high es mayor y el pullback evita perseguir precio.")
        else:
            lines.append("Presión bajista alineada con estructura M15.")
            lines.append("El último low es menor y el pullback evita perseguir precio.")
        lines.append("ADX/DMI confirma fuerza; flujo M15 acompaña si Koncorde favorable.")

    if chase_risk == "high":
        lines.append("Hay momentum, pero riesgo de perseguir movimiento. Mejor esperar pullback.")
    if pullback not in {"LONG", "SHORT"}:
        lines.append("Sin pullback claro. Señal válida, pero entrada menos eficiente.")
    if choch == ("BEAR" if signal_direction == "LONG" else "BULL"):
        lines.append("CHoCH contra señal. Riesgo de cambio estructural.")
    if bool(koncorde_human.get("is_contrary", False)):
        lines.append("Flujo M15 contradice momentum. Bajar confianza.")
    if bool(adx_human.get("is_weak", False)):
        lines.append("ADX débil. Riesgo de rango/falso breakout.")

    ema_against = (signal_direction == "LONG" and ema_trend == "BEAR") or (signal_direction == "SHORT" and ema_trend == "BULL")
    if ema_against:
        lines.append("Momentum va contra EMA200 M15. Puede ser movimiento correctivo.")

    if not lines:
        lines.append("Lectura mixta: momentum válido con filtros sin consenso pleno.")
        if structure_notes:
            lines.append(f"Detalle estructura: {structure_notes}")

    return lines[:4]


def evaluate_signal_quality(
    *,
    tf_directions: dict[str, str],
    m15_ema: dict[str, float | str | None],
    koncorde_m15: dict[str, float | str | bool | None],
    adx_m15: dict[str, float],
    macd_m15: dict[str, float],
    sqzmom_m15: dict[str, float | bool],
    structure_by_tf: dict[str, dict[str, str | bool | float | None]] | None = None,
) -> dict[str, int | str | bool | list[str] | dict[str, str | int | float | bool]]:
    sync_m15 = tf_directions.get("15m")
    sync_m5 = tf_directions.get("5m")
    sync_m3 = tf_directions.get("3m")
    sync_m1 = tf_directions.get("1m")

    if sync_m15 not in {"LONG", "SHORT"}:
        return {
            "result": "SIN_SEÑAL",
            "signal_type": "SIN_SEÑAL",
            "quality": "BAJA",
            "raw_quality": "BAJA",
            "final_quality": "BAJA",
            "score": 0,
            "score_total": 0,
            "score_items": [],
            "penalties": [],
            "blockers": ["M15 no alineada"],
            "degraders": [],
            "no_signal_reason": "M15 no está alineada con dirección operativa.",
            "alert_allowed": False,
        }

    long_valid = sync_m15 == "LONG" and sync_m5 == "LONG" and sync_m3 == "LONG"
    short_valid = sync_m15 == "SHORT" and sync_m5 == "SHORT" and sync_m3 == "SHORT"
    if not long_valid and not short_valid:
        return {
            "result": "SIN_SEÑAL",
            "signal_type": "SIN_SEÑAL",
            "quality": "BAJA",
            "raw_quality": "BAJA",
            "final_quality": "BAJA",
            "score": 0,
            "score_total": 0,
            "score_items": [],
            "penalties": [],
            "blockers": ["Falta sync M15+M5+M3"],
            "degraders": [],
            "no_signal_reason": "No hay sincronía base entre M15, M5 y M3.",
            "alert_allowed": False,
        }

    direction = "LONG" if long_valid else "SHORT"
    expected_bias = "BULL" if direction == "LONG" else "BEAR"
    opposite_bias = "BEAR" if direction == "LONG" else "BULL"

    m15_structure = (structure_by_tf or {}).get("15m", {})
    m5_structure = (structure_by_tf or {}).get("5m", {})
    m3_structure = (structure_by_tf or {}).get("3m", {})

    chase_risk = "high" if (structure_by_tf and m15_structure.get("pullback") != direction) else "low"

    score_groups = {
        "momentum": 0,
        "fuerza": 0,
        "tendencia": 0,
        "estructura": 0,
        "flujo": 0,
        "timing": 0,
    }
    group_caps = {"momentum": 4, "fuerza": 3, "tendencia": 4, "estructura": 6, "flujo": 3, "timing": 1}
    score_items: list[str] = []
    penalties: list[str] = []
    blockers: list[str] = []
    degraders: list[str] = []

    # Sync score base
    score_groups["estructura"] += 3
    score_items.append("+3 M15 sync")
    score_groups["estructura"] += 2
    score_items.append("+2 M5 sync")
    score_groups["estructura"] += 2
    score_items.append("+2 M3 sync")

    # Momentum group
    sqz_direction = "LONG" if bool(sqzmom_m15.get("strong_bull")) else ("SHORT" if bool(sqzmom_m15.get("strong_bear")) else "NEUTRAL")
    macd_direction = "LONG" if macd_m15["histogram"] > 0 else ("SHORT" if macd_m15["histogram"] < 0 else "NEUTRAL")
    if sqz_direction == direction:
        score_groups["momentum"] += 2
        score_items.append("+2 SQZ favor")
    if macd_direction == direction:
        score_groups["momentum"] += 2
        score_items.append("+2 MACD favor")

    # Trend group
    ema_trend = "BULL" if m15_ema["trend"] == "BULL" else ("BEAR" if m15_ema["trend"] == "BEAR" else "MIX")
    if ema_trend == expected_bias:
        score_groups["tendencia"] += 3
        score_items.append("+3 EMA200 favor")
    else:
        penalties.append("-1 EMA200 contra")
        score_groups["tendencia"] -= 1
        degraders.append("EMA200 contra")

    if sync_m1 == direction:
        score_groups["timing"] += 1
        score_items.append("+1 M1 timing")
    elif sync_m1 not in {"NONE", direction}:
        penalties.append("-1 M1 contrario fuerte")
        score_groups["timing"] -= 1
        degraders.append("M1 contrario fuerte")

    # Structure group
    if m15_structure.get("bos") == expected_bias:
        score_groups["estructura"] += 2
        score_items.append(f"+2 BOS {expected_bias}")
    else:
        degraders.append("BOS débil")

    if m15_structure.get("pullback") == direction:
        score_groups["estructura"] += 2
        score_items.append("+2 pullback favor")
        if not bool(m15_structure.get("pullback_clean", False)):
            penalties.append("-1 pullback no limpio")
            score_groups["estructura"] -= 1
            degraders.append("pullback no limpio")
    else:
        penalties.append("-1 no pullback")
        score_groups["estructura"] -= 1
        degraders.append("no pullback")

    if m15_structure.get("bias") == expected_bias:
        score_groups["estructura"] += 2
        score_items.append("+2 estructura M15 favor")
    elif m15_structure.get("bias") == opposite_bias:
        penalties.append("-2 estructura M15 contra")
        score_groups["estructura"] -= 2
        blockers.append("estructura M15 contraria fuerte")

    if m5_structure.get("bias") == expected_bias:
        score_groups["estructura"] += 1
        score_items.append("+1 estructura M5 favor")
    if m3_structure.get("bias") == expected_bias:
        score_groups["estructura"] += 1
        score_items.append("+1 estructura M3 favor")

    if m15_structure.get("choch") == opposite_bias:
        penalties.append("-3 CHoCH contra fuerte")
        score_groups["estructura"] -= 3
        blockers.append("CHoCH fuerte contra señal")

    # ADX/Koncorde explainers
    adx_human = explain_adx_dmi(
        adx=float(adx_m15["adx"]),
        adx_prev=float(adx_m15.get("adx_prev", adx_m15["adx"])),
        plus_di=float(adx_m15["plus_di"]),
        minus_di=float(adx_m15["minus_di"]),
        signal_direction=direction,
        sqz_direction=sqz_direction,
        macd_direction=macd_direction,
        ema_trend=ema_trend,
        structure_bias=str(m15_structure.get("bias", "MIX")),
        bos=str(m15_structure.get("bos", "NONE")),
        choch=str(m15_structure.get("choch", "NONE")),
        pullback=str(m15_structure.get("pullback", "NONE")),
        chase_risk=chase_risk,
    )
    adx_impact = int(adx_human["impact_score"])
    score_groups["fuerza"] += adx_impact
    if adx_impact > 0:
        score_items.append(f"+{adx_impact} ADX favor")
    elif adx_impact < 0:
        penalties.append(f"{adx_impact} ADX")
    if bool(adx_human.get("is_weak", False)):
        degraders.append("ADX weak")

    koncorde_human = explain_koncorde_lite(
        rsi=float(koncorde_m15["rsi"]),
        rsi_prev=(float(koncorde_m15["rsi_prev"]) if koncorde_m15.get("rsi_prev") is not None else None),
        volume=float(koncorde_m15["volume"]),
        volume_ma20=float(koncorde_m15["volume_ma20"]),
        vol_ratio=float(koncorde_m15["volume_ratio"]),
        open_price=float(koncorde_m15["open"]),
        high=float(koncorde_m15["high"]),
        low=float(koncorde_m15["low"]),
        close=float(koncorde_m15["close"]),
        close_prev=float(koncorde_m15["close_prev"]),
        signal_direction=direction,
        structure_bias=str(m15_structure.get("bias", "MIX")),
        pullback=str(m15_structure.get("pullback", "NONE")),
        chase_risk=chase_risk,
    )
    kon_impact = int(koncorde_human["impact_score"])
    score_groups["flujo"] += kon_impact
    if kon_impact > 0:
        score_items.append(f"+{kon_impact} Koncorde favor")
    elif kon_impact < 0:
        penalties.append(f"{kon_impact} Koncorde")
    if bool(koncorde_human.get("is_contrary", False)):
        degraders.append("Koncorde contrary")

    if chase_risk == "high":
        penalties.append("-1 chase risk")
        score_groups["timing"] -= 1
        degraders.append("chase_risk true")

    # Cap by group
    capped_total = 0
    for group, value in score_groups.items():
        if value > group_caps[group]:
            value = group_caps[group]
        if value < -group_caps[group]:
            value = -group_caps[group]
        score_groups[group] = value
        capped_total += value

    raw_quality = _quality_from_score(capped_total)
    final_quality = raw_quality
    if "ADX weak" in degraders and capped_total <= 10:
        final_quality = {"MUY_ALTA": "ALTA", "ALTA": "MEDIA", "MEDIA": "BAJA", "BAJA": "BAJA"}[final_quality]
    if "Koncorde contrary" in degraders:
        final_quality = {"MUY_ALTA": "ALTA", "ALTA": "MEDIA", "MEDIA": "BAJA", "BAJA": "BAJA"}[final_quality]
    if "CHoCH fuerte contra señal" in blockers:
        final_quality = "BAJA"

    # signal_type
    pullback_ok = m15_structure.get("pullback") == direction and m15_structure.get("bias") == expected_bias
    continuation_ok = m15_structure.get("bias") == expected_bias and ema_trend == expected_bias and m15_structure.get("choch") != opposite_bias
    reversal_ok = m15_structure.get("bos") == expected_bias and ema_trend != expected_bias

    signal_type = f"{direction}_CONTINUATION"
    if pullback_ok:
        signal_type = f"{direction}_PULLBACK"
    elif continuation_ok:
        signal_type = f"{direction}_CONTINUATION"
    elif reversal_ok:
        signal_type = f"{direction}_REVERSAL_EARLY"
    elif chase_risk == "high" and not pullback_ok and capped_total >= 8:
        signal_type = "MOMENTUM_CHASE"
    if blockers:
        signal_type = "CONFLICT"

    no_signal_reason = ""
    if blockers:
        no_signal_reason = "; ".join(blockers)

    alert_allowed = not blockers and signal_type not in {"SIN_SEÑAL", "CONFLICT"}

    human_report = build_human_market_report(
        signal_direction=direction,
        quality=final_quality,
        sync_label="4/4" if sync_m1 == direction else "3/4",
        ema_trend=ema_trend,
        structure_bias=str(m15_structure.get("bias", "MIX")),
        bos=str(m15_structure.get("bos", "NONE")),
        choch=str(m15_structure.get("choch", "NONE")),
        pullback=str(m15_structure.get("pullback", "NONE")),
        structure_notes=" | ".join(score_items) if score_items else "sin ajuste estructura",
        chase_risk=chase_risk,
        adx_human=adx_human,
        koncorde_human=koncorde_human,
    )

    result = direction if alert_allowed else ("CONFLICT" if blockers else "SIN_SEÑAL")

    return {
        "result": result,
        "signal_type": signal_type,
        "score": capped_total,
        "score_total": capped_total,
        "quality": final_quality,
        "raw_quality": raw_quality,
        "final_quality": final_quality,
        "chase_risk": chase_risk,
        "structure_notes": " | ".join(score_items) if score_items else "sin ajuste estructura",
        "score_items": score_items,
        "penalties": penalties,
        "blockers": blockers,
        "degraders": sorted(set(degraders)),
        "no_signal_reason": no_signal_reason,
        "alert_allowed": alert_allowed,
        "adx_human": adx_human,
        "koncorde_human": koncorde_human,
        "human_report": human_report,
    }

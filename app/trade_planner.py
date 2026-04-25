from __future__ import annotations

from dataclasses import asdict, dataclass, is_dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


ALLOWED_SIGNAL_TYPES = {
    "LONG_PULLBACK",
    "SHORT_PULLBACK",
    "LONG_CONTINUATION",
    "SHORT_CONTINUATION",
    "LONG_REVERSAL_EARLY",
    "SHORT_REVERSAL_EARLY",
}


def evaluate_plan_gate(
    *,
    planner_enabled: bool,
    quality_payload: dict[str, Any],
    structure_m15: dict[str, Any] | None,
) -> tuple[bool, str]:
    if not planner_enabled:
        return False, "planner disabled"
    signal_type = str(quality_payload.get("signal_type", "SIN_SEÑAL"))
    quality = str(quality_payload.get("quality", "BAJA"))
    adx_state = str(quality_payload.get("adx_human", {}).get("state", ""))
    kon_state = str(quality_payload.get("koncorde_human", {}).get("state", ""))
    pullback = str((structure_m15 or {}).get("pullback", "NONE"))
    bos = str((structure_m15 or {}).get("bos", "NONE"))

    if signal_type in {"SIN_SEÑAL", "CONFLICT"}:
        return False, "signal_type no permitido"
    if signal_type == "MOMENTUM_CHASE":
        return False, "signal_type no permitido"
    if quality == "BAJA":
        return False, "quality insuficiente"
    if adx_state == "ADX_WEAK":
        return False, "ADX débil"
    if kon_state == "DRY_VOLUME":
        return False, "volumen seco"
    if pullback not in {"LONG", "SHORT"} and adx_state == "ADX_WEAK" and kon_state == "DRY_VOLUME":
        return False, "ADX débil + volumen seco + sin pullback"
    if signal_type.endswith("CONTINUATION") and bos == "NONE" and pullback not in {"LONG", "SHORT"}:
        return False, "sin pullback"
    return True, "OK"


@dataclass(slots=True)
class TradePlan:
    symbol: str
    direction: str
    signal_type: str
    setup_type: str
    execution_tf: str
    trigger_tf: str
    quality: str
    current_price: float
    entry_zone_low: float
    entry_zone_high: float
    stop_loss: float
    tp1: float
    tp2: float
    tp3: float
    rr_tp1: float
    rr_tp2: float
    rr_tp3: float
    entry_price: float
    invalidation_reason: str
    reasons: list[str]
    warnings: list[str]
    created_at: str
    expires_at: str
    status: str = "CREATED"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def trade_plan_to_dict(plan: Any) -> dict[str, Any]:
    if isinstance(plan, dict):
        return dict(plan)
    if hasattr(plan, "model_dump"):
        payload = plan.model_dump()
        return dict(payload) if isinstance(payload, dict) else {"value": payload}
    if is_dataclass(plan):
        return asdict(plan)
    if hasattr(plan, "to_dict"):
        payload = plan.to_dict()
        return dict(payload) if isinstance(payload, dict) else {"value": payload}
    known_fields = (
        "symbol",
        "direction",
        "signal_type",
        "setup_type",
        "execution_tf",
        "trigger_tf",
        "quality",
        "current_price",
        "entry_zone_low",
        "entry_zone_high",
        "stop_loss",
        "tp1",
        "tp2",
        "tp3",
        "rr_tp1",
        "rr_tp2",
        "rr_tp3",
        "entry_price",
        "invalidation_reason",
        "reasons",
        "warnings",
        "created_at",
        "expires_at",
        "status",
    )
    payload: dict[str, Any] = {}
    for name in known_fields:
        if hasattr(plan, name):
            payload[name] = getattr(plan, name)
    if payload:
        return payload
    if hasattr(plan, "__slots__"):
        slot_payload = {name: getattr(plan, name) for name in plan.__slots__ if hasattr(plan, name)}
        if slot_payload:
            return slot_payload
    if hasattr(plan, "__dict__"):
        return dict(plan.__dict__)
    raise TypeError(f"No se pudo serializar TradePlan desde tipo {type(plan)!r}")


def fib_retracement_zone(swing_high: float, swing_low: float, direction: str) -> dict[str, float]:
    rng = abs(swing_high - swing_low)
    if direction == "LONG":
        return {
            "0.5": swing_high - (rng * 0.5),
            "0.618": swing_high - (rng * 0.618),
            "0.705": swing_high - (rng * 0.705),
        }
    return {
        "0.5": swing_low + (rng * 0.5),
        "0.618": swing_low + (rng * 0.618),
        "0.705": swing_low + (rng * 0.705),
    }


def fib_extensions(swing_high: float, swing_low: float, direction: str) -> dict[str, float]:
    rng = abs(swing_high - swing_low)
    if direction == "LONG":
        return {
            "1.272": swing_high + (rng * 0.272),
            "1.618": swing_high + (rng * 0.618),
            "2.0": swing_high + rng,
        }
    return {
        "1.272": swing_low - (rng * 0.272),
        "1.618": swing_low - (rng * 0.618),
        "2.0": swing_low - rng,
    }


def detect_order_block(candles: list[dict[str, float]] | None, direction: str, bos_context: str) -> dict[str, float] | None:
    if not candles or bos_context not in {"BULL", "BEAR"}:
        return None
    if direction == "LONG":
        for candle in reversed(candles[:-1]):
            if candle["close"] < candle["open"]:
                return {"low": candle["low"], "high": candle["high"]}
    else:
        for candle in reversed(candles[:-1]):
            if candle["close"] > candle["open"]:
                return {"low": candle["low"], "high": candle["high"]}
    return None


def build_trade_plan(
    *,
    symbol: str,
    quality_payload: dict[str, Any],
    current_price: float,
    structure_m15: dict[str, Any],
    atr_value: float | None = None,
    expire_minutes: int = 120,
    min_rr_tp1: float = 1.0,
    atr_buffer_mult: float = 0.25,
    use_fib: bool = True,
    use_order_blocks: bool = True,
    study_mode: bool = False,
) -> TradePlan | None:
    signal_type = str(quality_payload.get("signal_type", "SIN_SEÑAL"))
    direction = str(quality_payload.get("result", "SIN_SEÑAL"))
    alert_allowed = bool(quality_payload.get("alert_allowed", False))

    if not alert_allowed:
        return None
    if signal_type == "MOMENTUM_CHASE" and not study_mode:
        return None
    if signal_type in {"SIN_SEÑAL", "CONFLICT"}:
        return None
    if signal_type not in ALLOWED_SIGNAL_TYPES:
        return None
    if direction not in {"LONG", "SHORT"}:
        return None

    ema_human = quality_payload.get("ema_human", {})
    above_ema200 = str(ema_human.get("close_vs_ema200", "UNKNOWN")) == "ABOVE"
    ema55_above_ema200 = str(ema_human.get("ema55_vs_ema200", "UNKNOWN")) == "ABOVE"
    has_bos_bull = str(structure_m15.get("bos", "NONE")) == "BULL"
    has_pullback_long = str(structure_m15.get("pullback", "NONE")) == "LONG"
    has_higher_low = bool(structure_m15.get("hl", False))

    setup_type = signal_type
    if direction == "LONG":
        if not above_ema200:
            return None
        if has_bos_bull and has_pullback_long and (has_higher_low or ema55_above_ema200):
            setup_type = "LONG_PIVOT_RETEST_M15"

    swing_high = float(structure_m15.get("last_high") or current_price * 1.01)
    swing_low = float(structure_m15.get("last_low") or current_price * 0.99)
    if swing_high <= swing_low:
        swing_high = max(swing_high, current_price * 1.01)
        swing_low = min(swing_low, current_price * 0.99)

    retr = fib_retracement_zone(swing_high, swing_low, direction) if use_fib else {}
    ext = fib_extensions(swing_high, swing_low, direction)

    reasons = ["pullback pivot", "estructura M15"]
    warnings: list[str] = []
    ob = None
    if use_order_blocks:
        ob = detect_order_block(None, direction, str(structure_m15.get("bos", "NONE")))
        if ob is not None:
            reasons.append("order block")

    if retr:
        entry_candidates = [retr["0.5"], retr["0.618"], retr["0.705"]]
        entry_zone_low = min(entry_candidates)
        entry_zone_high = max(entry_candidates)
        reasons.append("fib 0.5/0.618/0.705")
    else:
        spread = abs(swing_high - swing_low) * 0.2
        entry_zone_low = current_price - spread
        entry_zone_high = current_price + spread

    atr = atr_value if atr_value is not None else (abs(swing_high - swing_low) * 0.2)
    buffer = max(atr * atr_buffer_mult, current_price * 0.001)
    broken_level = float(structure_m15.get("broken_level") or (swing_high if direction == "LONG" else swing_low))
    pivot_zone_low = broken_level - (atr * 0.25)
    pivot_zone_high = broken_level + (atr * 0.25)

    if direction == "LONG":
        if setup_type == "LONG_PIVOT_RETEST_M15":
            entry_zone_low = min(entry_zone_low, pivot_zone_low)
            entry_zone_high = max(entry_zone_high, pivot_zone_high)
        stop_loss = min((ob["low"] if ob else swing_low), entry_zone_low) - buffer
        risk = max(1e-9, ((entry_zone_low + entry_zone_high) / 2) - stop_loss)
        tp1 = max(((entry_zone_low + entry_zone_high) / 2) + risk, swing_high)
        tp2 = max(((entry_zone_low + entry_zone_high) / 2) + (risk * 2), ext["1.272"])
        tp3 = max(((entry_zone_low + entry_zone_high) / 2) + (risk * 3), ext["1.618"])
        if current_price > entry_zone_high:
            warnings.append("no perseguir si no toca entrada")
    else:
        stop_loss = (ob["high"] if ob else swing_high) + buffer
        risk = max(1e-9, stop_loss - ((entry_zone_low + entry_zone_high) / 2))
        tp1 = min(((entry_zone_low + entry_zone_high) / 2) - risk, swing_low)
        tp2 = min(((entry_zone_low + entry_zone_high) / 2) - (risk * 2), ext["1.272"])
        tp3 = min(((entry_zone_low + entry_zone_high) / 2) - (risk * 3), ext["1.618"])
        if current_price < entry_zone_low:
            warnings.append("no perseguir si no toca entrada")

    entry_mid = (entry_zone_low + entry_zone_high) / 2
    if direction == "LONG":
        rr_tp1 = (tp1 - entry_mid) / max(1e-9, entry_mid - stop_loss)
        rr_tp2 = (tp2 - entry_mid) / max(1e-9, entry_mid - stop_loss)
        rr_tp3 = (tp3 - entry_mid) / max(1e-9, entry_mid - stop_loss)
    else:
        rr_tp1 = (entry_mid - tp1) / max(1e-9, stop_loss - entry_mid)
        rr_tp2 = (entry_mid - tp2) / max(1e-9, stop_loss - entry_mid)
        rr_tp3 = (entry_mid - tp3) / max(1e-9, stop_loss - entry_mid)

    if rr_tp1 < min_rr_tp1:
        warnings.append("TP1 < 1R, plan_quality baja")
    if abs(stop_loss - entry_mid) / entry_mid > 0.03:
        warnings.append("SL amplio")
    if "REVERSAL_EARLY" in signal_type:
        warnings.append("reversal temprano: requiere confirmación")

    now = datetime.now(tz=timezone.utc)
    entry_mid = (entry_zone_low + entry_zone_high) / 2
    return TradePlan(
        symbol=symbol,
        direction=direction,
        signal_type=signal_type,
        setup_type=setup_type,
        execution_tf="15m",
        trigger_tf="15m",
        quality=str(quality_payload.get("quality", "BAJA")),
        current_price=float(current_price),
        entry_zone_low=float(entry_zone_low),
        entry_zone_high=float(entry_zone_high),
        stop_loss=float(stop_loss),
        tp1=float(tp1),
        tp2=float(tp2),
        tp3=float(tp3),
        rr_tp1=float(rr_tp1),
        rr_tp2=float(rr_tp2),
        rr_tp3=float(rr_tp3),
        entry_price=float(entry_mid),
        invalidation_reason="loss_of_m15_structure",
        reasons=reasons,
        warnings=warnings,
        created_at=now.isoformat(),
        expires_at=(now + timedelta(minutes=expire_minutes)).isoformat(),
    )

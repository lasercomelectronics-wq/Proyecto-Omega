from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class TradeSide(StrEnum):
    LONG = "LONG"
    SHORT = "SHORT"


class TradeStatus(StrEnum):
    ACTIVE = "ACTIVE"
    PAUSED = "PAUSED"
    CLOSED = "CLOSED"
    DELETED = "DELETED"


class AlertPriority(StrEnum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"


class Bias(StrEnum):
    BULLISH = "bullish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"


@dataclass(slots=True)
class InvalidationZone:
    min: float
    max: float


@dataclass(slots=True)
class AddZone:
    price: float
    size_usdt: float
    note: str = ""


@dataclass(slots=True)
class TradeAlertConfig:
    cooldown_seconds: int = 300
    sl_distance_threshold_pct: float | None = None
    notify_on_tp: bool = True
    notify_on_sl_distance: bool = True
    notify_on_invalidation_zone: bool = True
    notify_on_break_even: bool = True
    notify_on_add_zone: bool = True


@dataclass(slots=True)
class TradeStrategyConfig:
    timeframe: str | None = None
    ema_fast: int = 55
    ema_slow: int = 200
    use_structure: bool = True
    pivot_length: int = 5
    use_squeeze_momentum_placeholder: bool = False


@dataclass(slots=True)
class TradeConfig:
    symbol: str
    side: TradeSide
    entry: float
    leverage: int
    stop_loss: float
    id: int | None = None
    status: TradeStatus = TradeStatus.ACTIVE
    take_profits: list[float] = field(default_factory=list)
    invalidation_zone: InvalidationZone | None = None
    add_zones: list[AddZone] = field(default_factory=list)
    alerts: TradeAlertConfig = field(default_factory=TradeAlertConfig)
    strategy: TradeStrategyConfig = field(default_factory=TradeStrategyConfig)
    note: str = ""
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(slots=True)
class PositionSnapshot:
    symbol: str
    side: TradeSide
    quantity: float
    entry_price: float
    mark_price: float
    unrealized_pnl: float = 0.0
    break_even_price: float | None = None
    liquidation_price: float | None = None
    update_time: int | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    close_time: int
    volume: float = 0.0


@dataclass(slots=True)
class StructureState:
    trend: str
    last_swing_high: float | None = None
    last_swing_low: float | None = None
    pivot_highs: list[tuple[int, float]] = field(default_factory=list)
    pivot_lows: list[tuple[int, float]] = field(default_factory=list)


@dataclass(slots=True)
class PnlEstimate:
    raw_pct: float
    leveraged_pct: float
    pnl_usdt: float | None = None


@dataclass(slots=True)
class TradeRuntimeState:
    symbol: str
    side: TradeSide
    highest_price: float | None = None
    lowest_price: float | None = None
    last_price: float | None = None
    entry_seen_profit: bool = False
    tp_hits: list[float] = field(default_factory=list)
    updated_at: datetime | None = None


@dataclass(slots=True)
class MarketContext:
    symbol: str
    ema_fast: float | None = None
    ema_slow: float | None = None
    bias: Bias = Bias.NEUTRAL
    structure: str = "neutral"
    refreshed_at: datetime | None = None


@dataclass(slots=True)
class PriceUpdate:
    symbol: str
    price: float
    event_time: int
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AlertEvent:
    key: str
    priority: AlertPriority
    reason: str
    symbol: str = "SYSTEM"
    side: TradeSide | None = None
    current_price: float | None = None
    entry: float | None = None
    stop_loss: float | None = None
    take_profits: list[float] = field(default_factory=list)
    approx_pnl_usdt: float | None = None
    approx_pnl_pct: float | None = None
    note: str | None = None
    bias: Bias | None = None
    ema_fast: float | None = None
    ema_slow: float | None = None
    structure: str | None = None
    cooldown_seconds: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class AppRuntimeSettings:
    log_level: str = "INFO"
    context_refresh_seconds: int = 300


@dataclass(slots=True)
class BinanceSettings:
    recv_window: int = 5000
    rest_timeout_seconds: float = 15.0
    kline_interval: str = "1m"
    kline_limit: int = 250
    market_stream_suffix: str = "@markPrice@1s"
    user_stream_keepalive_minutes: int = 45
    reconnect_backoff_seconds: list[int] = field(
        default_factory=lambda: [1, 3, 5, 10, 20]
    )


@dataclass(slots=True)
class AlertEngineSettings:
    stop_loss_warning_pct: float = 0.35
    break_even_trigger_rr: float = 0.5
    entry_revisit_tolerance_pct: float = 0.05
    add_zone_tolerance_pct: float = 0.05
    system_cooldown_seconds: int = 120
    telegram_parse_mode: str = "HTML"


@dataclass(slots=True)
class AppSettings:
    app: AppRuntimeSettings = field(default_factory=AppRuntimeSettings)
    binance: BinanceSettings = field(default_factory=BinanceSettings)
    alerts: AlertEngineSettings = field(default_factory=AlertEngineSettings)


def make_trade_key(symbol: str, side: TradeSide) -> str:
    return f"{symbol.upper()}::{side.value}"

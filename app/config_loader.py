from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from app.models import (
    AddZone,
    AlertEngineSettings,
    AppRuntimeSettings,
    AppSettings,
    BinanceSettings,
    InvalidationZone,
    TradeAlertConfig,
    TradeConfig,
    TradeSide,
    TradeStatus,
    TradeStrategyConfig,
)


class ConfigError(ValueError):
    """Raised when the YAML configuration is invalid."""


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"No existe el archivo de configuración: {path}")

    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}

    if not isinstance(raw, dict):
        raise ConfigError(f"El archivo {path} debe contener un objeto YAML raíz.")
    return raw


def _as_float(value: Any, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"'{field_name}' debe ser numérico.") from exc


def _as_int(value: Any, field_name: str) -> int:
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"'{field_name}' debe ser entero.") from exc


def _as_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    raise ConfigError(f"'{field_name}' debe ser booleano.")


def load_settings(path: Path) -> AppSettings:
    raw = _read_yaml(path)

    app_raw = raw.get("app", {})
    binance_raw = raw.get("binance", {})
    alerts_raw = raw.get("alerts", {})

    settings = AppSettings(
        app=AppRuntimeSettings(
            log_level=str(app_raw.get("log_level", "INFO")).upper(),
            context_refresh_seconds=_as_int(
                app_raw.get("context_refresh_seconds", 300),
                "app.context_refresh_seconds",
            ),
        ),
        binance=BinanceSettings(
            recv_window=_as_int(
                binance_raw.get("recv_window", 5000), "binance.recv_window"
            ),
            rest_timeout_seconds=_as_float(
                binance_raw.get("rest_timeout_seconds", 15),
                "binance.rest_timeout_seconds",
            ),
            kline_interval=str(binance_raw.get("kline_interval", "1m")),
            kline_limit=_as_int(binance_raw.get("kline_limit", 250), "binance.kline_limit"),
            market_stream_suffix=str(
                binance_raw.get("market_stream_suffix", "@markPrice@1s")
            ),
            user_stream_keepalive_minutes=_as_int(
                binance_raw.get("user_stream_keepalive_minutes", 45),
                "binance.user_stream_keepalive_minutes",
            ),
            reconnect_backoff_seconds=[
                _as_int(value, "binance.reconnect_backoff_seconds")
                for value in binance_raw.get(
                    "reconnect_backoff_seconds", [1, 3, 5, 10, 20]
                )
            ],
        ),
        alerts=AlertEngineSettings(
            stop_loss_warning_pct=_as_float(
                alerts_raw.get("stop_loss_warning_pct", 0.35),
                "alerts.stop_loss_warning_pct",
            ),
            break_even_trigger_rr=_as_float(
                alerts_raw.get("break_even_trigger_rr", 0.5),
                "alerts.break_even_trigger_rr",
            ),
            entry_revisit_tolerance_pct=_as_float(
                alerts_raw.get("entry_revisit_tolerance_pct", 0.05),
                "alerts.entry_revisit_tolerance_pct",
            ),
            add_zone_tolerance_pct=_as_float(
                alerts_raw.get("add_zone_tolerance_pct", 0.05),
                "alerts.add_zone_tolerance_pct",
            ),
            system_cooldown_seconds=_as_int(
                alerts_raw.get("system_cooldown_seconds", 120),
                "alerts.system_cooldown_seconds",
            ),
            telegram_parse_mode=str(alerts_raw.get("telegram_parse_mode", "HTML")),
        ),
    )

    if settings.binance.kline_limit < 50:
        raise ConfigError("binance.kline_limit debe ser al menos 50 para EMAs y estructura.")

    return settings


def load_trades(path: Path) -> list[TradeConfig]:
    raw = _read_yaml(path)
    trades_raw = raw.get("trades", [])

    if not isinstance(trades_raw, list):
        raise ConfigError("'trades' debe ser una lista.")

    trades: list[TradeConfig] = []
    for index, item in enumerate(trades_raw, start=1):
        if not isinstance(item, dict):
            raise ConfigError(f"Cada trade debe ser un objeto. Error en índice {index}.")

        symbol = str(item.get("symbol", "")).upper().strip()
        side_text = str(item.get("side", "")).upper().strip()
        if not symbol:
            raise ConfigError(f"trade[{index}].symbol es obligatorio.")

        try:
            side = TradeSide(side_text)
        except ValueError as exc:
            raise ConfigError(f"trade[{index}].side debe ser LONG o SHORT.") from exc

        invalidation_raw = item.get("invalidation_zone")
        invalidation_zone = None
        if invalidation_raw is not None:
            if not isinstance(invalidation_raw, dict):
                raise ConfigError(f"trade[{index}].invalidation_zone debe ser un objeto.")
            invalidation_zone = InvalidationZone(
                min=_as_float(invalidation_raw.get("min"), f"trade[{index}].invalidation_zone.min"),
                max=_as_float(invalidation_raw.get("max"), f"trade[{index}].invalidation_zone.max"),
            )
            if invalidation_zone.min > invalidation_zone.max:
                raise ConfigError(
                    f"trade[{index}].invalidation_zone.min no puede ser mayor que max."
                )

        add_zones_raw = item.get("add_zones", [])
        if not isinstance(add_zones_raw, list):
            raise ConfigError(f"trade[{index}].add_zones debe ser una lista.")
        add_zones = [
            AddZone(
                price=_as_float(zone.get("price"), f"trade[{index}].add_zones.price"),
                size_usdt=_as_float(
                    zone.get("size_usdt"), f"trade[{index}].add_zones.size_usdt"
                ),
                note=str(zone.get("note", "")),
            )
            for zone in add_zones_raw
        ]

        alerts_raw = item.get("alerts", {})
        if alerts_raw and not isinstance(alerts_raw, dict):
            raise ConfigError(f"trade[{index}].alerts debe ser un objeto.")
        alerts = TradeAlertConfig(
            cooldown_seconds=_as_int(
                alerts_raw.get("cooldown_seconds", 300),
                f"trade[{index}].alerts.cooldown_seconds",
            ),
            sl_distance_threshold_pct=(
                _as_float(
                    alerts_raw.get("sl_distance_threshold_pct"),
                    f"trade[{index}].alerts.sl_distance_threshold_pct",
                )
                if alerts_raw.get("sl_distance_threshold_pct") is not None
                else None
            ),
            notify_on_tp=_as_bool(
                alerts_raw.get("notify_on_tp", True),
                f"trade[{index}].alerts.notify_on_tp",
            ),
            notify_on_sl_distance=_as_bool(
                alerts_raw.get("notify_on_sl_distance", True),
                f"trade[{index}].alerts.notify_on_sl_distance",
            ),
            notify_on_invalidation_zone=_as_bool(
                alerts_raw.get("notify_on_invalidation_zone", True),
                f"trade[{index}].alerts.notify_on_invalidation_zone",
            ),
            notify_on_break_even=_as_bool(
                alerts_raw.get("notify_on_break_even", True),
                f"trade[{index}].alerts.notify_on_break_even",
            ),
            notify_on_add_zone=_as_bool(
                alerts_raw.get("notify_on_add_zone", True),
                f"trade[{index}].alerts.notify_on_add_zone",
            ),
        )

        strategy_raw = item.get("strategy", {})
        if strategy_raw and not isinstance(strategy_raw, dict):
            raise ConfigError(f"trade[{index}].strategy debe ser un objeto.")
        strategy = TradeStrategyConfig(
            timeframe=(
                str(strategy_raw.get("timeframe")).strip()
                if strategy_raw.get("timeframe") is not None
                else None
            ),
            ema_fast=_as_int(strategy_raw.get("ema_fast", 55), f"trade[{index}].strategy.ema_fast"),
            ema_slow=_as_int(strategy_raw.get("ema_slow", 200), f"trade[{index}].strategy.ema_slow"),
            use_structure=_as_bool(
                strategy_raw.get("use_structure", True),
                f"trade[{index}].strategy.use_structure",
            ),
            pivot_length=_as_int(
                strategy_raw.get("pivot_length", 5),
                f"trade[{index}].strategy.pivot_length",
            ),
            use_squeeze_momentum_placeholder=_as_bool(
                strategy_raw.get("use_squeeze_momentum_placeholder", False),
                f"trade[{index}].strategy.use_squeeze_momentum_placeholder",
            ),
        )

        take_profits_raw = item.get("take_profits", [])
        if not isinstance(take_profits_raw, list):
            raise ConfigError(f"trade[{index}].take_profits debe ser una lista.")

        trade = TradeConfig(
            symbol=symbol,
            side=side,
            entry=_as_float(item.get("entry"), f"trade[{index}].entry"),
            leverage=_as_int(item.get("leverage"), f"trade[{index}].leverage"),
            stop_loss=_as_float(item.get("stop_loss"), f"trade[{index}].stop_loss"),
            status=TradeStatus.ACTIVE,
            take_profits=[
                _as_float(tp, f"trade[{index}].take_profits") for tp in take_profits_raw
            ],
            invalidation_zone=invalidation_zone,
            add_zones=add_zones,
            alerts=alerts,
            strategy=strategy,
            note=str(item.get("note", "")),
        )
        trades.append(trade)

    return trades

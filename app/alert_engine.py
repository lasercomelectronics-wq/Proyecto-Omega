from __future__ import annotations

from app.models import (
    AlertEngineSettings,
    AlertEvent,
    AlertPriority,
    MarketContext,
    PositionSnapshot,
    TradeConfig,
    TradeRuntimeState,
    TradeSide,
    TradeStatus,
    make_trade_key,
)
from app.strategy_engine import (
    approximate_pnl,
    calculate_distance_to_stop_loss,
    detect_touched_add_zone,
    has_price_reached_level,
    is_price_in_invalidation_zone,
    should_alert_break_even,
)


class AlertEngine:
    def __init__(self, settings: AlertEngineSettings) -> None:
        self._settings = settings

    def _build_trade_event(
        self,
        trade: TradeConfig,
        *,
        priority: AlertPriority,
        reason: str,
        current_price: float | None,
        position: PositionSnapshot | None,
        market_context: MarketContext | None,
        key_suffix: str,
        note: str | None = None,
        metadata: dict | None = None,
    ) -> AlertEvent:
        entry_price = position.entry_price if position else trade.entry
        quantity = position.quantity if position else None
        pnl = (
            approximate_pnl(
                trade.side,
                entry_price=entry_price,
                current_price=current_price if current_price is not None else entry_price,
                quantity=quantity,
                leverage=trade.leverage,
            )
            if current_price is not None
            else None
        )
        return AlertEvent(
            key=f"{make_trade_key(trade.symbol, trade.side)}::{key_suffix}",
            priority=priority,
            reason=reason,
            symbol=trade.symbol,
            side=trade.side,
            current_price=current_price,
            entry=trade.entry,
            stop_loss=trade.stop_loss,
            take_profits=trade.take_profits,
            approx_pnl_usdt=pnl.pnl_usdt if pnl else None,
            approx_pnl_pct=pnl.leveraged_pct if pnl else None,
            note=note or trade.note or None,
            bias=market_context.bias if market_context else None,
            ema_fast=market_context.ema_fast if market_context else None,
            ema_slow=market_context.ema_slow if market_context else None,
            structure=market_context.structure if market_context else None,
            cooldown_seconds=trade.alerts.cooldown_seconds,
            metadata=metadata or {},
        )

    def evaluate_trade(
        self,
        trade: TradeConfig,
        *,
        current_price: float,
        runtime: TradeRuntimeState,
        previous_price: float | None,
        position: PositionSnapshot | None,
        market_context: MarketContext | None,
    ) -> list[AlertEvent]:
        if trade.status != TradeStatus.ACTIVE:
            return []

        events: list[AlertEvent] = []

        if (
            trade.alerts.notify_on_invalidation_zone
            and trade.invalidation_zone is not None
            and is_price_in_invalidation_zone(
                current_price,
                trade.invalidation_zone.min,
                trade.invalidation_zone.max,
            )
        ):
            events.append(
                self._build_trade_event(
                    trade,
                    priority=AlertPriority.CRITICAL,
                    reason=(
                        "Precio dentro de la zona de invalidación "
                        f"{trade.invalidation_zone.min:.6f} - {trade.invalidation_zone.max:.6f}"
                    ),
                    current_price=current_price,
                    position=position,
                    market_context=market_context,
                    key_suffix="invalidation",
                )
            )

        if trade.alerts.notify_on_sl_distance:
            sl_distance_pct = calculate_distance_to_stop_loss(current_price, trade.stop_loss)
            sl_threshold = (
                trade.alerts.sl_distance_threshold_pct
                if trade.alerts.sl_distance_threshold_pct is not None
                else self._settings.stop_loss_warning_pct
            )
            if sl_distance_pct <= sl_threshold:
                events.append(
                    self._build_trade_event(
                        trade,
                        priority=AlertPriority.WARNING,
                        reason=(
                            f"Precio a {sl_distance_pct:.4f}% del stop loss. "
                            f"Umbral configurado: {sl_threshold:.4f}%"
                        ),
                        current_price=current_price,
                        position=position,
                        market_context=market_context,
                        key_suffix="sl_distance",
                    )
                )

        if trade.alerts.notify_on_tp:
            for index, level in enumerate(trade.take_profits, start=1):
                if level in runtime.tp_hits:
                    continue
                current_hit = has_price_reached_level(trade.side, current_price, level)
                previous_hit = (
                    has_price_reached_level(trade.side, previous_price, level)
                    if previous_price is not None
                    else False
                )
                if current_hit and not previous_hit:
                    events.append(
                        self._build_trade_event(
                            trade,
                            priority=AlertPriority.INFO,
                            reason=f"TP{index} alcanzado en {level:.6f}",
                            current_price=current_price,
                            position=position,
                            market_context=market_context,
                            key_suffix=f"tp{index}",
                            metadata={"tp_level": level, "tp_index": index},
                        )
                    )

        add_zone = detect_touched_add_zone(
            previous_price,
            current_price,
            trade.add_zones,
            self._settings.add_zone_tolerance_pct,
        )
        if trade.alerts.notify_on_add_zone and add_zone is not None:
            events.append(
                self._build_trade_event(
                    trade,
                    priority=AlertPriority.INFO,
                    reason=f"Precio tocó zona de add en {add_zone.price:.6f}",
                    current_price=current_price,
                    position=position,
                    market_context=market_context,
                    key_suffix=f"add_{add_zone.price:.6f}",
                    note=add_zone.note or trade.note or None,
                )
            )

        if trade.alerts.notify_on_break_even and runtime.entry_seen_profit:
            best_price = runtime.highest_price if trade.side == TradeSide.LONG else runtime.lowest_price
            if should_alert_break_even(
                trade.side,
                entry_price=trade.entry,
                stop_loss=trade.stop_loss,
                current_price=current_price,
                best_price=best_price,
                min_rr=self._settings.break_even_trigger_rr,
                entry_tolerance_pct=self._settings.entry_revisit_tolerance_pct,
            ):
                events.append(
                    self._build_trade_event(
                        trade,
                        priority=AlertPriority.WARNING,
                        reason="El precio volvió a la entrada después de haber estado en ganancia.",
                        current_price=current_price,
                        position=position,
                        market_context=market_context,
                        key_suffix="break_even",
                    )
                )

        return events

    def evaluate_position_alignment(
        self,
        trades: list[TradeConfig],
        positions: list[PositionSnapshot],
    ) -> list[AlertEvent]:
        events: list[AlertEvent] = []
        declared_trades = {
            make_trade_key(trade.symbol, trade.side): trade
            for trade in trades
            if trade.status in {TradeStatus.ACTIVE, TradeStatus.PAUSED}
        }
        active_trades = {
            key: trade
            for key, trade in declared_trades.items()
            if trade.status == TradeStatus.ACTIVE
        }
        positions_by_key = {
            make_trade_key(position.symbol, position.side): position for position in positions
        }

        for key, position in positions_by_key.items():
            if key in declared_trades:
                continue
            pnl = approximate_pnl(
                position.side,
                entry_price=position.entry_price,
                current_price=position.mark_price,
                quantity=position.quantity,
            )
            events.append(
                AlertEvent(
                    key=f"system::undeclared::{key}",
                    priority=AlertPriority.WARNING,
                    reason="Posición abierta detectada en Binance pero no declarada en trades.yaml.",
                    symbol=position.symbol,
                    side=position.side,
                    current_price=position.mark_price,
                    entry=position.entry_price,
                    approx_pnl_usdt=pnl.pnl_usdt,
                    approx_pnl_pct=pnl.leveraged_pct,
                    cooldown_seconds=self._settings.system_cooldown_seconds,
                )
            )

        for key, trade in active_trades.items():
            if key in positions_by_key:
                continue
            events.append(
                AlertEvent(
                    key=f"system::missing_position::{key}",
                    priority=AlertPriority.INFO,
                    reason="Trade declarado en trades.yaml pero sin posición abierta en Binance.",
                    symbol=trade.symbol,
                    side=trade.side,
                    entry=trade.entry,
                    stop_loss=trade.stop_loss,
                    take_profits=trade.take_profits,
                    cooldown_seconds=max(
                        trade.alerts.cooldown_seconds,
                        self._settings.system_cooldown_seconds,
                    ),
                    note=trade.note or None,
                )
            )

        return events

    def build_system_alert(
        self,
        *,
        key: str,
        reason: str,
        priority: AlertPriority,
        note: str | None = None,
        cooldown_seconds: int | None = None,
    ) -> AlertEvent:
        return AlertEvent(
            key=f"system::{key}",
            priority=priority,
            reason=reason,
            note=note,
            cooldown_seconds=cooldown_seconds or self._settings.system_cooldown_seconds,
        )

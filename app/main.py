from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

from app.alert_engine import AlertEngine
from app.binance_client import BinanceAPIError, BinanceFuturesClient
from app.binance_ws import BinanceMarketStream, BinanceUserDataStream
from app.config_loader import ConfigError, load_settings, load_trades
from app.logger import configure_logging
from app.models import AlertPriority, PriceUpdate, TradeConfig, TradeStatus
from app.scanner import SignalScanner
from app.storage import Storage
from app.strategy_engine import build_market_context
from app.telegram_command_bot import TelegramCommandBot, TelegramCommandBotError
from app.telegram_notifier import TelegramNotificationError, TelegramNotifier
from app.plan_monitor import PlanMonitor
from app.trade_planner import build_trade_plan
from app.trade_manager import TradeManager


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw.strip())
    except ValueError:
        return default


class TradingAlertBot:
    def __init__(self, root_dir: Path) -> None:
        self.root_dir = root_dir
        self.logger = logging.getLogger("trading_alert_bot")
        self.settings = None
        self.storage: Storage | None = None
        self.trade_manager: TradeManager | None = None
        self.alert_engine: AlertEngine | None = None
        self.binance_client: BinanceFuturesClient | None = None
        self.telegram: TelegramNotifier | None = None
        self.command_bot: TelegramCommandBot | None = None
        self.market_stream: BinanceMarketStream | None = None
        self.user_stream: BinanceUserDataStream | None = None
        self.scanner: SignalScanner | None = None
        self.tasks: list[asyncio.Task] = []
        self.dry_run = True
        self.binance_testnet = False
        self.binance_private_account_sync = True
        self.binance_market_source = "mark_price"
        self.scanner_enabled = True
        self.scanner_alerts_enabled = True
        self.scanner_interval_seconds = 60
        self.structure_enabled = True
        self.structure_timeframes = ("15m", "5m", "3m")
        self.pivot_window = 3
        self.pullback_tolerance_mode = "atr"
        self.pullback_atr_mult = 0.25
        self.pullback_pct = 0.15
        self.latest_prices: dict[str, PriceUpdate] = {}
        self.trade_planner_enabled = True
        self.plan_monitor_enabled = True
        self.plan_monitor_interval_seconds = 15
        self.plan_expire_minutes = 120
        self.plan_use_volume_profile = False
        self.plan_use_order_blocks = True
        self.plan_use_fib = True
        self.plan_min_rr_tp1 = 1.0
        self.plan_atr_buffer_mult = 0.25
        self.plan_monitor: PlanMonitor | None = None

    def _runtime_status(self) -> dict[str, str | int]:
        assert self.trade_manager is not None
        return {
            "DRY_RUN": str(self.dry_run).lower(),
            "BINANCE_TESTNET": str(self.binance_testnet).lower(),
            "BINANCE_PRIVATE_ACCOUNT_SYNC": str(self.binance_private_account_sync).lower(),
            "BINANCE_MARKET_SOURCE": self.binance_market_source,
            "active_trades": self.trade_manager.count_trades((TradeStatus.ACTIVE,)),
            "SCANNER_ENABLED": str(self.scanner.is_running if self.scanner is not None else False).lower(),
            "SCANNER_ALERTS_ENABLED": str(self.scanner_alerts_enabled).lower(),
            "SCANNER_INTERVAL_SECONDS": self.scanner_interval_seconds,
        }

    async def initialize(self) -> None:
        load_dotenv(self.root_dir / ".env")

        self.settings = load_settings(self.root_dir / "config" / "settings.yaml")
        yaml_trades = load_trades(self.root_dir / "config" / "trades.yaml")

        configured_log_level = os.getenv("LOG_LEVEL", self.settings.app.log_level).upper()
        self.logger = configure_logging(
            configured_log_level,
            secrets=[
                os.getenv("BINANCE_API_KEY", ""),
                os.getenv("BINANCE_API_SECRET", ""),
                os.getenv("TELEGRAM_BOT_TOKEN", ""),
            ],
        )

        self.storage = Storage(self.root_dir / "data" / "bot.sqlite")
        await self.storage.initialize()

        self.trade_manager = TradeManager(self.storage)
        await self.trade_manager.bootstrap_trades(yaml_trades)
        self.alert_engine = AlertEngine(self.settings.alerts)

        self.dry_run = _env_flag("DRY_RUN", True)
        self.binance_testnet = _env_flag("BINANCE_TESTNET", False)
        self.binance_private_account_sync = _env_flag("BINANCE_PRIVATE_ACCOUNT_SYNC", True)
        self.binance_market_source = os.getenv("BINANCE_MARKET_SOURCE", "mark_price").strip() or "mark_price"
        self.scanner_enabled = _env_flag("SCANNER_ENABLED", True)
        self.scanner_alerts_enabled = _env_flag("SCANNER_ALERTS_ENABLED", True)
        self.scanner_interval_seconds = _env_int("SCANNER_INTERVAL_SECONDS", 60)
        self.structure_enabled = _env_flag("STRUCTURE_ENABLED", True)
        self.structure_timeframes = tuple(
            item.strip().lower()
            for item in os.getenv("STRUCTURE_TIMEFRAMES", "15m,5m,3m").split(",")
            if item.strip()
        )
        self.pivot_window = _env_int("PIVOT_WINDOW", 3)
        self.pullback_tolerance_mode = os.getenv("PULLBACK_TOLERANCE_MODE", "atr").strip().lower() or "atr"
        self.pullback_atr_mult = _env_float("PULLBACK_ATR_MULT", 0.25)
        self.pullback_pct = _env_float("PULLBACK_PCT", 0.15)
        self.trade_planner_enabled = _env_flag("TRADE_PLANNER_ENABLED", True)
        self.plan_monitor_enabled = _env_flag("PLAN_MONITOR_ENABLED", True)
        self.plan_monitor_interval_seconds = _env_int("PLAN_MONITOR_INTERVAL_SECONDS", 15)
        self.plan_expire_minutes = _env_int("PLAN_EXPIRE_MINUTES", 120)
        self.plan_use_volume_profile = _env_flag("PLAN_USE_VOLUME_PROFILE", False)
        self.plan_use_order_blocks = _env_flag("PLAN_USE_ORDER_BLOCKS", True)
        self.plan_use_fib = _env_flag("PLAN_USE_FIB", True)
        self.plan_min_rr_tp1 = _env_float("PLAN_MIN_RR_TP1", 1.0)
        self.plan_atr_buffer_mult = _env_float("PLAN_ATR_BUFFER_MULT", 0.25)

        self.binance_client = BinanceFuturesClient(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
            testnet=self.binance_testnet,
            recv_window=self.settings.binance.recv_window,
            timeout_seconds=self.settings.binance.rest_timeout_seconds,
        )
        self.telegram = TelegramNotifier(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            parse_mode=self.settings.alerts.telegram_parse_mode,
        )
        self.command_bot = TelegramCommandBot(
            bot_token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            authorized_chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            trade_manager=self.trade_manager,
            alert_settings=self.settings.alerts,
            logger=self.logger,
            status_provider=self._runtime_status,
            binance_client=self.binance_client,
            current_price_provider=self.get_cached_price,
            on_trade_changed=self.handle_tradebook_changed,
            storage=self.storage,
            plan_builder=self.build_plan_for_signal,
            plan_expire_minutes=self.plan_expire_minutes,
            plan_use_fib=self.plan_use_fib,
            plan_use_order_blocks=self.plan_use_order_blocks,
            plan_min_rr_tp1=self.plan_min_rr_tp1,
            plan_atr_buffer_mult=self.plan_atr_buffer_mult,
            structure_enabled=self.structure_enabled,
            structure_timeframes=self.structure_timeframes,
            pivot_window=self.pivot_window,
            pullback_tolerance_mode=self.pullback_tolerance_mode,
            pullback_atr_mult=self.pullback_atr_mult,
            pullback_pct=self.pullback_pct,
        )
        if self.scanner_enabled:
            self.scanner = SignalScanner(
                binance_client=self.binance_client,
                trade_manager=self.trade_manager,
                storage=self.storage,
                dispatch_alerts=self._dispatch_alerts,
                logger=self.logger,
                interval_seconds=self.scanner_interval_seconds,
                alerts_enabled=self.scanner_alerts_enabled,
                structure_enabled=self.structure_enabled,
                structure_timeframes=self.structure_timeframes,
                pivot_window=self.pivot_window,
                pullback_tolerance_mode=self.pullback_tolerance_mode,
                pullback_atr_mult=self.pullback_atr_mult,
                pullback_pct=self.pullback_pct,
                on_signal_sent=self.handle_scanner_signal_sent,
            )
        if self.plan_monitor_enabled:
            self.plan_monitor = PlanMonitor(
                storage=self.storage,
                binance_client=self.binance_client,
                dispatch_alerts=self._dispatch_alerts,
                logger=self.logger,
                interval_seconds=self.plan_monitor_interval_seconds,
            )

    async def close(self) -> None:
        if self.scanner:
            await self.scanner.stop()
        for task in self.tasks:
            task.cancel()
        if self.market_stream:
            await self.market_stream.stop()
        if self.user_stream:
            await self.user_stream.stop()
        if self.plan_monitor:
            await self.plan_monitor.stop()
        if self.command_bot:
            await self.command_bot.close()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.telegram:
            await self.telegram.close()
        if self.binance_client:
            await self.binance_client.close()
        if self.storage:
            await self.storage.close()

    async def _dispatch_alerts(self, events) -> int:
        assert self.storage is not None
        assert self.telegram is not None
        assert self.trade_manager is not None

        sent_count = 0
        for event in events:
            should_send = await self.storage.should_send_alert(
                event.key,
                cooldown_seconds=event.cooldown_seconds,
            )
            if not should_send:
                continue

            try:
                await self.telegram.send_alert(event)
                await self.storage.record_alert(event)
                tp_level = event.metadata.get("tp_level")
                if tp_level is not None and event.side is not None:
                    trade = next(
                        (
                            candidate
                            for candidate in self.trade_manager.get_trades_for_symbol(
                                event.symbol,
                                statuses=(
                                    TradeStatus.ACTIVE,
                                    TradeStatus.PAUSED,
                                    TradeStatus.CLOSED,
                                ),
                            )
                            if candidate.side == event.side
                        ),
                        None,
                    )
                    if trade is not None and trade.side == event.side:
                        await self.trade_manager.mark_take_profit_hit(trade, float(tp_level))
                sent_count += 1
            except TelegramNotificationError as exc:
                self.logger.error("No se pudo enviar alerta a Telegram: %s", exc)
        return sent_count

    async def _sync_positions(self) -> None:
        assert self.binance_client is not None
        assert self.trade_manager is not None
        assert self.alert_engine is not None

        if not self.binance_private_account_sync:
            self.logger.info("BINANCE_PRIVATE_ACCOUNT_SYNC=false. Se omite sincronización privada.")
            return

        try:
            positions = await self.binance_client.get_positions()
            await self.trade_manager.sync_positions(positions)
            await self._dispatch_alerts(
                self.alert_engine.evaluate_position_alignment(
                    self.trade_manager.get_declared_trades(),
                    self.trade_manager.get_open_positions(),
                )
            )
        except BinanceAPIError as exc:
            await self._dispatch_alerts(
                [
                    self.alert_engine.build_system_alert(
                        key="api_error_positions",
                        reason="Error de API al sincronizar posiciones iniciales.",
                        priority=AlertPriority.CRITICAL,
                        note=str(exc),
                    )
                ]
            )

    async def _refresh_trade_context(self, trade: TradeConfig) -> None:
        assert self.binance_client is not None
        assert self.trade_manager is not None

        interval = trade.strategy.timeframe or self.settings.binance.kline_interval
        limit = max(
            self.settings.binance.kline_limit,
            trade.strategy.ema_slow + (trade.strategy.pivot_length * 2) + 10,
        )
        candles = await self.binance_client.get_klines(
            trade.symbol,
            interval=interval,
            limit=limit,
        )
        current_price = candles[-1].close
        context = build_market_context(
            trade.symbol,
            candles,
            current_price=current_price,
            ema_fast_period=trade.strategy.ema_fast,
            ema_slow_period=trade.strategy.ema_slow,
            pivot_length=trade.strategy.pivot_length,
        )
        await self.trade_manager.set_market_context(trade, context)

    async def handle_tradebook_changed(self, symbol: str | None = None) -> None:
        assert self.trade_manager is not None

        if self.market_stream is not None:
            await self.market_stream.update_symbols(self.trade_manager.get_monitored_symbols())

        if symbol:
            for trade in self.trade_manager.get_trades_for_symbol(
                symbol,
                statuses=(TradeStatus.ACTIVE, TradeStatus.PAUSED),
            ):
                try:
                    await self._refresh_trade_context(trade)
                except BinanceAPIError as exc:
                    await self._dispatch_alerts(
                        [
                            self.alert_engine.build_system_alert(
                                key=f"api_error_trade_change_{symbol.lower()}",
                                reason=f"Error al refrescar contexto para {symbol}.",
                                priority=AlertPriority.WARNING,
                                note=str(exc),
                            )
                        ]
                    )

    async def _context_refresh_loop(self) -> None:
        while True:
            trades = self.trade_manager.get_declared_trades(
                statuses=(TradeStatus.ACTIVE, TradeStatus.PAUSED)
            )
            for trade in trades:
                try:
                    await self._refresh_trade_context(trade)
                except BinanceAPIError as exc:
                    await self._dispatch_alerts(
                        [
                            self.alert_engine.build_system_alert(
                                key=f"api_error_klines_{trade.symbol.lower()}",
                                reason=f"Error de API al refrescar contexto de {trade.symbol}.",
                                priority=AlertPriority.WARNING,
                                note=str(exc),
                            )
                        ]
                    )
            await asyncio.sleep(self.settings.app.context_refresh_seconds)

    async def get_cached_price(self, symbol: str) -> tuple[float, str, int | None] | None:
        update = self.latest_prices.get(symbol.upper())
        if update is None:
            return None
        return update.price, "Binance Futures mark price (cache websocket)", update.event_time

    async def handle_price_update(self, update) -> None:
        assert self.trade_manager is not None
        assert self.alert_engine is not None

        self.latest_prices[update.symbol.upper()] = update
        trades = self.trade_manager.get_trades_for_symbol(
            update.symbol,
            statuses=(TradeStatus.ACTIVE,),
        )
        for trade in trades:
            runtime, previous_price = await self.trade_manager.update_runtime_with_price(
                trade,
                update.price,
            )
            position = self.trade_manager.get_position_for_trade(trade)
            market_context = self.trade_manager.get_market_context(trade)
            events = self.alert_engine.evaluate_trade(
                trade,
                current_price=update.price,
                runtime=runtime,
                previous_price=previous_price,
                position=position,
                market_context=market_context,
            )
            await self._dispatch_alerts(events)

    async def handle_user_event(self, payload: dict) -> None:
        assert self.trade_manager is not None
        assert self.alert_engine is not None

        event_type = payload.get("e")
        if event_type == "ACCOUNT_UPDATE":
            await self.trade_manager.apply_account_update(payload)
            await self._dispatch_alerts(
                self.alert_engine.evaluate_position_alignment(
                    self.trade_manager.get_declared_trades(),
                    self.trade_manager.get_open_positions(),
                )
            )
        elif event_type == "ORDER_TRADE_UPDATE":
            self.logger.info("ORDER_TRADE_UPDATE recibido para %s", payload.get("o", {}).get("s"))

    async def handle_stream_status(self, kind: str, payload: dict) -> None:
        assert self.alert_engine is not None

        mapping = {
            "market_ws_disconnected": (
                AlertPriority.CRITICAL,
                "WebSocket de mercado desconectado.",
            ),
            "user_ws_disconnected": (
                AlertPriority.CRITICAL,
                "User Data Stream desconectado.",
            ),
            "listenkey_renewed": (
                AlertPriority.INFO,
                "listenKey renovado correctamente.",
            ),
            "listenkey_keepalive_failed": (
                AlertPriority.WARNING,
                "Fallo en keepalive del listenKey.",
            ),
            "listenkey_expiring": (
                AlertPriority.WARNING,
                "listenKey próximo a expirar.",
            ),
            "listenkey_expired": (
                AlertPriority.CRITICAL,
                "listenKey expirado.",
            ),
        }
        priority, reason = mapping[kind]
        note_parts: list[str] = []
        if "retry_in_seconds" in payload:
            note_parts.append(f"Reintento en {payload['retry_in_seconds']}s")
        if "remaining_seconds" in payload:
            note_parts.append(f"Restan {payload['remaining_seconds']}s")
        if "error" in payload:
            note_parts.append(str(payload["error"]))

        await self._dispatch_alerts(
            [
                self.alert_engine.build_system_alert(
                    key=kind,
                    reason=reason,
                    priority=priority,
                    note=" | ".join(note_parts) or None,
                )
            ]
        )

    async def run(self) -> None:
        assert self.settings is not None
        assert self.trade_manager is not None
        assert self.binance_client is not None
        assert self.command_bot is not None

        await self._sync_positions()
        for trade in self.trade_manager.get_declared_trades(
            statuses=(TradeStatus.ACTIVE, TradeStatus.PAUSED)
        ):
            await self._refresh_trade_context(trade)

        self.market_stream = BinanceMarketStream(
            self.binance_client,
            self.trade_manager.get_monitored_symbols(),
            stream_suffix=self.settings.binance.market_stream_suffix,
            reconnect_backoff=self.settings.binance.reconnect_backoff_seconds,
            logger=self.logger,
        )

        self.tasks = [
            asyncio.create_task(self.market_stream.run(self.handle_price_update, self.handle_stream_status)),
            asyncio.create_task(self._context_refresh_loop()),
            asyncio.create_task(self.command_bot.run_polling()),
        ]
        if self.scanner is not None:
            self.tasks.append(asyncio.create_task(self.scanner.run()))
        if self.plan_monitor is not None:
            self.tasks.append(asyncio.create_task(self.plan_monitor.run()))

        if self.binance_private_account_sync:
            self.user_stream = BinanceUserDataStream(
                self.binance_client,
                keepalive_minutes=self.settings.binance.user_stream_keepalive_minutes,
                reconnect_backoff=self.settings.binance.reconnect_backoff_seconds,
                logger=self.logger,
            )
            self.tasks.append(
                asyncio.create_task(
                    self.user_stream.run(self.handle_user_event, self.handle_stream_status)
                )
            )

        await asyncio.gather(*self.tasks)

    async def build_plan_for_signal(
        self,
        *,
        symbol: str,
        quality_payload: dict,
        current_price: float,
        structure_m15: dict,
        source_alert_id: str | None = None,
        study_mode: bool = False,
    ) -> int | None:
        if not self.trade_planner_enabled:
            return None
        assert self.storage is not None
        plan = build_trade_plan(
            symbol=symbol,
            quality_payload=quality_payload,
            current_price=current_price,
            structure_m15=structure_m15,
            expire_minutes=self.plan_expire_minutes,
            min_rr_tp1=self.plan_min_rr_tp1,
            atr_buffer_mult=self.plan_atr_buffer_mult,
            use_fib=self.plan_use_fib,
            use_order_blocks=self.plan_use_order_blocks,
            study_mode=study_mode,
        )
        if plan is None:
            self.logger.info("plan skipped reason | symbol=%s signal_type=%s", symbol, quality_payload.get("signal_type"))
            return None
        payload = plan.__dict__.copy()
        payload["source_alert_id"] = source_alert_id
        plan_id = await self.storage.create_trade_plan(payload)
        self.logger.info("plan created | id=%s symbol=%s type=%s", plan_id, symbol, payload.get("signal_type"))
        return plan_id

    async def handle_scanner_signal_sent(self, event) -> None:
        if not self.trade_planner_enabled:
            return
        metadata = event.metadata or {}
        quality = metadata.get("quality_payload")
        structure = metadata.get("structure_m15")
        if not isinstance(quality, dict) or not isinstance(structure, dict):
            return
        plan_id = await self.build_plan_for_signal(
            symbol=event.symbol,
            quality_payload=quality,
            current_price=float(event.current_price or 0.0),
            structure_m15=structure,
            source_alert_id=event.key,
        )
        if plan_id is not None:
            await self._dispatch_alerts(
                [
                    self.alert_engine.build_system_alert(
                        key=f"plan_created_{event.symbol.lower()}_{plan_id}",
                        reason=f"Plan sugerido creado para {event.symbol} (#{plan_id}).",
                        priority=AlertPriority.INFO,
                        note=event.note,
                    )
                ]
            )


async def async_main() -> None:
    root_dir = Path(__file__).resolve().parents[1]
    bot = TradingAlertBot(root_dir)
    try:
        await bot.initialize()
        await bot.run()
    except (
        ConfigError,
        BinanceAPIError,
        TelegramNotificationError,
        TelegramCommandBotError,
    ) as exc:
        logging.getLogger("trading_alert_bot").error("Fallo fatal: %s", exc)
        raise
    finally:
        await bot.close()


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()

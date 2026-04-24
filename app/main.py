from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import time
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv

from app.alert_engine import AlertEngine
from app.binance_client import BinanceAPIError, BinanceFuturesClient
from app.binance_ws import BinanceMarketStream, BinanceUserDataStream
from app.config_loader import ConfigError, load_settings, load_trades
from app.logger import configure_logging
from app.models import AlertPriority, PriceUpdate, TradeConfig, TradeStatus
from app.scanner import SignalScanner
from app.storage import Storage, TERMINAL_TRADE_PLAN_STATUSES
from app.strategy_engine import build_market_context
from app.telegram_command_bot import TelegramCommandBot, TelegramCommandBotError
from app.telegram_notifier import TelegramNotificationError, TelegramNotifier
from app.plan_monitor import PlanMonitor
from app.single_instance import SingleInstanceError, SingleInstanceLock
from app.trade_planner import build_trade_plan, evaluate_plan_gate, trade_plan_to_dict
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
        self.scanner_alert_low_quality = False
        self.scanner_alert_momentum_chase = False
        self.scanner_min_quality = "MEDIA"
        self.scanner_require_adx_not_weak = True
        self.scanner_require_structure_confirmation = False
        self.scanner_block_dry_volume = True
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
        self.binance_guard_enabled = True
        self.binance_cache_enabled = True
        self.binance_backoff_enabled = True
        self.binance_rate_limit_pause_seconds = 60
        self.plan_min_cooldown_seconds = 900
        self.enable_direct_m15_plans = False
        self.armed_m15_ttl_candles = 2
        self.armed_m15_tf_seconds = 15 * 60
        self._plan_create_locks: dict[str, asyncio.Lock] = {}

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

    def _scanner_state(self) -> dict[str, dict[str, object]]:
        if self.scanner is None:
            return {}
        return self.scanner.get_last_scan_state()

    def _monitor_status(self) -> dict[str, object]:
        if self.plan_monitor is None:
            return {
                "enabled": False,
                "interval_seconds": self.plan_monitor_interval_seconds,
                "open_plans_count": 0,
                "events_sent": 0,
                "last_cycle_at": None,
                "price_source": "mark_price",
                "alerts_enabled": True,
                "last_error": "PLAN_MONITOR_ENABLED=false o no iniciado en main",
            }
        payload = self.plan_monitor.get_status()
        payload["enabled"] = bool(self.plan_monitor_enabled)
        return payload

    def _binance_status(self) -> dict[str, object]:
        if self.binance_client is None:
            return {"status": "DEGRADED", "last_error": "Binance client no disponible"}
        return self.binance_client.get_api_status()

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
        self.scanner_alert_low_quality = _env_flag("SCANNER_ALERT_LOW_QUALITY", False)
        self.scanner_alert_momentum_chase = _env_flag("SCANNER_ALERT_MOMENTUM_CHASE", False)
        self.scanner_min_quality = (os.getenv("SCANNER_MIN_QUALITY", "MEDIA").strip().upper() or "MEDIA")
        self.scanner_require_adx_not_weak = _env_flag("SCANNER_REQUIRE_ADX_NOT_WEAK", True)
        self.scanner_require_structure_confirmation = _env_flag("SCANNER_REQUIRE_STRUCTURE_CONFIRMATION", False)
        self.scanner_block_dry_volume = _env_flag("SCANNER_BLOCK_DRY_VOLUME", True)
        self.trade_planner_enabled = _env_flag("TRADE_PLANNER_ENABLED", True)
        self.plan_monitor_enabled = _env_flag("PLAN_MONITOR_ENABLED", True)
        self.plan_monitor_interval_seconds = _env_int("PLAN_MONITOR_INTERVAL_SECONDS", 15)
        self.plan_expire_minutes = _env_int("PLAN_EXPIRE_MINUTES", 120)
        self.plan_use_volume_profile = _env_flag("PLAN_USE_VOLUME_PROFILE", False)
        self.plan_use_order_blocks = _env_flag("PLAN_USE_ORDER_BLOCKS", True)
        self.plan_use_fib = _env_flag("PLAN_USE_FIB", True)
        self.plan_min_rr_tp1 = _env_float("PLAN_MIN_RR_TP1", 1.0)
        self.plan_atr_buffer_mult = _env_float("PLAN_ATR_BUFFER_MULT", 0.25)
        self.enable_direct_m15_plans = _env_flag("ENABLE_DIRECT_M15_PLANS", False)
        self.armed_m15_ttl_candles = max(1, _env_int("ARMED_M15_TTL_CANDLES", 2))
        self.binance_guard_enabled = _env_flag("BINANCE_GUARD_ENABLED", True)
        self.binance_cache_enabled = _env_flag("BINANCE_CACHE_ENABLED", True)
        self.binance_backoff_enabled = _env_flag("BINANCE_BACKOFF_ENABLED", True)
        self.binance_rate_limit_pause_seconds = _env_int("BINANCE_RATE_LIMIT_PAUSE_SECONDS", 60)

        self.binance_client = BinanceFuturesClient(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
            testnet=self.binance_testnet,
            recv_window=self.settings.binance.recv_window,
            timeout_seconds=self.settings.binance.rest_timeout_seconds,
            guard_enabled=self.binance_guard_enabled,
            cache_enabled=self.binance_cache_enabled,
            backoff_enabled=self.binance_backoff_enabled,
            rate_limit_pause_seconds=self.binance_rate_limit_pause_seconds,
            mark_price_cache_ttl=_env_int("MARK_PRICE_CACHE_TTL", 2),
            klines_1m_cache_ttl=_env_int("KLINES_1M_CACHE_TTL", 15),
            klines_3m_cache_ttl=_env_int("KLINES_3M_CACHE_TTL", 30),
            klines_5m_cache_ttl=_env_int("KLINES_5M_CACHE_TTL", 45),
            klines_15m_cache_ttl=_env_int("KLINES_15M_CACHE_TTL", 90),
            position_cache_ttl=_env_int("POSITION_CACHE_TTL", 10),
            exchange_info_cache_ttl=_env_int("EXCHANGE_INFO_CACHE_TTL", 3600),
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
            scanner_alert_low_quality=self.scanner_alert_low_quality,
            scanner_alert_momentum_chase=self.scanner_alert_momentum_chase,
            scanner_min_quality=self.scanner_min_quality,
            scanner_require_adx_not_weak=self.scanner_require_adx_not_weak,
            scanner_require_structure_confirmation=self.scanner_require_structure_confirmation,
            scanner_block_dry_volume=self.scanner_block_dry_volume,
            scanner_state_provider=self._scanner_state,
            monitor_status_provider=self._monitor_status,
            binance_status_provider=self._binance_status,
            plan_monitor_enabled=self.plan_monitor_enabled,
            plan_monitor_interval_seconds=self.plan_monitor_interval_seconds,
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
                alert_low_quality=self.scanner_alert_low_quality,
                alert_momentum_chase=self.scanner_alert_momentum_chase,
                min_quality=self.scanner_min_quality,
                require_adx_not_weak=self.scanner_require_adx_not_weak,
                require_structure_confirmation=self.scanner_require_structure_confirmation,
                block_dry_volume=self.scanner_block_dry_volume,
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
        plan_fingerprint: str | None = None,
        study_mode: bool = False,
    ) -> int | None:
        if not self.trade_planner_enabled:
            return None
        if not str(plan_fingerprint or "").strip():
            raise ValueError("missing_plan_fingerprint")
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
        payload = trade_plan_to_dict(plan)
        payload["source_alert_id"] = source_alert_id
        payload["plan_fingerprint"] = plan_fingerprint
        try:
            plan_id = await self.storage.create_trade_plan(payload)
        except sqlite3.IntegrityError:
            self.logger.info("plan duplicate suppressed on insert | symbol=%s fingerprint=%s", symbol, plan_fingerprint)
            return None
        self.logger.info("plan created | id=%s symbol=%s type=%s", plan_id, symbol, payload.get("signal_type"))
        return plan_id

    def _get_plan_lock(self, symbol: str, side: str) -> asyncio.Lock:
        key = f"{symbol.upper()}:{side.upper()}"
        lock = self._plan_create_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._plan_create_locks[key] = lock
        return lock

    def _armed_setup_key(self, symbol: str, side: str, signal_type: str, m15_candle_open_time: int) -> str:
        return f"armed_m15::{symbol.upper()}:{side.upper()}:{signal_type}:{m15_candle_open_time}"

    async def handle_scanner_signal_sent(self, event) -> None:
        metadata = event.metadata or {}
        quality = metadata.get("quality_payload")
        structure = metadata.get("structure_m15")
        source_tf = str(metadata.get("source_tf", metadata.get("trigger_tf", "unknown"))).lower()
        execution_tf = str(metadata.get("execution_tf", "15m")).lower()
        m15_candle_open_time = metadata.get("m15_candle_open_time")
        source_candle_closed = bool(metadata.get("source_candle_closed", metadata.get("is_closed_candle", False)))
        execution_candle_closed = bool(metadata.get("execution_candle_closed", metadata.get("is_closed_candle", False)))
        metadata["source_candle_closed"] = source_candle_closed
        metadata["execution_candle_closed"] = execution_candle_closed
        signal_type = str(metadata.get("signal_type", quality.get("signal_type") if isinstance(quality, dict) else "unknown"))
        side = str(metadata.get("direction", quality.get("result") if isinstance(quality, dict) else "unknown")).upper()
        symbol = event.symbol.upper()
        if not isinstance(quality, dict) or not isinstance(structure, dict):
            metadata["plan_created"] = False
            metadata["plan_block_reason"] = "datos insuficientes"
            return
        if self.storage is None:
            metadata["plan_created"] = False
            metadata["plan_block_reason"] = "storage_not_available"
            return
        if m15_candle_open_time is None:
            metadata["plan_created"] = False
            metadata["plan_block_reason"] = "missing_plan_fingerprint"
            return
        try:
            m15_open_int = int(m15_candle_open_time)
        except (TypeError, ValueError):
            metadata["plan_created"] = False
            metadata["plan_block_reason"] = "missing_plan_fingerprint"
            return

        lock = self._get_plan_lock(symbol, side)
        async with lock:
            fingerprint = f"{symbol}:{side}:{signal_type}:{execution_tf}:{m15_open_int}"
            metadata["plan_fingerprint"] = fingerprint
            metadata["duplicate_suppressed"] = False

            if execution_tf != "15m":
                metadata["plan_created"] = False
                metadata["plan_block_reason"] = f"execution_tf_not_allowed:{execution_tf}"
                return
            if not execution_candle_closed:
                metadata["plan_created"] = False
                metadata["plan_block_reason"] = "m15_open_candle"
                return

            arm_key = self._armed_setup_key(symbol, side, signal_type, m15_open_int)
            armed_created_at = m15_open_int / 1000.0
            armed_expires_at = armed_created_at + (self.armed_m15_ttl_candles * self.armed_m15_tf_seconds)
            now_ts = time.time()

            if source_tf == "15m":
                await self.storage.record_scanner_alert_state(
                    key=arm_key,
                    symbol=symbol,
                    direction=side,
                    level="ARMED_M15_SETUP",
                    trigger_tf=source_tf,
                    closed_candle_time=str(m15_open_int),
                )
                self.logger.info(
                    "armed setup state | armed_key=%s armed_created_at=%s armed_expires_at=%s now=%s armed_status=%s reason=%s",
                    arm_key,
                    armed_created_at,
                    armed_expires_at,
                    now_ts,
                    "ARMED",
                    "m15_setup_created",
                )
                if not self.enable_direct_m15_plans:
                    self.logger.info(
                        "m15 setup-only armed | reason=%s action=%s symbol=%s side=%s signal_type=%s source_tf=%s execution_tf=%s m15_candle_open_time=%s armed_key=%s armed_expires_at=%s",
                        "armed_m15_setup_only",
                        "armed_setup",
                        symbol,
                        side,
                        signal_type,
                        source_tf,
                        execution_tf,
                        m15_open_int,
                        arm_key,
                        armed_expires_at,
                    )
                    metadata["plan_created"] = False
                    metadata["plan_block_reason"] = "armed_m15_setup_only"
                    return
            else:
                if not source_candle_closed:
                    metadata["plan_created"] = False
                    metadata["plan_block_reason"] = "source_candle_not_closed"
                    return
                if not await self.storage.has_scanner_alert_state(arm_key):
                    metadata["plan_created"] = False
                    metadata["plan_block_reason"] = "missing_armed_m15_setup"
                    return
                if now_ts > armed_expires_at:
                    await self.storage.delete_scanner_alert_state(arm_key)
                    self.logger.info(
                        "armed setup state | armed_key=%s armed_created_at=%s armed_expires_at=%s now=%s armed_status=%s reason=%s",
                        arm_key,
                        armed_created_at,
                        armed_expires_at,
                        now_ts,
                        "EXPIRED",
                        "armed_m15_expired",
                    )
                    metadata["plan_created"] = False
                    metadata["plan_block_reason"] = "armed_m15_expired"
                    return

            if await self.storage.has_scanner_alert_state(f"planfp::{fingerprint}"):
                metadata["plan_created"] = False
                metadata["duplicate_suppressed"] = True
                metadata["plan_block_reason"] = "duplicate_fingerprint"
                return

            if await self.storage.active_plan_exists(symbol, side):
                metadata["plan_created"] = False
                metadata["plan_block_reason"] = "active_plan_exists"
                return

            all_plans = await self.storage.list_trade_plans()
            same_side_plans = [
                plan
                for plan in all_plans
                if str(plan.get("symbol", "")).upper() == symbol and str(plan.get("direction", "")).upper() == side
            ]
            cooldown_allowed_statuses = set(TERMINAL_TRADE_PLAN_STATUSES)
            if same_side_plans:
                latest = same_side_plans[0]
                latest_status = str(latest.get("status", "")).upper()
                created_at_raw = str(latest.get("created_at", ""))
                try:
                    created_at = datetime.fromisoformat(created_at_raw)
                    if created_at.tzinfo is None:
                        created_at = created_at.replace(tzinfo=timezone.utc)
                    elapsed = (datetime.now(tz=timezone.utc) - created_at).total_seconds()
                except ValueError:
                    elapsed = self.plan_min_cooldown_seconds + 1
                if elapsed < self.plan_min_cooldown_seconds and latest_status not in cooldown_allowed_statuses:
                    metadata["plan_created"] = False
                    metadata["plan_block_reason"] = "cooldown_active"
                    return

            allowed, reason = evaluate_plan_gate(
                planner_enabled=self.trade_planner_enabled,
                quality_payload=quality,
                structure_m15=structure,
            )
            metadata["auto_plan_allowed"] = allowed
            metadata["plan_block_reason"] = reason
            metadata["plan_created"] = False
            if not allowed:
                return

            try:
                plan_id = await self.build_plan_for_signal(
                    symbol=event.symbol,
                    quality_payload=quality,
                    current_price=float(event.current_price or 0.0),
                    structure_m15=structure,
                    source_alert_id=event.key,
                    plan_fingerprint=fingerprint,
                )
            except ValueError:
                metadata["plan_created"] = False
                metadata["plan_block_reason"] = "missing_plan_fingerprint"
                return
            if plan_id is not None:
                metadata["plan_created"] = True
                metadata["plan_id"] = plan_id
                metadata["plan_block_reason"] = "OK"
                await self.storage.record_scanner_alert_state(
                    key=f"planfp::{fingerprint}",
                    symbol=symbol,
                    direction=side,
                    level="PLAN_FP",
                    trigger_tf=source_tf,
                    closed_candle_time=str(m15_open_int),
                )
                await self.storage.delete_scanner_alert_state(arm_key)
                if self.storage is not None:
                    plan = await self.storage.get_trade_plan(plan_id)
                    if plan is not None:
                        metadata["plan_payload"] = {
                            "entry_low": float(plan["entry_low"]),
                            "entry_high": float(plan["entry_high"]),
                            "stop_loss": float(plan["stop_loss"]),
                            "tp1": float(plan["tp1"]),
                            "tp2": float(plan["tp2"]),
                            "tp3": float(plan["tp3"]),
                            "rr_tp1": float(plan["rr_tp1"]),
                            "rr_tp2": float(plan["rr_tp2"]),
                            "rr_tp3": float(plan["rr_tp3"]),
                        }
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
                return

            if await self.storage.has_trade_plan_fingerprint(fingerprint):
                metadata["plan_block_reason"] = "duplicate_fingerprint"
                metadata["duplicate_suppressed"] = True
            else:
                metadata["plan_block_reason"] = "RR insuficiente / datos insuficientes / precio lejos de entrada ideal"


async def async_main() -> None:
    root_dir = Path(__file__).resolve().parents[1]
    lock = SingleInstanceLock(root_dir / "runtime" / "bot.lock")
    logger = logging.getLogger("trading_alert_bot")
    try:
        lock.acquire()
        if lock.last_stale_pid is not None:
            logger.warning("stale lock removed pid=%s", lock.last_stale_pid)
        logger.info("single instance lock acquired pid=%s", os.getpid())
    except SingleInstanceError as exc:
        logger.error("%s. Abortando nueva instancia.", exc)
        return

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
        lock.release()
        logger.info("single instance lock released")


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

import httpx

from app.binance_client import BinanceAPIError, BinanceFuturesClient
from app.models import (
    AlertEngineSettings,
    InvalidationZone,
    TradeAlertConfig,
    TradeConfig,
    TradeSide,
    TradeStatus,
    TradeStrategyConfig,
)
from app.strategy_engine import (
    analyze_adx_dmi,
    analyze_ema_signal,
    analyze_koncorde_lite,
    analyze_macd,
    analyze_sqzmom,
    evaluate_signal_quality,
)
from app.trade_manager import TradeManager

MessageSender = Callable[[str, str], Awaitable[None]]
StatusProvider = Callable[[], dict[str, Any]]
TradeChangeCallback = Callable[[str | None], Awaitable[None]]
CurrentPriceProvider = Callable[[str], Awaitable[tuple[float, str, int | None] | None]]

SYMBOL_PATTERN = re.compile(r"^[A-Z0-9]{2,20}$")
TIMEFRAME_PATTERN = re.compile(r"^\d+[mhdwM]$", re.IGNORECASE)
WATCHLIST_TIMEFRAMES = ("M1", "M3", "M5", "M15")
WATCHLIST_INDICATORS = (
    "ADX/DMI",
    "Squeeze Momentum",
    "MACD",
    "EMA55/EMA200 + cruces",
    "Koncorde Lite M15",
)
WATCHLIST_ALERT_RULES = (
    "Sync 3/4: cierre M5",
    "Full 4/4: cierre M1",
    "Cruce EMA M15: cierre M15",
)


@dataclass(slots=True)
class ChatSession:
    mode: str
    step: str
    data: dict[str, Any] = field(default_factory=dict)


class TelegramCommandBotError(RuntimeError):
    pass


class TelegramCommandBot:
    def __init__(
        self,
        *,
        bot_token: str,
        authorized_chat_id: str,
        trade_manager: TradeManager,
        alert_settings: AlertEngineSettings,
        logger: logging.Logger,
        status_provider: StatusProvider,
        binance_client: BinanceFuturesClient | None = None,
        current_price_provider: CurrentPriceProvider | None = None,
        on_trade_changed: TradeChangeCallback | None = None,
        timeout_seconds: float = 15.0,
        polling_timeout_seconds: int = 20,
        sender: MessageSender | None = None,
    ) -> None:
        if not bot_token:
            raise TelegramCommandBotError("TELEGRAM_BOT_TOKEN no configurado.")
        if not authorized_chat_id:
            raise TelegramCommandBotError("TELEGRAM_CHAT_ID no configurado.")

        self._authorized_chat_id = str(authorized_chat_id)
        self._trade_manager = trade_manager
        self._alert_settings = alert_settings
        self._logger = logger
        self._status_provider = status_provider
        self._binance_client = binance_client
        self._current_price_provider = current_price_provider
        self._on_trade_changed = on_trade_changed
        self._polling_timeout_seconds = polling_timeout_seconds
        self._sender_override = sender
        self._offset: int | None = None
        self._sessions: dict[str, ChatSession] = {}
        self._stop_event = asyncio.Event()
        self._client = (
            None
            if sender is not None
            else httpx.AsyncClient(
                base_url=f"https://api.telegram.org/bot{bot_token}",
                timeout=httpx.Timeout(timeout_seconds),
            )
        )

    async def close(self) -> None:
        self._stop_event.set()
        if self._client is not None:
            await self._client.aclose()

    async def run_polling(self) -> None:
        while not self._stop_event.is_set():
            try:
                updates = await self._fetch_updates()
                for update in updates:
                    self._offset = int(update["update_id"]) + 1
                    await self.handle_update(update)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.error("Error en polling de Telegram: %s", exc)
                await asyncio.sleep(3)

    async def _fetch_updates(self) -> list[dict[str, Any]]:
        if self._client is None:
            return []
        response = await self._client.get(
            "/getUpdates",
            params={
                "timeout": self._polling_timeout_seconds,
                "offset": self._offset,
                "allowed_updates": '["message"]',
            },
        )
        data = response.json()
        if response.status_code >= 400 or not data.get("ok", False):
            raise TelegramCommandBotError(
                data.get("description", f"Telegram respondio con {response.status_code}")
            )
        return list(data.get("result", []))

    async def _send_message(self, chat_id: str, text: str) -> None:
        if self._sender_override is not None:
            await self._sender_override(str(chat_id), text)
            return
        assert self._client is not None
        response = await self._client.post(
            "/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )
        data = response.json()
        if response.status_code >= 400 or not data.get("ok", False):
            raise TelegramCommandBotError(
                data.get("description", f"Telegram respondio con {response.status_code}")
            )

    async def _notify_trade_changed(self, symbol: str | None) -> None:
        if self._on_trade_changed is not None:
            await self._on_trade_changed(symbol)

    @staticmethod
    def _parse_symbol(text: str) -> str:
        symbol = text.strip().upper()
        if not SYMBOL_PATTERN.fullmatch(symbol):
            raise ValueError("El simbolo debe ser alfanumerico, por ejemplo BTCUSDT.")
        return symbol

    @classmethod
    def _normalize_futures_symbol(cls, text: str) -> str:
        symbol = cls._parse_symbol(text)
        if symbol.endswith("USDT"):
            return symbol
        return f"{symbol}USDT"

    @staticmethod
    def _parse_side(text: str) -> TradeSide:
        value = text.strip().upper()
        if value not in {"LONG", "SHORT"}:
            raise ValueError("El side debe ser LONG o SHORT.")
        return TradeSide(value)

    @staticmethod
    def _parse_positive_float(text: str, field_name: str) -> float:
        try:
            value = float(text.strip())
        except ValueError as exc:
            raise ValueError(f"{field_name} debe ser numerico.") from exc
        if value <= 0:
            raise ValueError(f"{field_name} debe ser mayor que cero.")
        return value

    @staticmethod
    def _parse_positive_int(text: str, field_name: str) -> int:
        try:
            value = int(text.strip())
        except ValueError as exc:
            raise ValueError(f"{field_name} debe ser entero.") from exc
        if value <= 0:
            raise ValueError(f"{field_name} debe ser mayor que cero.")
        return value

    @classmethod
    def _parse_take_profits(cls, text: str) -> list[float]:
        parts = [part.strip() for part in text.split(",") if part.strip()]
        if not parts:
            raise ValueError("Debes indicar al menos un take profit.")
        return [cls._parse_positive_float(part, "take_profit") for part in parts]

    @classmethod
    def _parse_invalidation_zone(cls, text: str) -> InvalidationZone | None:
        raw = text.strip()
        if raw.lower() in {"skip", "-", "none"}:
            return None
        parts = [part for part in raw.replace(",", " ").split() if part]
        if len(parts) != 2:
            raise ValueError("La invalidation zone debe ser 'MIN MAX' o 'skip'.")
        min_price = cls._parse_positive_float(parts[0], "invalidation min")
        max_price = cls._parse_positive_float(parts[1], "invalidation max")
        if min_price > max_price:
            raise ValueError("El minimo de invalidacion no puede ser mayor que el maximo.")
        return InvalidationZone(min=min_price, max=max_price)

    @staticmethod
    def _parse_timeframe(text: str) -> str:
        value = text.strip()
        if not TIMEFRAME_PATTERN.fullmatch(value):
            raise ValueError("El timeframe debe tener formato como 1m, 5m, 15m o 1h.")
        return value

    @staticmethod
    def _normalize_note(text: str) -> str:
        value = text.strip()
        if value.lower() in {"", "-", "skip", "none"}:
            return ""
        return value

    @staticmethod
    def _format_trade(trade: TradeConfig) -> str:
        tps = ", ".join(f"{price:.6f}" for price in trade.take_profits) if trade.take_profits else "-"
        return (
            f"{trade.symbol} | {trade.side.value} | entry {trade.entry:.6f} | "
            f"SL {trade.stop_loss:.6f} | TPs {tps} | status {trade.status.value}"
        )

    @staticmethod
    def _format_timestamp(event_time_ms: int | None) -> str:
        if event_time_ms is None:
            return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S")
        return (
            datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc)
            .astimezone()
            .strftime("%Y-%m-%d %H:%M:%S")
        )

    def _default_render_symbol(self) -> str:
        active_trades = self._trade_manager.get_declared_trades(statuses=(TradeStatus.ACTIVE,))
        if active_trades:
            return active_trades[0].symbol
        return "BTCUSDT"

    async def _get_price_snapshot(self, symbol: str) -> tuple[float, str, int | None]:
        if self._current_price_provider is not None:
            cached = await self._current_price_provider(symbol)
            if cached is not None:
                return cached

        if self._binance_client is None:
            raise BinanceAPIError("Cliente de Binance no disponible.")

        mark_price, event_time = await self._binance_client.get_mark_price(symbol)
        return mark_price, "Binance Futures mark price (REST)", event_time

    async def handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message") or {}
        chat = message.get("chat") or {}
        chat_id = str(chat.get("id", ""))
        text = str(message.get("text", "") or "").strip()
        if not chat_id or not text:
            return

        if chat_id != self._authorized_chat_id:
            await self._send_message(chat_id, "Unauthorized")
            return

        self._logger.info("Telegram command recibido: %s", text[:200])
        session = self._sessions.get(chat_id)

        if text == "/cancel":
            if session is None:
                await self._send_message(chat_id, "No hay flujo activo para cancelar.")
            else:
                self._sessions.pop(chat_id, None)
                await self._send_message(chat_id, "Flujo cancelado.")
            return

        if session is not None and text.startswith("/"):
            await self._send_message(
                chat_id,
                "Hay un flujo activo. Responde al flujo actual o usa /cancel.",
            )
            return

        if session is not None:
            await self._handle_session_message(chat_id, text, session)
            return

        if not text.startswith("/"):
            await self._send_message(chat_id, "Comando no reconocido. Usa /help.")
            return

        command, *rest = text.split(maxsplit=1)
        command = command.split("@", maxsplit=1)[0]
        args = rest[0] if rest else ""

        handlers = {
            "/start": self._handle_start,
            "/help": self._handle_help,
            "/comandos": self._handle_help,
            "/status": self._handle_status,
            "/trades": self._handle_trades,
            "/watchlist": self._handle_watchlist,
            "/addtrade": self._handle_addtrade,
            "/add": self._handle_add,
            "/close": self._handle_close,
            "/delete": self._handle_delete,
            "/pause": self._handle_pause,
            "/remove": self._handle_remove,
            "/resume": self._handle_resume,
            "/setsl": self._handle_setsl,
            "/addtp": self._handle_addtp,
            "/settp": self._handle_settp,
            "/setinvalidation": self._handle_setinvalidation,
            "/setentry": self._handle_setentry,
            "/note": self._handle_note,
            "/consulta": self._handle_consulta,
            "/renderizar": self._handle_renderizar,
            "/signal": self._handle_signal,
        }
        handler = handlers.get(command)
        if handler is None:
            await self._send_message(chat_id, "Comando no reconocido. Usa /help.")
            return
        try:
            await handler(chat_id, args)
        except ValueError as exc:
            await self._send_message(chat_id, str(exc))
        except Exception as exc:
            self._logger.exception("Fallo inesperado manejando %s", command)
            await self._send_message(chat_id, f"No pude procesar {command}. Motivo: {exc}")

    async def _handle_start(self, chat_id: str, _: str) -> None:
        status = self._status_provider()
        await self._send_message(
            chat_id,
            (
                "Trading alert bot operativo.\n"
                f"DRY_RUN={status['DRY_RUN']} | TESTNET={status['BINANCE_TESTNET']} | "
                f"trades activos={status['active_trades']}\n"
                "Usa /help para ver comandos."
            ),
        )

    async def _handle_help(self, chat_id: str, _: str) -> None:
        status = self._status_provider()
        await self._send_message(
            chat_id,
            "\n".join(
                [
                    "🤖 Bot Trading — Comandos",
                    "",
                    "📌 Consulta:",
                    "/consulta precio BTC",
                    "/renderizar BTCUSDT 1m",
                    "/signal BTC",
                    "",
                    "📡 Watchlist:",
                    "/watchlist",
                    "/add watchlist BTC",
                    "/remove watchlist BTC",
                    "",
                    "🧠 Señales:",
                    "El BE vigila:",
                    "- M1",
                    "- M3",
                    "- M5",
                    "- M15",
                    "",
                    "Indicadores:",
                    "- ADX/DMI",
                    "- Squeeze Momentum",
                    "- MACD",
                    "- EMA55/EMA200 + cruces",
                    "- Koncorde Lite M15",
                    "",
                    "Regla sync:",
                    "- Señal válida: M15 + M5 + M3 alineadas",
                    "- Full sync: M15 + M5 + M3 + M1",
                    "- M15 manda dirección",
                    "- Koncorde Lite solo filtra/suma score",
                    "",
                    "⚠️ Seguridad:",
                    "- No abre órdenes",
                    "- No cierra órdenes",
                    "- Solo alertas",
                    f"- DRY_RUN {'activo' if str(status.get('DRY_RUN', 'true')).lower() == 'true' else 'inactivo'}",
                ]
            ),
        )

    async def _handle_status(self, chat_id: str, _: str) -> None:
        status = self._status_provider()
        await self._send_message(
            chat_id,
            "\n".join(
                [
                    "Estado del bot:",
                    f"DRY_RUN={status['DRY_RUN']}",
                    f"BINANCE_TESTNET={status['BINANCE_TESTNET']}",
                    f"BINANCE_PRIVATE_ACCOUNT_SYNC={status['BINANCE_PRIVATE_ACCOUNT_SYNC']}",
                    f"BINANCE_MARKET_SOURCE={status['BINANCE_MARKET_SOURCE']}",
                    f"SCANNER_ENABLED={status.get('SCANNER_ENABLED', 'false')}",
                    f"SCANNER_ALERTS_ENABLED={status.get('SCANNER_ALERTS_ENABLED', 'false')}",
                    f"SCANNER_INTERVAL_SECONDS={status.get('SCANNER_INTERVAL_SECONDS', 60)}",
                    f"Trades activos={status['active_trades']}",
                ]
            ),
        )

    async def _handle_trades(self, chat_id: str, _: str) -> None:
        trades = self._trade_manager.get_declared_trades(
            statuses=(TradeStatus.ACTIVE, TradeStatus.PAUSED)
        )
        if not trades:
            await self._send_message(chat_id, "No hay trades activos o pausados.")
            return
        await self._send_message(
            chat_id,
            "\n".join(["Trades monitoreados:"] + [self._format_trade(trade) for trade in trades]),
        )

    async def _handle_watchlist(self, chat_id: str, _: str) -> None:
        status = self._status_provider()
        symbols = self._trade_manager.get_watchlist_symbols()
        if not symbols:
            await self._send_message(
                chat_id,
                "\n".join(
                    [
                        "📡 Watchlist BE vacía",
                        "Usá:",
                        "/add watchlist BTC",
                    ]
                ),
            )
            return

        lines = [
            "📡 Watchlist BE",
            "",
            f"Scanner: {'ON' if str(status.get('SCANNER_ENABLED', 'false')).lower() == 'true' else 'OFF'}",
            f"Alertas: {'ON' if str(status.get('SCANNER_ALERTS_ENABLED', 'false')).lower() == 'true' else 'OFF'}",
            f"Intervalo: {status.get('SCANNER_INTERVAL_SECONDS', 60)}s",
            "Pares vigilados:",
        ]
        lines.extend(f"{index}. {symbol} ✅" for index, symbol in enumerate(symbols, start=1))
        lines.extend(
            [
                "",
                "TF:",
                ", ".join(WATCHLIST_TIMEFRAMES),
                "",
                "Indicadores:",
            ]
        )
        lines.extend(f"- {item}" for item in WATCHLIST_INDICATORS)
        lines.extend(["", "Alertas:"])
        lines.extend(f"- {item}" for item in WATCHLIST_ALERT_RULES)
        await self._send_message(chat_id, "\n".join(lines))

    async def _handle_addtrade(self, chat_id: str, _: str) -> None:
        self._sessions[chat_id] = ChatSession(mode="addtrade", step="symbol")
        await self._send_message(chat_id, "Paso 1/9: envia el symbol, por ejemplo WLDUSDT.")

    async def _handle_add(self, chat_id: str, args: str) -> None:
        parts = args.split()
        if len(parts) != 2 or parts[0].lower() != "watchlist":
            await self._send_message(
                chat_id,
                "Uso:\n/add watchlist BTC\n/add watchlist ETHUSDT",
            )
            return
        symbol = self._normalize_futures_symbol(parts[1])
        await self._trade_manager.add_watchlist_symbol(symbol)
        await self._notify_trade_changed(None)
        self._logger.info("Watchlist agregada: %s", symbol)
        await self._send_message(chat_id, f"{symbol} agregado a watchlist.")

    async def _handle_remove(self, chat_id: str, args: str) -> None:
        parts = args.split()
        if len(parts) != 2 or parts[0].lower() != "watchlist":
            await self._send_message(chat_id, "Uso:\n/remove watchlist BTC")
            return
        symbol = self._normalize_futures_symbol(parts[1])
        symbols = await self._trade_manager.remove_watchlist_symbol(symbol)
        await self._notify_trade_changed(None)
        self._logger.info("Watchlist removida: %s", symbol)
        if symbols:
            await self._send_message(chat_id, f"{symbol} removido de watchlist.")
            return
        await self._send_message(chat_id, f"{symbol} removido. Watchlist vacía.")

    async def _handle_consulta(self, chat_id: str, args: str) -> None:
        stripped = args.strip()
        if not stripped:
            await self._send_message(chat_id, "Uso: /consulta precio BTC")
            return

        parts = stripped.split()
        if len(parts) != 2 or parts[0].lower() != "precio":
            await self._send_message(chat_id, "Consulta no reconocida. Proba: /consulta precio BTC")
            return

        symbol = self._normalize_futures_symbol(parts[1])
        self._logger.info("Consulta de precio solicitada para %s", symbol)
        try:
            price, source, event_time = await self._get_price_snapshot(symbol)
        except Exception as exc:
            self._logger.warning("No se pudo consultar el precio de %s: %s", symbol, exc)
            await self._send_message(
                chat_id,
                f"No pude consultar el precio de {symbol}.\nMotivo: {exc}",
            )
            return

        await self._send_message(
            chat_id,
            "\n".join(
                [
                    symbol,
                    f"Precio actual: {price:,.2f} USDT",
                    f"Fuente: {source}",
                    f"Hora: {self._format_timestamp(event_time)}",
                ]
            ),
        )

    async def _handle_renderizar(self, chat_id: str, args: str) -> None:
        raw = args.strip()
        symbol = self._default_render_symbol()
        interval = "1m"

        if raw:
            parts = raw.split()
            if len(parts) == 1:
                symbol = self._normalize_futures_symbol(parts[0])
            elif len(parts) == 2:
                symbol = self._normalize_futures_symbol(parts[0])
                interval = self._parse_timeframe(parts[1])
            else:
                raise ValueError("Uso: /renderizar BTCUSDT 1m")

        if self._binance_client is None:
            await self._send_message(
                chat_id,
                "Renderizado posible: NO\nMotivo: cliente de Binance no disponible.",
            )
            return

        self._logger.info("Diagnostico de renderizado solicitado para %s %s", symbol, interval)
        try:
            candles = await self._binance_client.get_klines(symbol, interval=interval, limit=50)
        except Exception as exc:
            self._logger.warning("No se pudo obtener klines para %s %s: %s", symbol, interval, exc)
            await self._send_message(
                chat_id,
                "\n".join(
                    [
                        "Renderizado posible: NO",
                        "Motivo: no se pudo obtener OHLCV / klines desde el backend.",
                        f"Falta implementar o corregir: {exc}",
                    ]
                ),
            )
            return

        if not candles:
            await self._send_message(
                chat_id,
                "\n".join(
                    [
                        "Renderizado posible: NO",
                        "Motivo: no se pudo obtener OHLCV / klines desde el backend.",
                        "Falta implementar o corregir: Binance devolvio 0 velas.",
                    ]
                ),
            )
            return

        first_candle = candles[0]
        fields = []
        for field_name in ("open_time", "open", "high", "low", "close", "volume"):
            if hasattr(first_candle, field_name):
                fields.append(field_name)

        if len(fields) < 6:
            await self._send_message(
                chat_id,
                "\n".join(
                    [
                        "Renderizado posible: NO",
                        "Motivo: no se pudo obtener OHLCV / klines desde el backend.",
                        f"Falta implementar o corregir: campos incompletos ({', '.join(fields)}).",
                    ]
                ),
            )
            return

        await self._send_message(
            chat_id,
            "\n".join(
                [
                    "Renderizado posible: SI",
                    f"Simbolo: {symbol}",
                    f"Intervalo: {interval}",
                    f"Velas disponibles: {len(candles)}",
                    f"Campos detectados: {', '.join(fields)}",
                    "Conclusion: el FE podria renderizar velas con esta informacion.",
                ]
            ),
        )

    async def _handle_signal(self, chat_id: str, args: str) -> None:
        raw = args.strip()
        if not raw:
            raise ValueError("Uso:\n/signal BTC\n/signal ETHUSDT")
        if self._binance_client is None:
            await self._send_message(
                chat_id,
                "No pude calcular /signal. Motivo: cliente de Binance no disponible.",
            )
            return

        symbol = self._normalize_futures_symbol(raw.split()[0])
        self._logger.info("Signal solicitado para %s", symbol)
        lines = [f"{symbol} — Señal", ""]
        try:
            tf_icons = {"LONG": "🟢", "SHORT": "🔴", "NONE": "⚪"}
            tf_directions: dict[str, str] = {}
            m15_result = None
            koncorde = None
            adx = None
            macd = None
            sqz = None
            koncorde_line = ""
            for interval, label in (("15m", "M15"), ("5m", "M5 "), ("3m", "M3 "), ("1m", "M1 ")):
                candles = await self._binance_client.get_klines(symbol, interval=interval, limit=210)
                result = analyze_ema_signal(candles, fast_period=55, slow_period=200)
                direction = "NONE"
                if result["trend"] == "BULL":
                    direction = "LONG"
                elif result["trend"] == "BEAR":
                    direction = "SHORT"
                tf_directions[interval] = direction
                if interval == "15m":
                    m15_result = result
                    koncorde = analyze_koncorde_lite(candles)
                    adx = analyze_adx_dmi(candles)
                    macd = analyze_macd([candle.close for candle in candles[:-1]])
                    sqz = analyze_sqzmom(candles)
                    koncorde_line = (
                        f"{koncorde['icon']} {koncorde['text']} | "
                        f"RSI {koncorde['rsi']:.1f} | Vol {koncorde['volume_ratio']:.2f}x"
                    )
            assert m15_result is not None and koncorde is not None and adx is not None and macd is not None and sqz is not None
            quality = evaluate_signal_quality(
                tf_directions=tf_directions,
                m15_ema=m15_result,
                koncorde_m15=koncorde,
                adx_m15=adx,
                macd_m15=macd,
                sqzmom_m15=sqz,
            )
        except Exception as exc:
            self._logger.warning("No se pudo calcular /signal para %s: %s", symbol, exc)
            await self._send_message(chat_id, f"No pude calcular /signal para {symbol}. Motivo: {exc}")
            return

        result_icon = "⚪"
        if quality["result"] == "LONG":
            result_icon = "🟢"
        elif quality["result"] == "SHORT":
            result_icon = "🔴"
        cross = "none"
        if m15_result["cross"] == "bull_cross":
            cross = "🟢 bull"
        elif m15_result["cross"] == "bear_cross":
            cross = "🔴 bear"

        lines.extend(
            [
                f"Resultado: {result_icon} {quality['result']}",
                "",
                "Sync:",
                f"M15 {tf_icons[tf_directions['15m']]}",
                f"M5  {tf_icons[tf_directions['5m']]}",
                f"M3  {tf_icons[tf_directions['3m']]}",
                f"M1  {tf_icons[tf_directions['1m']]}",
                "",
                "MAs M15:",
                f"{m15_result['icon']} {m15_result['relation']} | cruce {cross}",
                "",
                "Koncorde Lite:",
                koncorde_line,
                "",
                "Calidad:",
                f"🎯 {quality['quality']} | score {quality['score']}",
            ]
        )
        await self._send_message(chat_id, "\n".join(lines))

    async def _handle_close(self, chat_id: str, args: str) -> None:
        symbol = self._parse_symbol_argument(args)
        trade = await self._trade_manager.close_trade(symbol)
        await self._notify_trade_changed(symbol)
        await self._send_message(chat_id, f"Trade {trade.symbol} marcado como CLOSED.")

    async def _handle_delete(self, chat_id: str, args: str) -> None:
        symbol = self._parse_symbol_argument(args)
        trade = self._trade_manager.get_trade_by_symbol(
            symbol,
            statuses=(TradeStatus.ACTIVE, TradeStatus.PAUSED, TradeStatus.CLOSED),
        )
        if trade is None:
            raise ValueError(f"No se encontro trade para {symbol}.")
        self._sessions[chat_id] = ChatSession(
            mode="delete_confirm",
            step="confirm",
            data={"symbol": trade.symbol},
        )
        await self._send_message(
            chat_id,
            f"Vas a desactivar {trade.symbol}. Confirma con yes/no.",
        )

    async def _handle_pause(self, chat_id: str, args: str) -> None:
        symbol = self._parse_symbol_argument(args)
        trade = await self._trade_manager.pause_trade(symbol)
        await self._notify_trade_changed(symbol)
        await self._send_message(chat_id, f"Trade {trade.symbol} pausado.")

    async def _handle_resume(self, chat_id: str, args: str) -> None:
        symbol = self._parse_symbol_argument(args)
        trade = await self._trade_manager.resume_trade(symbol)
        await self._notify_trade_changed(symbol)
        await self._send_message(chat_id, f"Trade {trade.symbol} reactivado.")

    async def _handle_setsl(self, chat_id: str, args: str) -> None:
        symbol, price = self._parse_symbol_and_price(args, "stop_loss")
        trade = await self._trade_manager.set_trade_stop_loss(symbol, price)
        await self._notify_trade_changed(symbol)
        await self._send_message(chat_id, f"SL actualizado para {trade.symbol}: {trade.stop_loss:.6f}")

    async def _handle_addtp(self, chat_id: str, args: str) -> None:
        symbol, price = self._parse_symbol_and_price(args, "take_profit")
        trade = await self._trade_manager.add_trade_take_profit(symbol, price)
        await self._notify_trade_changed(symbol)
        await self._send_message(chat_id, f"TP agregado para {trade.symbol}: {price:.6f}")

    async def _handle_settp(self, chat_id: str, args: str) -> None:
        symbol, raw_prices = self._split_args(args, expected=2)
        prices = self._parse_take_profits(raw_prices)
        normalized_symbol = self._parse_symbol(symbol)
        trade = await self._trade_manager.set_trade_take_profits(normalized_symbol, prices)
        await self._notify_trade_changed(normalized_symbol)
        await self._send_message(chat_id, f"TPs reemplazados para {trade.symbol}.")

    async def _handle_setinvalidation(self, chat_id: str, args: str) -> None:
        parts = [part for part in args.split() if part]
        if len(parts) != 3:
            raise ValueError("Uso: /setinvalidation SYMBOL MIN MAX")
        symbol = self._parse_symbol(parts[0])
        zone = InvalidationZone(
            min=self._parse_positive_float(parts[1], "invalidation min"),
            max=self._parse_positive_float(parts[2], "invalidation max"),
        )
        if zone.min > zone.max:
            raise ValueError("El minimo de invalidacion no puede ser mayor que el maximo.")
        trade = await self._trade_manager.set_trade_invalidation(symbol, zone)
        await self._notify_trade_changed(symbol)
        await self._send_message(
            chat_id,
            f"Zona de invalidacion actualizada para {trade.symbol}: {zone.min:.6f} - {zone.max:.6f}",
        )

    async def _handle_setentry(self, chat_id: str, args: str) -> None:
        symbol, price = self._parse_symbol_and_price(args, "entry")
        trade = await self._trade_manager.set_trade_entry(symbol, price)
        await self._notify_trade_changed(symbol)
        await self._send_message(chat_id, f"Entry actualizada para {trade.symbol}: {trade.entry:.6f}")

    async def _handle_note(self, chat_id: str, args: str) -> None:
        parts = args.split(maxsplit=1)
        if len(parts) != 2:
            raise ValueError("Uso: /note SYMBOL texto")
        symbol = self._parse_symbol(parts[0])
        note = parts[1].strip()
        if not note:
            raise ValueError("La nota no puede estar vacia.")
        trade = await self._trade_manager.set_trade_note(symbol, note)
        await self._notify_trade_changed(symbol)
        await self._send_message(chat_id, f"Nota actualizada para {trade.symbol}.")

    async def _handle_session_message(self, chat_id: str, text: str, session: ChatSession) -> None:
        try:
            if session.mode == "addtrade":
                await self._handle_addtrade_flow(chat_id, text, session)
                return
            if session.mode == "delete_confirm":
                await self._handle_delete_confirmation(chat_id, text, session)
                return
        except ValueError as exc:
            await self._send_message(chat_id, str(exc))
            return

    async def _handle_delete_confirmation(
        self,
        chat_id: str,
        text: str,
        session: ChatSession,
    ) -> None:
        answer = text.strip().lower()
        if answer not in {"yes", "no", "y", "n"}:
            raise ValueError("Confirma con yes o no.")
        if answer in {"no", "n"}:
            self._sessions.pop(chat_id, None)
            await self._send_message(chat_id, "Operacion cancelada.")
            return
        symbol = session.data["symbol"]
        trade = await self._trade_manager.delete_trade(symbol)
        self._sessions.pop(chat_id, None)
        await self._notify_trade_changed(symbol)
        await self._send_message(chat_id, f"Trade {trade.symbol} desactivado.")

    async def _handle_addtrade_flow(
        self,
        chat_id: str,
        text: str,
        session: ChatSession,
    ) -> None:
        step = session.step
        if step == "symbol":
            symbol = self._parse_symbol(text)
            session.data["symbol"] = symbol
            session.step = "side"
            suffix_note = ""
            if not symbol.endswith("USDT"):
                suffix_note = " Advertencia: para USD-M Futures normalmente se usa sufijo USDT."
            await self._send_message(chat_id, f"Paso 2/9: side LONG o SHORT.{suffix_note}")
            return

        if step == "side":
            session.data["side"] = self._parse_side(text)
            session.step = "entry"
            await self._send_message(chat_id, "Paso 3/9: entry (precio de entrada).")
            return

        if step == "entry":
            session.data["entry"] = self._parse_positive_float(text, "entry")
            session.step = "leverage"
            await self._send_message(chat_id, "Paso 4/9: leverage entero positivo.")
            return

        if step == "leverage":
            session.data["leverage"] = self._parse_positive_int(text, "leverage")
            session.step = "stop_loss"
            await self._send_message(chat_id, "Paso 5/9: stop_loss.")
            return

        if step == "stop_loss":
            session.data["stop_loss"] = self._parse_positive_float(text, "stop_loss")
            session.step = "take_profits"
            await self._send_message(
                chat_id,
                "Paso 6/9: take profits separados por coma. Ejemplo: 0.8820,0.8740,0.8600",
            )
            return

        if step == "take_profits":
            session.data["take_profits"] = self._parse_take_profits(text)
            session.step = "invalidation_zone"
            await self._send_message(
                chat_id,
                "Paso 7/9: invalidation zone como 'MIN MAX' o 'skip'.",
            )
            return

        if step == "invalidation_zone":
            session.data["invalidation_zone"] = self._parse_invalidation_zone(text)
            session.step = "timeframe"
            await self._send_message(chat_id, "Paso 8/9: timeframe. Ejemplo: 1m, 5m o 15m.")
            return

        if step == "timeframe":
            session.data["timeframe"] = self._parse_timeframe(text)
            session.step = "note"
            await self._send_message(chat_id, "Paso 9/9: nota opcional o '-' para omitir.")
            return

        if step == "note":
            session.data["note"] = self._normalize_note(text)
            session.step = "confirm"
            draft = self._build_draft_trade(session.data)
            summary = [
                "Confirmacion de trade:",
                self._format_trade(draft),
                f"timeframe {draft.strategy.timeframe or '-'}",
                f"note {draft.note or '-'}",
                "Responde yes/no para guardar.",
            ]
            await self._send_message(chat_id, "\n".join(summary))
            return

        if step == "confirm":
            answer = text.strip().lower()
            if answer not in {"yes", "no", "y", "n"}:
                raise ValueError("Confirma con yes o no.")
            if answer in {"no", "n"}:
                self._sessions.pop(chat_id, None)
                await self._send_message(chat_id, "Creacion de trade cancelada.")
                return
            trade = await self._trade_manager.create_trade(self._build_draft_trade(session.data))
            self._sessions.pop(chat_id, None)
            await self._notify_trade_changed(trade.symbol)
            await self._send_message(chat_id, f"Trade guardado: {self._format_trade(trade)}")

    def _build_draft_trade(self, data: dict[str, Any]) -> TradeConfig:
        return TradeConfig(
            symbol=data["symbol"],
            side=data["side"],
            entry=data["entry"],
            leverage=data["leverage"],
            stop_loss=data["stop_loss"],
            status=TradeStatus.ACTIVE,
            take_profits=list(data["take_profits"]),
            invalidation_zone=data["invalidation_zone"],
            alerts=TradeAlertConfig(
                cooldown_seconds=300,
                sl_distance_threshold_pct=self._alert_settings.stop_loss_warning_pct,
                notify_on_tp=True,
                notify_on_sl_distance=True,
                notify_on_invalidation_zone=True,
                notify_on_break_even=True,
                notify_on_add_zone=True,
            ),
            strategy=TradeStrategyConfig(
                timeframe=data["timeframe"],
                ema_fast=55,
                ema_slow=200,
                use_structure=True,
                pivot_length=5,
                use_squeeze_momentum_placeholder=False,
            ),
            note=data["note"],
        )

    @classmethod
    def _split_args(cls, raw: str, expected: int) -> tuple[str, ...]:
        parts = raw.split(maxsplit=expected - 1)
        if len(parts) != expected:
            raise ValueError("Argumentos insuficientes.")
        return tuple(parts)

    @classmethod
    def _parse_symbol_argument(cls, raw: str) -> str:
        if not raw.strip():
            raise ValueError("Debes indicar un simbolo.")
        return cls._parse_symbol(raw)

    @classmethod
    def _parse_symbol_and_price(cls, raw: str, field_name: str) -> tuple[str, float]:
        symbol_raw, price_raw = cls._split_args(raw, expected=2)
        return cls._parse_symbol(symbol_raw), cls._parse_positive_float(price_raw, field_name)

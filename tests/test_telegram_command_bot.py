import asyncio
import logging
from pathlib import Path

from app.models import AlertEngineSettings, Candle, TradeConfig, TradeSide, TradeStatus
from app.storage import Storage
from app.telegram_command_bot import TelegramCommandBot
from app.trade_manager import TradeManager


def run(coro):
    return asyncio.run(coro)


def make_trade(symbol: str = "WLDUSDT") -> TradeConfig:
    return TradeConfig(
        symbol=symbol,
        side=TradeSide.SHORT,
        entry=0.8912,
        leverage=20,
        stop_loss=0.9050,
        take_profits=[0.8820, 0.8740, 0.8600],
        note="test trade",
    )


def make_update(chat_id: str, text: str, update_id: int = 1) -> dict:
    return {
        "update_id": update_id,
        "message": {
            "message_id": update_id,
            "chat": {"id": chat_id, "type": "private"},
            "text": text,
        },
    }


class FakeBinanceClient:
    def __init__(
        self,
        *,
        mark_prices: dict[str, tuple[float, int | None]] | None = None,
        klines: dict[tuple[str, str], list[Candle]] | None = None,
        fail_mark_price: bool = False,
        fail_klines: bool = False,
    ) -> None:
        self.mark_prices = mark_prices or {}
        self.klines = klines or {}
        self.fail_mark_price = fail_mark_price
        self.fail_klines = fail_klines

    async def get_mark_price(self, symbol: str) -> tuple[float, int | None]:
        if self.fail_mark_price:
            raise RuntimeError("mark price unavailable")
        return self.mark_prices[symbol.upper()]

    async def get_klines(self, symbol: str, interval: str, limit: int) -> list[Candle]:
        if self.fail_klines:
            raise RuntimeError("kline fetch failed")
        return self.klines[(symbol.upper(), interval)][:limit]


async def build_bot(
    tmp_path: Path,
    *,
    fake_client: FakeBinanceClient | None = None,
    cached_prices: dict[str, tuple[float, str, int | None]] | None = None,
):
    sent_messages: list[tuple[str, str]] = []
    storage = Storage(tmp_path / "bot.sqlite")
    await storage.initialize()
    trade_manager = TradeManager(storage)
    await trade_manager.bootstrap_trades([])

    async def sender(chat_id: str, text: str) -> None:
        sent_messages.append((chat_id, text))

    def status_provider() -> dict[str, str | int]:
        return {
            "DRY_RUN": "true",
            "BINANCE_TESTNET": "false",
            "BINANCE_PRIVATE_ACCOUNT_SYNC": "true",
            "BINANCE_MARKET_SOURCE": "mark_price",
            "SCANNER_ENABLED": "true",
            "SCANNER_ALERTS_ENABLED": "true",
            "SCANNER_INTERVAL_SECONDS": 60,
            "active_trades": trade_manager.count_trades((TradeStatus.ACTIVE,)),
        }

    async def current_price_provider(symbol: str):
        if cached_prices is None:
            return None
        return cached_prices.get(symbol.upper())

    bot = TelegramCommandBot(
        bot_token="test-token",
        authorized_chat_id="123",
        trade_manager=trade_manager,
        alert_settings=AlertEngineSettings(),
        logger=logging.getLogger("test.telegram_command_bot"),
        status_provider=status_provider,
        binance_client=fake_client,
        current_price_provider=current_price_provider,
        sender=sender,
    )
    return bot, trade_manager, storage, sent_messages


def test_create_trade_valid(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, trade_manager, storage, sent = await build_bot(tmp_path)
        try:
            messages = [
                "/addtrade",
                "WLDUSDT",
                "SHORT",
                "0.8912",
                "20",
                "0.9050",
                "0.8820,0.8740,0.8600",
                "0.9020 0.9060",
                "1m",
                "Short manual",
                "yes",
            ]
            for index, message in enumerate(messages, start=1):
                await bot.handle_update(make_update("123", message, update_id=index))

            trade = trade_manager.get_trade_by_symbol("WLDUSDT", statuses=(TradeStatus.ACTIVE,))
            assert trade is not None
            assert trade.strategy.timeframe == "1m"
            assert trade.note == "Short manual"
            assert trade.take_profits == [0.8820, 0.8740, 0.8600]
            assert any("Trade guardado" in message for _, message in sent)
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_rejects_unauthorized_chat(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, _, storage, sent = await build_bot(tmp_path)
        try:
            await bot.handle_update(make_update("999", "/status"))
            assert sent == [("999", "Unauthorized")]
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_rejects_invalid_price(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, trade_manager, storage, sent = await build_bot(tmp_path)
        try:
            await bot.handle_update(make_update("123", "/addtrade", 1))
            await bot.handle_update(make_update("123", "WLDUSDT", 2))
            await bot.handle_update(make_update("123", "SHORT", 3))
            await bot.handle_update(make_update("123", "-1", 4))
            assert trade_manager.count_trades((TradeStatus.ACTIVE,)) == 0
            assert "entry debe ser mayor que cero." in sent[-1][1]
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_pause_and_resume_trade(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, trade_manager, storage, _ = await build_bot(tmp_path)
        try:
            await trade_manager.create_trade(make_trade())
            await bot.handle_update(make_update("123", "/pause WLDUSDT", 1))
            paused = trade_manager.get_trade_by_symbol("WLDUSDT")
            assert paused is not None
            assert paused.status == TradeStatus.PAUSED

            await bot.handle_update(make_update("123", "/resume WLDUSDT", 2))
            resumed = trade_manager.get_trade_by_symbol("WLDUSDT")
            assert resumed is not None
            assert resumed.status == TradeStatus.ACTIVE
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_close_trade(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, trade_manager, storage, _ = await build_bot(tmp_path)
        try:
            await trade_manager.create_trade(make_trade())
            await bot.handle_update(make_update("123", "/close WLDUSDT", 1))
            trade = trade_manager.get_trade_by_symbol("WLDUSDT")
            assert trade is not None
            assert trade.status == TradeStatus.CLOSED
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_update_stop_loss(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, trade_manager, storage, _ = await build_bot(tmp_path)
        try:
            await trade_manager.create_trade(make_trade())
            await bot.handle_update(make_update("123", "/setsl WLDUSDT 0.9100", 1))
            trade = trade_manager.get_trade_by_symbol("WLDUSDT")
            assert trade is not None
            assert trade.stop_loss == 0.9100
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_list_active_trades(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, trade_manager, storage, sent = await build_bot(tmp_path)
        try:
            await trade_manager.create_trade(make_trade())
            await bot.handle_update(make_update("123", "/trades", 1))
            assert any("WLDUSDT | SHORT" in message for _, message in sent)
            assert any("status ACTIVE" in message for _, message in sent)
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_help_and_comandos_show_watchlist_guidance(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, _, storage, sent = await build_bot(tmp_path)
        try:
            await bot.handle_update(make_update("123", "/help", 1))
            await bot.handle_update(make_update("123", "/comandos", 2))
            assert "🤖 Bot Trading — Comandos" in sent[0][1]
            assert "/watchlist" in sent[0][1]
            assert "/add watchlist BTC" in sent[0][1]
            assert "DRY_RUN activo" in sent[0][1]
            assert sent[1][1] == sent[0][1]
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_watchlist_empty_and_add_remove_flow(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, trade_manager, storage, sent = await build_bot(tmp_path)
        try:
            await bot.handle_update(make_update("123", "/watchlist", 1))
            assert "Watchlist BE vacía" in sent[-1][1]

            await bot.handle_update(make_update("123", "/add watchlist BTC", 2))
            assert trade_manager.get_watchlist_symbols() == ["BTCUSDT"]

            await bot.handle_update(make_update("123", "/watchlist", 3))
            assert "Scanner: ON" in sent[-1][1]
            assert "Alertas: ON" in sent[-1][1]
            assert "Intervalo: 60s" in sent[-1][1]
            assert "1. BTCUSDT ✅" in sent[-1][1]
            assert "M1, M3, M5, M15" in sent[-1][1]
            assert "ADX/DMI" in sent[-1][1]
            assert "Cruce EMA M15: cierre M15" in sent[-1][1]

            await bot.handle_update(make_update("123", "/remove watchlist BTC", 4))
            assert trade_manager.get_watchlist_symbols() == []
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_add_and_signal_usage_messages(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, _, storage, sent = await build_bot(tmp_path)
        try:
            await bot.handle_update(make_update("123", "/add watchlist", 1))
            await bot.handle_update(make_update("123", "/remove watchlist", 2))
            await bot.handle_update(make_update("123", "/signal", 3))
            assert sent[0][1] == "Uso:\n/add watchlist BTC\n/add watchlist ETHUSDT"
            assert sent[1][1] == "Uso:\n/remove watchlist BTC"
            assert sent[2][1] == "Uso:\n/signal BTC\n/signal ETHUSDT"
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_consulta_precio_uses_cached_symbol_normalization(tmp_path: Path) -> None:
    async def scenario() -> None:
        cached = {
            "BTCUSDT": (64250.50, "Binance Futures mark price (cache websocket)", 1710000000000)
        }
        bot, _, storage, sent = await build_bot(tmp_path, cached_prices=cached)
        try:
            await bot.handle_update(make_update("123", "/consulta precio BTC", 1))
            assert any("BTCUSDT" in message for _, message in sent)
            assert any("64,250.50 USDT" in message for _, message in sent)
            assert any("cache websocket" in message for _, message in sent)
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_consulta_precio_falls_back_to_rest(tmp_path: Path) -> None:
    async def scenario() -> None:
        fake_client = FakeBinanceClient(mark_prices={"ETHUSDT": (3100.25, 1710000001000)})
        bot, _, storage, sent = await build_bot(tmp_path, fake_client=fake_client)
        try:
            await bot.handle_update(make_update("123", "/consulta precio ETH", 1))
            assert any("ETHUSDT" in message for _, message in sent)
            assert any("3,100.25 USDT" in message for _, message in sent)
            assert any("mark price (REST)" in message for _, message in sent)
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_consulta_rejects_invalid_format(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, _, storage, sent = await build_bot(tmp_path)
        try:
            await bot.handle_update(make_update("123", "/consulta", 1))
            await bot.handle_update(make_update("123", "/consulta foo BTC", 2))
            assert sent[0][1] == "Uso: /consulta precio BTC"
            assert sent[1][1] == "Consulta no reconocida. Proba: /consulta precio BTC"
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_renderizar_reports_ohlcv_available(tmp_path: Path) -> None:
    async def scenario() -> None:
        candles = [
            Candle(
                open_time=1710000000000 + index * 60000,
                open=100 + index,
                high=101 + index,
                low=99 + index,
                close=100.5 + index,
                close_time=1710000059999 + index * 60000,
                volume=1000 + index,
            )
            for index in range(50)
        ]
        fake_client = FakeBinanceClient(klines={("BTCUSDT", "1m"): candles})
        bot, _, storage, sent = await build_bot(tmp_path, fake_client=fake_client)
        try:
            await bot.handle_update(make_update("123", "/renderizar BTCUSDT 1m", 1))
            assert any("Renderizado posible: SI" in message for _, message in sent)
            assert any("Velas disponibles: 50" in message for _, message in sent)
            assert any("open_time, open, high, low, close, volume" in message for _, message in sent)
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_renderizar_reports_controlled_error(tmp_path: Path) -> None:
    async def scenario() -> None:
        fake_client = FakeBinanceClient(fail_klines=True)
        bot, _, storage, sent = await build_bot(tmp_path, fake_client=fake_client)
        try:
            await bot.handle_update(make_update("123", "/renderizar BTCUSDT 1m", 1))
            assert any("Renderizado posible: NO" in message for _, message in sent)
            assert any("kline fetch failed" in message for _, message in sent)
        finally:
            await bot.close()
            await storage.close()

    run(scenario())


def test_signal_reports_ema_matrix(tmp_path: Path) -> None:
    async def scenario() -> None:
        bull_candles = [
            Candle(
                open_time=1710000000000 + index * 60000,
                open=100 + (index * 0.4),
                high=100.2 + (index * 0.4),
                low=99.8 + (index * 0.4),
                close=100 + (index * 0.4),
                close_time=1710000059999 + index * 60000,
                volume=100.0,
            )
            for index in range(210)
        ]
        bull_candles[-2].volume = 150.0
        bear_cross_closes = ([150.0] * 200) + [1.0, 9999.0]
        bear_cross_candles = [
            Candle(index, 0, close, close, close, index + 1, 1000.0)
            for index, close in enumerate(bear_cross_closes)
        ]
        fake_client = FakeBinanceClient(
            klines={
                ("BTCUSDT", "15m"): bull_candles,
                ("BTCUSDT", "5m"): bull_candles,
                ("BTCUSDT", "3m"): bear_cross_candles,
                ("BTCUSDT", "1m"): bull_candles,
            }
        )
        bot, _, storage, sent = await build_bot(tmp_path, fake_client=fake_client)
        try:
            await bot.handle_update(make_update("123", "/signal BTC", 1))
            assert any("BTCUSDT" in message for _, message in sent)
            assert any("Resultado:" in message for _, message in sent)
            assert any("Sync:" in message for _, message in sent)
            assert any("M15 🟢" in message for _, message in sent)
            assert any("MAs M15:" in message for _, message in sent)
            assert any("Koncorde Lite:" in message for _, message in sent)
            assert any("Calidad:" in message for _, message in sent)
            assert any("RSI " in message and "Vol " in message for _, message in sent)
        finally:
            await bot.close()
            await storage.close()

    run(scenario())

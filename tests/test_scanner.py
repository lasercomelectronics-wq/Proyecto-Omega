import asyncio
import logging
from pathlib import Path

from app.models import AlertEvent, AlertPriority, Candle
from app.scanner import SignalScanner, effective_quality_label
from app.storage import Storage
from app.trade_manager import TradeManager


def run(coro):
    return asyncio.run(coro)


class FakeBinanceClient:
    def __init__(self, klines: dict[tuple[str, str], list[Candle]]) -> None:
        self.klines = klines

    async def get_klines(self, symbol: str, interval: str, limit: int) -> list[Candle]:
        if symbol.upper() == "ETHUSDT":
            raise RuntimeError("simulated error")
        return self.klines[(symbol.upper(), interval)][:limit]


def make_bull_candles(limit: int = 210) -> list[Candle]:
    candles = [
        Candle(
            open_time=1710000000000 + (index * 60000),
            open=100 + (index * 0.4),
            high=100.2 + (index * 0.4),
            low=99.8 + (index * 0.4),
            close=100 + (index * 0.4),
            close_time=1710000059999 + (index * 60000),
            volume=100.0,
        )
        for index in range(limit)
    ]
    candles[-2].volume = 150.0
    return candles


def make_bear_cross_candles() -> list[Candle]:
    closes = ([150.0] * 200) + [1.0, 9999.0]
    return [
        Candle(index, 0, close, close, close, index + 1, 100.0)
        for index, close in enumerate(closes)
    ]


def test_scanner_sends_full_sync_once_per_closed_candle(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        trade_manager = TradeManager(storage)
        await trade_manager.bootstrap_trades([])
        await trade_manager.add_watchlist_symbol("BTCUSDT")

        candles = make_bull_candles()
        client = FakeBinanceClient(
            {
                ("BTCUSDT", "15m"): candles,
                ("BTCUSDT", "5m"): candles,
                ("BTCUSDT", "3m"): candles,
                ("BTCUSDT", "1m"): candles,
            }
        )
        sent_events = []

        async def dispatch(events):
            sent_events.extend(events)
            return len(events)

        scanner = SignalScanner(
            binance_client=client,
            trade_manager=trade_manager,
            storage=storage,
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.scanner"),
            interval_seconds=60,
            alerts_enabled=True,
        )
        try:
            await scanner.scan_once()
            await scanner.scan_once()
            assert len(sent_events) == 1
            assert sent_events[0].metadata["level"] == "FULL_4_4"
            assert sent_events[0].metadata["trigger_tf"] == "1m"
        finally:
            await scanner.stop()
            await storage.close()

    run(scenario())


def test_scanner_skips_when_sync_not_valid(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        trade_manager = TradeManager(storage)
        await trade_manager.bootstrap_trades([])
        await trade_manager.add_watchlist_symbol("BTCUSDT")

        bull = make_bull_candles()
        bear = make_bear_cross_candles()
        client = FakeBinanceClient(
            {
                ("BTCUSDT", "15m"): bull,
                ("BTCUSDT", "5m"): bull,
                ("BTCUSDT", "3m"): bear,
                ("BTCUSDT", "1m"): bull,
            }
        )
        sent_events = []

        async def dispatch(events):
            sent_events.extend(events)
            return len(events)

        scanner = SignalScanner(
            binance_client=client,
            trade_manager=trade_manager,
            storage=storage,
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.scanner"),
            interval_seconds=60,
            alerts_enabled=True,
        )
        try:
            await scanner.scan_once()
            assert sent_events == []
        finally:
            await scanner.stop()
            await storage.close()

    run(scenario())


def test_scanner_continues_other_symbols_on_error(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        trade_manager = TradeManager(storage)
        await trade_manager.bootstrap_trades([])
        for symbol in ("BTCUSDT", "ETHUSDT", "WLDUSDT"):
            await trade_manager.add_watchlist_symbol(symbol)

        candles = make_bull_candles()
        client = FakeBinanceClient(
            {
                ("BTCUSDT", "15m"): candles,
                ("BTCUSDT", "5m"): candles,
                ("BTCUSDT", "3m"): candles,
                ("BTCUSDT", "1m"): candles,
                ("WLDUSDT", "15m"): candles,
                ("WLDUSDT", "5m"): candles,
                ("WLDUSDT", "3m"): candles,
                ("WLDUSDT", "1m"): candles,
            }
        )
        sent_events = []

        async def dispatch(events):
            sent_events.extend(events)
            return len(events)

        scanner = SignalScanner(
            binance_client=client,
            trade_manager=trade_manager,
            storage=storage,
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.scanner"),
            interval_seconds=60,
            alerts_enabled=True,
            require_adx_not_weak=False,
            block_dry_volume=False,
            alert_low_quality=True,
        )
        try:
            await scanner.scan_once()
            symbols_sent = {event.symbol for event in sent_events}
            assert "BTCUSDT" in symbols_sent
            assert "WLDUSDT" in symbols_sent
            assert "ETHUSDT" not in symbols_sent
            state = scanner.get_last_scan_state()
            assert state["ETHUSDT"]["block_reason"] == "ERROR"
        finally:
            await scanner.stop()
            await storage.close()

    run(scenario())


def test_effective_quality_caps_weak_setup() -> None:
    quality = {
        "quality": "ALTA",
        "adx_human": {"state": "ADX_WEAK"},
        "koncorde_human": {"state": "DRY_VOLUME"},
    }
    structure = {"bos": "NONE", "pullback": "NONE"}
    assert effective_quality_label(quality_payload=quality, structure_m15=structure) == "MEDIA"


def test_scanner_note_plan_sections() -> None:
    scanner = SignalScanner(
        binance_client=FakeBinanceClient({}),
        trade_manager=type("TM", (), {"get_watchlist_symbols": lambda self: []})(),
        storage=type("S", (), {})(),
        dispatch_alerts=lambda events: None,  # type: ignore[arg-type]
        logger=logging.getLogger("test.scanner"),
    )
    event = AlertEvent(
        key="k",
        priority=AlertPriority.INFO,
        reason="r",
        symbol="BTCUSDT",
        note="⚠️ Señal detectada",
        metadata={"scanner": True, "plan_created": False, "plan_block_reason": "ADX débil"},
    )
    scanner._attach_plan_section(event)
    assert "No generado" in (event.note or "")
    assert "Motivo: ADX débil" in (event.note or "")

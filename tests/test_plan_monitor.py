import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.models import Candle
from app.plan_monitor import PlanMonitor
from app.storage import Storage


def run(coro):
    return asyncio.run(coro)


class FakeClient:
    def __init__(self, prices: list[float], klines_3m: list[Candle] | None = None) -> None:
        self._prices = prices
        self._idx = 0
        self._klines_3m = klines_3m or []

    async def get_mark_price(self, symbol: str):
        if self._idx >= len(self._prices):
            price = self._prices[-1]
        else:
            price = self._prices[self._idx]
            self._idx += 1
        return price, 1710000000000

    async def get_klines(self, symbol: str, interval: str, limit: int):
        assert interval == "3m"
        return self._klines_3m[:limit]


def _make_candle(*, open_time: int, high: float, low: float, close_time: int) -> Candle:
    return Candle(
        open_time=open_time,
        open=low,
        high=high,
        low=low,
        close=high,
        close_time=close_time,
        volume=1.0,
    )


async def _seed_plan(storage: Storage, *, direction: str, entry_low: float, entry_high: float, sl: float, tp1: float, tp2: float, tp3: float) -> int:
    now = datetime.now(tz=timezone.utc)
    return await storage.create_trade_plan(
        {
            "symbol": "WLDUSDT",
            "direction": direction,
            "signal_type": f"{direction}_PULLBACK",
            "quality": "ALTA",
            "entry_zone_low": entry_low,
            "entry_zone_high": entry_high,
            "stop_loss": sl,
            "tp1": tp1,
            "tp2": tp2,
            "tp3": tp3,
            "rr_tp1": 1.0,
            "rr_tp2": 2.0,
            "rr_tp3": 3.0,
            "status": "CREATED",
            "created_at": now.isoformat(),
            "expires_at": (now + timedelta(minutes=60)).isoformat(),
            "plan_fingerprint": f"seed::{direction}::{int(now.timestamp())}",
        }
    )


def test_long_jump_to_tp3_sends_single_consolidated_alert(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id = await _seed_plan(
            storage,
            direction="LONG",
            entry_low=1.000,
            entry_high=1.000,
            sl=0.990,
            tp1=1.010,
            tp2=1.020,
            tp3=1.030,
        )
        monitor = PlanMonitor(
            storage=storage,
            binance_client=FakeClient([1.035]),
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.plan_monitor"),
            interval_seconds=15,
        )
        await monitor._tick()

        row = await storage.get_trade_plan(plan_id)
        assert row is not None
        assert row["status"] == "TP3_HIT"
        assert len(sent) == 1
        assert "TP3 alcanzado" in (sent[0].note or "")
        assert "TP1, TP2, TP3" in (sent[0].note or "")
        count = storage._connection.execute(  # noqa: SLF001
            "SELECT COUNT(*) FROM trade_plan_events WHERE plan_id = ? AND event_type IN ('TP1_HIT','TP2_HIT','TP3_HIT')",
            (plan_id,),
        ).fetchone()[0]
        assert count == 3
        await storage.close()

    run(scenario())


def test_short_jump_to_tp3_sends_single_consolidated_alert(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id = await _seed_plan(
            storage,
            direction="SHORT",
            entry_low=1.000,
            entry_high=1.000,
            sl=1.010,
            tp1=0.990,
            tp2=0.980,
            tp3=0.970,
        )
        monitor = PlanMonitor(
            storage=storage,
            binance_client=FakeClient([0.965]),
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.plan_monitor"),
            interval_seconds=15,
        )
        await monitor._tick()

        row = await storage.get_trade_plan(plan_id)
        assert row is not None
        assert row["status"] == "TP3_HIT"
        assert len(sent) == 1
        assert "TP3 alcanzado" in (sent[0].note or "")
        assert "TP1, TP2, TP3" in (sent[0].note or "")
        await storage.close()

    run(scenario())


def test_long_tp_progression_across_cycles_and_no_duplicates_after_tp3(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id = await _seed_plan(
            storage,
            direction="LONG",
            entry_low=1.000,
            entry_high=1.000,
            sl=0.990,
            tp1=1.010,
            tp2=1.020,
            tp3=1.030,
        )
        monitor = PlanMonitor(
            storage=storage,
            binance_client=FakeClient([1.011, 1.021, 1.031, 1.031]),
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.plan_monitor"),
            interval_seconds=15,
        )
        await monitor._tick()
        await monitor._tick()
        await monitor._tick()
        before_recheck = len(sent)
        await monitor._tick()

        assert before_recheck == 3
        assert len(sent) == 3
        assert "TP1 alcanzado" in (sent[0].note or "")
        assert "TP2 alcanzado" in (sent[1].note or "")
        assert "TP3 alcanzado" in (sent[2].note or "")

        row = await storage.get_trade_plan(plan_id)
        assert row is not None
        assert row["status"] == "TP3_HIT"

        inserted_again = await storage.record_trade_plan_event(plan_id, "TP1_HIT", 1.011, {})
        assert inserted_again is False
        await storage.close()

    run(scenario())


def test_long_mark_ambiguous_resolves_tp_first_with_m3(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id = await _seed_plan(
            storage,
            direction="LONG",
            entry_low=1.000,
            entry_high=1.000,
            sl=1.015,
            tp1=1.010,
            tp2=1.020,
            tp3=1.030,
        )
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        klines = [
            _make_candle(open_time=now_ms - 600000, high=1.011, low=1.016, close_time=now_ms - 420000),
            _make_candle(open_time=now_ms - 420000, high=1.012, low=1.016, close_time=now_ms - 240000),
        ]
        monitor = PlanMonitor(
            storage=storage,
            binance_client=FakeClient([1.012], klines_3m=klines),
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.plan_monitor"),
            interval_seconds=15,
        )
        await monitor._tick()

        row = await storage.get_trade_plan(plan_id)
        assert row is not None
        assert row["status"] == "TP1_HIT"
        assert len(sent) == 1
        assert "TP1 alcanzado" in (sent[0].note or "")
        assert "ambiguo" not in (sent[0].note or "").lower()
        await storage.close()

    run(scenario())


def test_long_mark_ambiguous_resolves_sl_first_with_m3(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id = await _seed_plan(
            storage,
            direction="LONG",
            entry_low=1.000,
            entry_high=1.000,
            sl=1.015,
            tp1=1.010,
            tp2=1.020,
            tp3=1.030,
        )
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        klines = [
            _make_candle(open_time=now_ms - 600000, high=1.009, low=1.014, close_time=now_ms - 420000),
            _make_candle(open_time=now_ms - 420000, high=1.009, low=1.013, close_time=now_ms - 240000),
        ]
        monitor = PlanMonitor(
            storage=storage,
            binance_client=FakeClient([1.012], klines_3m=klines),
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.plan_monitor"),
            interval_seconds=15,
        )
        await monitor._tick()

        row = await storage.get_trade_plan(plan_id)
        assert row is not None
        assert row["status"] == "SL_HIT"
        assert len(sent) == 1
        assert "SL alcanzado" in (sent[0].note or "")
        assert "ambiguo" not in (sent[0].note or "").lower()
        await storage.close()

    run(scenario())


def test_short_mark_ambiguous_resolves_tp_first_with_m3(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id = await _seed_plan(
            storage,
            direction="SHORT",
            entry_low=1.000,
            entry_high=1.000,
            sl=0.985,
            tp1=0.990,
            tp2=0.980,
            tp3=0.970,
        )
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        klines = [
            _make_candle(open_time=now_ms - 600000, high=0.984, low=0.983, close_time=now_ms - 420000),
            _make_candle(open_time=now_ms - 420000, high=0.984, low=0.982, close_time=now_ms - 240000),
        ]
        monitor = PlanMonitor(
            storage=storage,
            binance_client=FakeClient([0.988], klines_3m=klines),
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.plan_monitor"),
            interval_seconds=15,
        )
        await monitor._tick()

        row = await storage.get_trade_plan(plan_id)
        assert row is not None
        assert row["status"] == "TP1_HIT"
        assert len(sent) == 1
        assert "TP1 alcanzado" in (sent[0].note or "")
        await storage.close()

    run(scenario())


def test_short_mark_ambiguous_resolves_sl_first_with_m3(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id = await _seed_plan(
            storage,
            direction="SHORT",
            entry_low=1.000,
            entry_high=1.000,
            sl=0.985,
            tp1=0.990,
            tp2=0.980,
            tp3=0.970,
        )
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        klines = [
            _make_candle(open_time=now_ms - 600000, high=0.986, low=0.991, close_time=now_ms - 420000),
            _make_candle(open_time=now_ms - 420000, high=0.987, low=0.992, close_time=now_ms - 240000),
        ]
        monitor = PlanMonitor(
            storage=storage,
            binance_client=FakeClient([0.988], klines_3m=klines),
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.plan_monitor"),
            interval_seconds=15,
        )
        await monitor._tick()

        row = await storage.get_trade_plan(plan_id)
        assert row is not None
        assert row["status"] == "SL_HIT"
        assert len(sent) == 1
        assert "SL alcanzado" in (sent[0].note or "")
        await storage.close()

    run(scenario())


def test_same_m3_candle_keeps_ambiguous_fallback(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id = await _seed_plan(
            storage,
            direction="LONG",
            entry_low=1.000,
            entry_high=1.000,
            sl=1.015,
            tp1=1.010,
            tp2=1.020,
            tp3=1.030,
        )
        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        klines = [
            _make_candle(open_time=now_ms - 600000, high=1.011, low=1.014, close_time=now_ms - 420000),
        ]
        monitor = PlanMonitor(
            storage=storage,
            binance_client=FakeClient([1.012, 1.012], klines_3m=klines),
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.plan_monitor"),
            interval_seconds=15,
        )
        await monitor._tick()
        await monitor._tick()

        row = await storage.get_trade_plan(plan_id)
        assert row is not None
        assert row["status"] == "TP_SL_AMBIGUOUS"
        assert len(sent) == 1
        assert "same_3m_candle" in (sent[0].note or "")
        events = storage._connection.execute(  # noqa: SLF001
            "SELECT event_type FROM trade_plan_events WHERE plan_id = ? ORDER BY id",
            (plan_id,),
        ).fetchall()
        assert [row[0] for row in events] == ["AMBIGUOUS_TP_SL"]
        await storage.close()

    run(scenario())


def test_no_m3_data_uses_ambiguous_fallback(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id = await _seed_plan(
            storage,
            direction="LONG",
            entry_low=1.000,
            entry_high=1.000,
            sl=1.015,
            tp1=1.010,
            tp2=1.020,
            tp3=1.030,
        )
        monitor = PlanMonitor(
            storage=storage,
            binance_client=FakeClient([1.012], klines_3m=[]),
            dispatch_alerts=dispatch,
            logger=logging.getLogger("test.plan_monitor"),
            interval_seconds=15,
        )
        await monitor._tick()

        row = await storage.get_trade_plan(plan_id)
        assert row is not None
        assert row["status"] == "TP_SL_AMBIGUOUS"
        assert len(sent) == 1
        assert "insufficient_3m_data" in (sent[0].note or "")
        await storage.close()

    run(scenario())

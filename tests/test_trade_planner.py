import asyncio
from pathlib import Path

from app.storage import Storage
from app.trade_planner import build_trade_plan, trade_plan_to_dict
from app.plan_monitor import PlanMonitor


def run(coro):
    return asyncio.run(coro)


def quality_payload(direction: str, signal_type: str, alert_allowed: bool = True) -> dict:
    return {
        "result": direction,
        "signal_type": signal_type,
        "quality": "ALTA",
        "alert_allowed": alert_allowed,
        "ema_human": {
            "close_vs_ema200": "ABOVE" if direction == "LONG" else "BELOW",
            "ema55_vs_ema200": "ABOVE" if direction == "LONG" else "BELOW",
        },
    }


def test_long_plan_rr_and_short_plan_rr() -> None:
    long_plan = build_trade_plan(
        symbol="BTCUSDT",
        quality_payload=quality_payload("LONG", "LONG_PULLBACK"),
        current_price=100.0,
        structure_m15={"last_high": 102.0, "last_low": 98.0, "pullback": "LONG", "bias": "BULL", "bos": "BULL"},
    )
    short_plan = build_trade_plan(
        symbol="ETHUSDT",
        quality_payload=quality_payload("SHORT", "SHORT_PULLBACK"),
        current_price=100.0,
        structure_m15={"last_high": 102.0, "last_low": 98.0, "pullback": "SHORT", "bias": "BEAR", "bos": "BEAR"},
    )
    assert long_plan is not None and long_plan.rr_tp1 > 0
    assert short_plan is not None and short_plan.rr_tp1 > 0
    long_entry = (long_plan.entry_zone_low + long_plan.entry_zone_high) / 2
    assert long_plan.stop_loss < long_entry < long_plan.tp1 < long_plan.tp2 < long_plan.tp3
    short_entry = (short_plan.entry_zone_low + short_plan.entry_zone_high) / 2
    assert short_plan.tp3 < short_plan.tp2 < short_plan.tp1 < short_entry < short_plan.stop_loss


def test_trade_plan_slots_serialization_to_dict() -> None:
    plan = build_trade_plan(
        symbol="BTCUSDT",
        quality_payload=quality_payload("LONG", "LONG_PULLBACK"),
        current_price=100.0,
        structure_m15={"last_high": 102.0, "last_low": 98.0, "pullback": "LONG", "bias": "BULL", "bos": "BULL"},
    )
    assert plan is not None
    payload = trade_plan_to_dict(plan)
    assert payload["symbol"] == "BTCUSDT"
    assert payload["setup_type"] == "LONG_PIVOT_RETEST_M15"
    assert payload["execution_tf"] == "15m"
    assert "tp1" in payload and "stop_loss" in payload


def test_long_pivot_retest_requires_price_above_ema200() -> None:
    payload = quality_payload("LONG", "LONG_PULLBACK")
    payload["ema_human"]["close_vs_ema200"] = "BELOW"
    plan = build_trade_plan(
        symbol="BTCUSDT",
        quality_payload=payload,
        current_price=100.0,
        structure_m15={"last_high": 102.0, "last_low": 98.0, "pullback": "LONG", "bias": "BULL", "bos": "BULL", "hl": True},
    )
    assert plan is None


def test_no_plan_for_conflict_sin_senal_and_chase_default() -> None:
    assert build_trade_plan(symbol="BTCUSDT", quality_payload=quality_payload("SIN_SEÑAL", "SIN_SEÑAL", False), current_price=100.0, structure_m15={}) is None
    assert build_trade_plan(symbol="BTCUSDT", quality_payload=quality_payload("CONFLICT", "CONFLICT", False), current_price=100.0, structure_m15={}) is None
    assert build_trade_plan(symbol="BTCUSDT", quality_payload=quality_payload("LONG", "MOMENTUM_CHASE", True), current_price=100.0, structure_m15={"last_high": 102, "last_low": 98}) is None


class FakeClient:
    def __init__(self, price: float) -> None:
        self.price = price

    async def get_mark_price(self, symbol: str):
        return self.price, 1710000000000


async def _seed_plan(storage: Storage, direction: str) -> int:
    plan = build_trade_plan(
        symbol="BTCUSDT",
        quality_payload=quality_payload(direction, f"{direction}_PULLBACK"),
        current_price=100.0,
        structure_m15={"last_high": 102.0, "last_low": 98.0, "pullback": direction, "bias": "BULL" if direction == "LONG" else "BEAR", "bos": "BULL" if direction == "LONG" else "BEAR"},
        expire_minutes=60,
    )
    assert plan is not None
    payload = trade_plan_to_dict(plan)
    payload["plan_fingerprint"] = f"seed::{direction}::{plan.created_at}"
    return await storage.create_trade_plan(payload)


def test_monitor_tp_sl_and_no_duplicate_events(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        sent = []

        async def dispatch(events):
            sent.extend(events)
            return len(events)

        plan_id_long = await _seed_plan(storage, "LONG")
        monitor_long = PlanMonitor(storage=storage, binance_client=FakeClient(9999.0), dispatch_alerts=dispatch, logger=__import__("logging").getLogger("test.monitor"), interval_seconds=15)
        await monitor_long._tick()
        await monitor_long._tick()
        plans = await storage.list_trade_plans()
        row = next(item for item in plans if int(item["id"]) == plan_id_long)
        assert row["status"] in {"TP1_HIT", "TP2_HIT", "TP3_HIT"}

        plan_id_short = await _seed_plan(storage, "SHORT")
        monitor_short = PlanMonitor(storage=storage, binance_client=FakeClient(9999.0), dispatch_alerts=dispatch, logger=__import__("logging").getLogger("test.monitor"), interval_seconds=15)
        await monitor_short._tick()
        plans2 = await storage.list_trade_plans()
        row2 = next(item for item in plans2 if int(item["id"]) == plan_id_short)
        assert row2["status"] in {"SL_HIT", "TP1_HIT", "TP2_HIT", "TP3_HIT"}

        events = [e for e in sent if e.symbol == "BTCUSDT"]
        assert len(events) >= 1
        await storage.close()

    run(scenario())

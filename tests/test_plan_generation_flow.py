import asyncio
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.main import TradingAlertBot
from app.models import AlertEvent, AlertPriority, TradeSide
from app.storage import Storage
from app.trade_planner import build_trade_plan


def run(coro):
    return asyncio.run(coro)


def _quality(direction: str = "LONG", signal_type: str = "LONG_PULLBACK") -> dict:
    return {
        "alert_allowed": True,
        "signal_type": signal_type,
        "quality": "ALTA",
        "adx_human": {"state": "ADX_OK"},
        "koncorde_human": {"state": "LONG_OK" if direction == "LONG" else "SHORT_OK"},
        "ema_human": {
            "close_vs_ema200": "ABOVE" if direction == "LONG" else "BELOW",
            "ema55_vs_ema200": "ABOVE" if direction == "LONG" else "BELOW",
        },
        "result": direction,
    }


def _structure(direction: str = "LONG") -> dict:
    return {
        "pullback": direction,
        "bos": "BULL" if direction == "LONG" else "BEAR",
        "last_high": 105.0,
        "last_low": 95.0,
        "hl": direction == "LONG",
        "lh": direction == "SHORT",
        "pullback_clean": True,
    }


def _event(
    *,
    source_tf: str,
    execution_tf: str,
    m15_open: int,
    source_closed: bool,
    execution_closed: bool,
    key_suffix: str,
    direction: str = "LONG",
    signal_type: str = "LONG_PULLBACK",
) -> AlertEvent:
    return AlertEvent(
        key=f"scanner::BTCUSDT::{key_suffix}",
        priority=AlertPriority.INFO,
        reason="test",
        symbol="BTCUSDT",
        side=TradeSide.LONG if direction == "LONG" else TradeSide.SHORT,
        current_price=100.0,
        metadata={
            "scanner": True,
            "direction": direction,
            "signal_type": signal_type,
            "quality_payload": _quality(direction, signal_type),
            "structure_m15": _structure(direction),
            "source_tf": source_tf,
            "execution_tf": execution_tf,
            "trigger_tf": source_tf,
            "m15_candle_open_time": m15_open,
            "source_candle_closed": source_closed,
            "execution_candle_closed": execution_closed,
        },
    )


async def _build_bot(tmp_path: Path, *, enable_direct_m15_plans: bool = False) -> tuple[TradingAlertBot, Storage]:
    storage = Storage(tmp_path / "bot.sqlite")
    await storage.initialize()
    bot = TradingAlertBot(Path("."))
    bot.logger = logging.getLogger("test.plan.generation")
    bot.storage = storage
    bot.trade_planner_enabled = True
    bot.enable_direct_m15_plans = enable_direct_m15_plans

    async def fake_dispatch(events):
        return len(events)

    bot._dispatch_alerts = fake_dispatch  # type: ignore[method-assign]
    return bot, storage


def _arm_key(symbol: str, side: str, signal_type: str, m15_open: int) -> str:
    return f"armed_m15::{symbol}:{side}:{signal_type}:{m15_open}"


def _arm_stable_key(symbol: str, side: str, m15_open: int) -> str:
    return f"armed_m15::{symbol}:{side}:{m15_open}"


def test_m1_events_are_context_only_without_plan_authority(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path)
        try:
            now_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            for index in range(20):
                event = _event(
                    source_tf="1m",
                    execution_tf="15m",
                    m15_open=now_open,
                    source_closed=True,
                    execution_closed=True,
                    key_suffix=f"m1-{index}",
                )
                await bot.handle_scanner_signal_sent(event)
                assert event.metadata.get("plan_block_reason") == "non_execution_tf_without_armed_m15_setup"
            assert await storage.list_trade_plans() == []
        finally:
            await storage.close()

    run(scenario())


def test_m1_with_armed_m15_setup_can_generate_single_plan(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path)
        try:
            now_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            await storage.record_scanner_alert_state(
                key=_arm_key("BTCUSDT", "LONG", "LONG_PULLBACK", now_open),
                symbol="BTCUSDT",
                direction="LONG",
                level="ARMED_M15_SETUP",
                trigger_tf="15m",
                closed_candle_time=str(now_open),
            )
            event = _event(
                source_tf="1m",
                execution_tf="15m",
                m15_open=now_open,
                source_closed=True,
                execution_closed=True,
                key_suffix="ok",
            )
            await bot.handle_scanner_signal_sent(event)
            assert event.metadata.get("plan_created") is True
            assert event.metadata.get("execution_tf") == "15m"
            assert len(await storage.list_trade_plans()) == 1
        finally:
            await storage.close()

    run(scenario())


def test_m3_with_armed_m15_setup_can_generate_single_plan(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path)
        try:
            now_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            await storage.record_scanner_alert_state(
                key=_arm_key("BTCUSDT", "LONG", "LONG_PULLBACK", now_open),
                symbol="BTCUSDT",
                direction="LONG",
                level="ARMED_M15_SETUP",
                trigger_tf="15m",
                closed_candle_time=str(now_open),
            )
            event = _event(
                source_tf="3m",
                execution_tf="15m",
                m15_open=now_open,
                source_closed=True,
                execution_closed=True,
                key_suffix="m3-ok",
            )
            await bot.handle_scanner_signal_sent(event)
            assert event.metadata.get("plan_created") is True
            assert event.metadata.get("execution_tf") == "15m"
            assert len(await storage.list_trade_plans()) == 1
        finally:
            await storage.close()

    run(scenario())


def test_execution_candle_open_does_not_create_plan(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            now_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            event = _event(source_tf="15m", execution_tf="15m", m15_open=now_open, source_closed=True, execution_closed=False, key_suffix="open")
            await bot.handle_scanner_signal_sent(event)
            assert event.metadata.get("plan_block_reason") == "m15_open_candle"
            assert len(await storage.list_trade_plans()) == 0
        finally:
            await storage.close()

    run(scenario())


def test_m15_direct_disabled_logs_armed_setup_only(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=False)
        try:
            m15_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            event = _event(source_tf="15m", execution_tf="15m", m15_open=m15_open, source_closed=True, execution_closed=True, key_suffix="arm")
            await bot.handle_scanner_signal_sent(event)
            assert event.metadata.get("plan_block_reason") == "armed_m15_setup_only"
            assert await storage.has_scanner_alert_state(_arm_key("BTCUSDT", "LONG", "LONG_PULLBACK", m15_open))
            assert await storage.has_scanner_alert_state(_arm_stable_key("BTCUSDT", "LONG", m15_open))
            assert len(await storage.list_trade_plans()) == 0
        finally:
            await storage.close()

    caplog.set_level(logging.INFO)
    run(scenario())
    assert "reason=armed_m15_setup_only" in caplog.text


def test_m15_closed_direct_enabled_can_create_plan(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            m15_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            await bot.handle_scanner_signal_sent(
                _event(source_tf="15m", execution_tf="15m", m15_open=m15_open, source_closed=True, execution_closed=True, key_suffix="direct")
            )
            assert len(await storage.list_trade_plans()) == 1
        finally:
            await storage.close()

    run(scenario())


def test_m15_context_without_bos_pullback_is_context_only(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            m15_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            event = _event(
                source_tf="15m",
                execution_tf="15m",
                m15_open=m15_open,
                source_closed=True,
                execution_closed=True,
                key_suffix="ctx-only",
            )
            event.metadata["structure_m15"]["bos"] = "NONE"
            event.metadata["structure_m15"]["pullback"] = "NONE"
            await bot.handle_scanner_signal_sent(event)
            assert event.metadata.get("plan_created") is False
            assert event.metadata.get("plan_block_reason") == "missing_armed_m15_setup"
            assert event.metadata.get("plan_status") == "CONTEXT_ONLY"
            assert len(await storage.list_trade_plans()) == 0
        finally:
            await storage.close()

    run(scenario())


def test_timeframe_normalization_accepts_uppercase_m15(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            m15_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            event = _event(
                source_tf="M15",
                execution_tf="M15",
                m15_open=m15_open,
                source_closed=True,
                execution_closed=True,
                key_suffix="m15-upper",
            )
            await bot.handle_scanner_signal_sent(event)
            assert event.metadata.get("source_tf") == "15m"
            assert event.metadata.get("execution_tf") == "15m"
            assert len(await storage.list_trade_plans()) == 1
        finally:
            await storage.close()

    run(scenario())


def test_trigger_after_ttl_no_plan(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path)
        try:
            m15_open = int((datetime.now(tz=timezone.utc) - timedelta(hours=1)).timestamp() * 1000)
            await storage.record_scanner_alert_state(
                key=_arm_key("BTCUSDT", "LONG", "LONG_PULLBACK", m15_open),
                symbol="BTCUSDT",
                direction="LONG",
                level="ARMED_M15_SETUP",
                trigger_tf="15m",
                closed_candle_time=str(m15_open),
            )
            event = _event(source_tf="1m", execution_tf="15m", m15_open=m15_open, source_closed=True, execution_closed=True, key_suffix="ttl")
            await bot.handle_scanner_signal_sent(event)
            assert event.metadata.get("plan_block_reason") == "armed_m15_expired"
            assert not await storage.has_scanner_alert_state(_arm_key("BTCUSDT", "LONG", "LONG_PULLBACK", m15_open))
            assert not await storage.has_scanner_alert_state(_arm_stable_key("BTCUSDT", "LONG", m15_open))
            assert len(await storage.list_trade_plans()) == 0
        finally:
            await storage.close()

    run(scenario())


def test_insert_without_plan_fingerprint_fails(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "bot.sqlite")
        await storage.initialize()
        try:
            with pytest.raises(ValueError, match="missing_plan_fingerprint"):
                await storage.create_trade_plan(
                    {
                        "symbol": "BTCUSDT",
                        "direction": "LONG",
                        "signal_type": "LONG_PULLBACK",
                        "quality": "ALTA",
                        "entry_zone_low": 99.0,
                        "entry_zone_high": 100.0,
                        "stop_loss": 95.0,
                        "tp1": 101.0,
                        "tp2": 102.0,
                        "tp3": 103.0,
                        "rr_tp1": 1.0,
                        "rr_tp2": 2.0,
                        "rr_tp3": 3.0,
                        "status": "CREATED",
                        "created_at": datetime.now(tz=timezone.utc).isoformat(),
                        "expires_at": (datetime.now(tz=timezone.utc) + timedelta(minutes=120)).isoformat(),
                    }
                )
        finally:
            await storage.close()

    run(scenario())


def test_concurrent_same_fingerprint_creates_single_plan(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            m15_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            event = _event(source_tf="15m", execution_tf="15m", m15_open=m15_open, source_closed=True, execution_closed=True, key_suffix="same")
            await asyncio.gather(bot.handle_scanner_signal_sent(event), bot.handle_scanner_signal_sent(event))
            assert len(await storage.list_trade_plans()) == 1
        finally:
            await storage.close()

    run(scenario())


def test_concurrent_different_fingerprint_same_symbol_side_creates_max_one(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            e1 = _event(source_tf="15m", execution_tf="15m", m15_open=now_ms, source_closed=True, execution_closed=True, key_suffix="a")
            e2 = _event(source_tf="15m", execution_tf="15m", m15_open=now_ms + 900000, source_closed=True, execution_closed=True, key_suffix="b")
            await asyncio.gather(bot.handle_scanner_signal_sent(e1), bot.handle_scanner_signal_sent(e2))
            assert len(await storage.list_trade_plans()) == 1
        finally:
            await storage.close()

    run(scenario())


def test_active_created_blocks_even_when_cooldown_elapsed(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            old_iso = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).isoformat()
            await storage.create_trade_plan(
                {
                    "symbol": "BTCUSDT",
                    "direction": "LONG",
                    "signal_type": "LONG_PULLBACK",
                    "quality": "ALTA",
                    "entry_zone_low": 99.0,
                    "entry_zone_high": 100.0,
                    "stop_loss": 95.0,
                    "tp1": 101.0,
                    "tp2": 102.0,
                    "tp3": 103.0,
                    "rr_tp1": 1.0,
                    "rr_tp2": 2.0,
                    "rr_tp3": 3.0,
                    "status": "CREATED",
                    "created_at": old_iso,
                    "expires_at": (datetime.now(tz=timezone.utc) + timedelta(minutes=120)).isoformat(),
                    "source_alert_id": "old",
                    "plan_fingerprint": "old-created",
                }
            )
            await bot.handle_scanner_signal_sent(
                _event(source_tf="15m", execution_tf="15m", m15_open=int(datetime.now(tz=timezone.utc).timestamp() * 1000), source_closed=True, execution_closed=True, key_suffix="new")
            )
            assert len(await storage.list_trade_plans()) == 1
        finally:
            await storage.close()

    run(scenario())


def test_non_active_statuses_do_not_block_active_guard(tmp_path: Path) -> None:
    async def scenario() -> None:
        statuses = ["INVALIDATED", "EXPIRED", "RESOLVED", "CANCELLED"]
        for status in statuses:
            storage = Storage(tmp_path / f"{status}.sqlite")
            await storage.initialize()
            bot = TradingAlertBot(Path("."))
            bot.logger = logging.getLogger(f"test.plan.{status}")
            bot.storage = storage
            bot.trade_planner_enabled = True
            bot.enable_direct_m15_plans = True

            async def fake_dispatch(events):
                return len(events)

            bot._dispatch_alerts = fake_dispatch  # type: ignore[method-assign]
            try:
                await storage.create_trade_plan(
                    {
                        "symbol": "BTCUSDT",
                        "direction": "LONG",
                        "signal_type": "LONG_PULLBACK",
                        "quality": "ALTA",
                        "entry_zone_low": 99.0,
                        "entry_zone_high": 100.0,
                        "stop_loss": 95.0,
                        "tp1": 101.0,
                        "tp2": 102.0,
                        "tp3": 103.0,
                        "rr_tp1": 1.0,
                        "rr_tp2": 2.0,
                        "rr_tp3": 3.0,
                        "status": status,
                        "created_at": (datetime.now(tz=timezone.utc) - timedelta(hours=1)).isoformat(),
                        "expires_at": (datetime.now(tz=timezone.utc) + timedelta(minutes=120)).isoformat(),
                        "source_alert_id": f"old-{status}",
                        "plan_fingerprint": f"old-{status}",
                    }
                )
                await bot.handle_scanner_signal_sent(
                    _event(source_tf="15m", execution_tf="15m", m15_open=int(datetime.now(tz=timezone.utc).timestamp() * 1000), source_closed=True, execution_closed=True, key_suffix=status)
                )
                assert len(await storage.list_trade_plans()) == 2
            finally:
                await storage.close()

    run(scenario())


def test_cooldown_same_symbol_opposite_side_does_not_block(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            created_at = datetime.now(tz=timezone.utc).isoformat()
            await storage.create_trade_plan(
                {
                    "symbol": "BTCUSDT",
                    "direction": "LONG",
                    "signal_type": "LONG_PULLBACK",
                    "quality": "ALTA",
                    "entry_zone_low": 99.0,
                    "entry_zone_high": 100.0,
                    "stop_loss": 95.0,
                    "tp1": 101.0,
                    "tp2": 102.0,
                    "tp3": 103.0,
                    "rr_tp1": 1.0,
                    "rr_tp2": 2.0,
                    "rr_tp3": 3.0,
                    "status": "RESOLVED",
                    "created_at": created_at,
                    "expires_at": (datetime.now(tz=timezone.utc) + timedelta(minutes=120)).isoformat(),
                    "source_alert_id": "cooldown-long",
                    "plan_fingerprint": "cooldown-long",
                }
            )
            await bot.handle_scanner_signal_sent(
                _event(
                    source_tf="15m",
                    execution_tf="15m",
                    m15_open=int(datetime.now(tz=timezone.utc).timestamp() * 1000),
                    source_closed=True,
                    execution_closed=True,
                    key_suffix="short",
                    direction="SHORT",
                    signal_type="SHORT_PULLBACK",
                )
            )
            assert len(await storage.list_trade_plans()) == 2
        finally:
            await storage.close()

    run(scenario())


def test_lower_tf_can_consume_stable_arm_key_when_signal_type_changes(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path)
        try:
            now_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            await storage.record_scanner_alert_state(
                key=_arm_stable_key("BTCUSDT", "LONG", now_open),
                symbol="BTCUSDT",
                direction="LONG",
                level="ARMED_M15_SETUP",
                trigger_tf="15m",
                closed_candle_time=str(now_open),
            )
            event = _event(
                source_tf="1m",
                execution_tf="15m",
                m15_open=now_open,
                source_closed=True,
                execution_closed=True,
                key_suffix="stable-key",
                direction="LONG",
                signal_type="LONG_CONTINUATION",
            )
            await bot.handle_scanner_signal_sent(event)
            assert event.metadata.get("plan_created") is True
            assert len(await storage.list_trade_plans()) == 1
            assert not await storage.has_scanner_alert_state(_arm_stable_key("BTCUSDT", "LONG", now_open))
        finally:
            await storage.close()

    run(scenario())


def test_cooldown_created_less_than_15m_blocks(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            created_at = datetime.now(tz=timezone.utc).isoformat()
            await storage.create_trade_plan(
                {
                    "symbol": "BTCUSDT",
                    "direction": "LONG",
                    "signal_type": "LONG_PULLBACK",
                    "quality": "ALTA",
                    "entry_zone_low": 99.0,
                    "entry_zone_high": 100.0,
                    "stop_loss": 95.0,
                    "tp1": 101.0,
                    "tp2": 102.0,
                    "tp3": 103.0,
                    "rr_tp1": 1.0,
                    "rr_tp2": 2.0,
                    "rr_tp3": 3.0,
                    "status": "RESOLVED",
                    "created_at": created_at,
                    "expires_at": (datetime.now(tz=timezone.utc) + timedelta(minutes=120)).isoformat(),
                    "source_alert_id": "cooldown",
                    "plan_fingerprint": "cooldown-plan",
                }
            )
            await bot.handle_scanner_signal_sent(
                _event(source_tf="15m", execution_tf="15m", m15_open=int(datetime.now(tz=timezone.utc).timestamp() * 1000), source_closed=True, execution_closed=True, key_suffix="cd")
            )
            assert len(await storage.list_trade_plans()) == 1
        finally:
            await storage.close()

    run(scenario())


def test_cooldown_over_15m_allows_if_no_active(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=True)
        try:
            old_iso = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).isoformat()
            await storage.create_trade_plan(
                {
                    "symbol": "BTCUSDT",
                    "direction": "LONG",
                    "signal_type": "LONG_PULLBACK",
                    "quality": "ALTA",
                    "entry_zone_low": 99.0,
                    "entry_zone_high": 100.0,
                    "stop_loss": 95.0,
                    "tp1": 101.0,
                    "tp2": 102.0,
                    "tp3": 103.0,
                    "rr_tp1": 1.0,
                    "rr_tp2": 2.0,
                    "rr_tp3": 3.0,
                    "status": "RESOLVED",
                    "created_at": old_iso,
                    "expires_at": (datetime.now(tz=timezone.utc) + timedelta(minutes=120)).isoformat(),
                    "source_alert_id": "old",
                    "plan_fingerprint": "old-resolved",
                }
            )
            await bot.handle_scanner_signal_sent(
                _event(source_tf="15m", execution_tf="15m", m15_open=int(datetime.now(tz=timezone.utc).timestamp() * 1000), source_closed=True, execution_closed=True, key_suffix="allow")
            )
            assert len(await storage.list_trade_plans()) == 2
        finally:
            await storage.close()

    run(scenario())


def test_recovery_same_signal_not_duplicated(tmp_path: Path) -> None:
    async def scenario() -> None:
        db_path = tmp_path / "recovery.sqlite"

        storage1 = Storage(db_path)
        await storage1.initialize()
        bot1 = TradingAlertBot(Path("."))
        bot1.logger = logging.getLogger("test.plan.recovery.1")
        bot1.storage = storage1
        bot1.trade_planner_enabled = True
        bot1.enable_direct_m15_plans = True

        async def fake_dispatch(events):
            return len(events)

        bot1._dispatch_alerts = fake_dispatch  # type: ignore[method-assign]
        open_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        event = _event(source_tf="15m", execution_tf="15m", m15_open=open_ms, source_closed=True, execution_closed=True, key_suffix="recovery")
        await bot1.handle_scanner_signal_sent(event)
        await storage1.close()

        storage2 = Storage(db_path)
        await storage2.initialize()
        bot2 = TradingAlertBot(Path("."))
        bot2.logger = logging.getLogger("test.plan.recovery.2")
        bot2.storage = storage2
        bot2.trade_planner_enabled = True
        bot2.enable_direct_m15_plans = True
        bot2._dispatch_alerts = fake_dispatch  # type: ignore[method-assign]
        await bot2.handle_scanner_signal_sent(event)
        plans = await storage2.list_trade_plans()
        assert len(plans) == 1
        await storage2.close()

    run(scenario())


def test_trigger_m1_uses_m15_setup_levels_to_create_plan(tmp_path: Path) -> None:
    async def scenario() -> None:
        bot, storage = await _build_bot(tmp_path, enable_direct_m15_plans=False)
        try:
            m15_open = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
            await bot.handle_scanner_signal_sent(
                _event(source_tf="15m", execution_tf="15m", m15_open=m15_open, source_closed=True, execution_closed=True, key_suffix="setup")
            )
            trigger = _event(source_tf="1m", execution_tf="15m", m15_open=m15_open, source_closed=True, execution_closed=True, key_suffix="trigger")
            await bot.handle_scanner_signal_sent(trigger)
            plans = await storage.list_trade_plans()
            assert len(plans) == 1
            plan = plans[0]
            assert trigger.metadata.get("plan_created") is True
            assert trigger.metadata.get("execution_tf") == "15m"
            expected = build_trade_plan(
                symbol="BTCUSDT",
                quality_payload=_quality("LONG", "LONG_PULLBACK"),
                current_price=100.0,
                structure_m15=_structure("LONG"),
            )
            assert expected is not None
            assert float(plan["entry_low"]) == pytest.approx(expected.entry_zone_low)
            assert float(plan["entry_high"]) == pytest.approx(expected.entry_zone_high)
            assert float(plan["stop_loss"]) == pytest.approx(expected.stop_loss)
            assert float(plan["tp1"]) == pytest.approx(expected.tp1)
            assert float(plan["tp2"]) == pytest.approx(expected.tp2)
            assert float(plan["tp3"]) == pytest.approx(expected.tp3)
        finally:
            await storage.close()

    run(scenario())

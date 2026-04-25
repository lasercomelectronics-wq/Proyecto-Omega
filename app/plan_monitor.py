from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Awaitable, Callable

from app.binance_client import BinanceFuturesClient
from app.models import AlertEvent, AlertPriority
from app.storage import Storage

DispatchAlerts = Callable[[list[AlertEvent]], Awaitable[int]]


class PlanMonitor:
    def __init__(
        self,
        *,
        storage: Storage,
        binance_client: BinanceFuturesClient,
        dispatch_alerts: DispatchAlerts,
        logger: logging.Logger,
        interval_seconds: int = 15,
    ) -> None:
        self._storage = storage
        self._binance_client = binance_client
        self._dispatch_alerts = dispatch_alerts
        self._logger = logger
        self._interval_seconds = max(10, int(interval_seconds))
        self._stop_event = asyncio.Event()
        self._last_cycle_at: str | None = None
        self._last_error: str | None = None
        self._events_sent = 0
        self._last_open_plans_count = 0

    async def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        self._logger.info("Plan monitor start | interval=%ss", self._interval_seconds)
        while not self._stop_event.is_set():
            try:
                self._logger.info("Plan monitor cycle start")
                await self._tick()
                self._last_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._last_error = str(exc)
                self._logger.exception("Plan monitor error: %s", exc)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except asyncio.TimeoutError:
                continue
        self._logger.info("Plan monitor stop")

    async def _tick(self) -> None:
        plans = await self._storage.list_open_trade_plans()
        self._last_open_plans_count = len(plans)
        self._last_cycle_at = datetime.now(tz=timezone.utc).isoformat()
        self._logger.info("Plan monitor open plans=%s", len(plans))
        for plan in plans:
            plan_id = int(plan["id"])
            symbol = str(plan["symbol"])
            direction = str(plan["direction"])
            status = str(plan["status"])
            expires_at = datetime.fromisoformat(str(plan["expires_at"]))
            now = datetime.now(tz=timezone.utc)

            if status == "CREATED" and now > expires_at:
                if await self._storage.record_trade_plan_event(plan_id, "EXPIRED", None, {"reason": "timeout"}):
                    await self._storage.update_trade_plan_status(plan_id, "EXPIRED")
                    await self._send_plan_alert(symbol, f"⚪⏳ {symbol} — Plan expirado\n\nPlan: #{plan_id}\nMotivo: no tocó entrada en tiempo definido.")
                continue

            price, _ = await self._binance_client.get_mark_price(symbol)
            self._logger.info("Plan monitor price fetched | %s %.6f", symbol, price)
            entry_low = float(plan["entry_low"])
            entry_high = float(plan["entry_high"])
            tp1 = float(plan["tp1"])
            tp2 = float(plan["tp2"])
            tp3 = float(plan["tp3"])
            sl = float(plan["stop_loss"])

            in_entry = entry_low <= price <= entry_high
            if in_entry and status == "CREATED":
                if await self._storage.record_trade_plan_event(plan_id, "ENTRY_TOUCHED", price, {"status": status}):
                    self._logger.info("Plan monitor event detected | plan=%s event=ENTRY_TOUCHED", plan_id)
                    await self._storage.update_trade_plan_status(plan_id, "ENTRY_TOUCHED")
                    await self._send_plan_alert(symbol, f"🟡📍 {symbol} — Zona de entrada tocada\n\nPlan: #{plan_id}\nPrecio: {price:.6f}\nEntrada: {entry_low:.6f}-{entry_high:.6f}\n\n⚠ No orden automática.")
                    status = "ENTRY_TOUCHED"

            if direction == "LONG":
                await self._handle_hits_long(
                    plan_id,
                    symbol,
                    price,
                    tp1,
                    tp2,
                    tp3,
                    sl,
                    created_at=str(plan.get("created_at", "")),
                )
            else:
                await self._handle_hits_short(
                    plan_id,
                    symbol,
                    price,
                    tp1,
                    tp2,
                    tp3,
                    sl,
                    created_at=str(plan.get("created_at", "")),
                )

    async def _handle_hits_long(
        self,
        plan_id: int,
        symbol: str,
        price: float,
        tp1: float,
        tp2: float,
        tp3: float,
        sl: float,
        *,
        created_at: str,
    ) -> None:
        reached_levels = self._tp_reached_levels_long(price=price, tp1=tp1, tp2=tp2, tp3=tp3)
        if reached_levels and price <= sl:
            resolution, resolved_levels, reason = await self._resolve_tp_sl_order_with_candles(
                symbol=symbol,
                direction="LONG",
                tp1=tp1,
                tp2=tp2,
                tp3=tp3,
                sl=sl,
                created_at=created_at,
            )
            if resolution == "tp_first":
                await self._process_tp_hits(
                    plan_id=plan_id,
                    symbol=symbol,
                    direction="LONG",
                    price=price,
                    reached_levels=resolved_levels or reached_levels,
                )
                return
            if resolution == "sl_first":
                if await self._storage.record_trade_plan_event(plan_id, "SL_HIT", price, {}):
                    self._logger.info("Plan monitor event detected | plan=%s event=SL_HIT", plan_id)
                    await self._storage.update_trade_plan_status(plan_id, "SL_HIT")
                    await self._send_plan_alert(symbol, f"🔴🛑 {symbol} — SL alcanzado\n\nPlan: #{plan_id}\nResultado: invalidación técnica.\n⚠ Solo alerta. No orden.")
                return
            await self._handle_ambiguous_tp_sl(
                plan_id=plan_id,
                symbol=symbol,
                direction="LONG",
                price=price,
                reached_levels=reached_levels,
                sl=sl,
                reason=reason,
            )
            return
        await self._process_tp_hits(
            plan_id=plan_id,
            symbol=symbol,
            direction="LONG",
            price=price,
            reached_levels=reached_levels,
        )
        if price <= sl and await self._storage.record_trade_plan_event(plan_id, "SL_HIT", price, {}):
            self._logger.info("Plan monitor event detected | plan=%s event=SL_HIT", plan_id)
            await self._storage.update_trade_plan_status(plan_id, "SL_HIT")
            await self._send_plan_alert(symbol, f"🔴🛑 {symbol} — SL alcanzado\n\nPlan: #{plan_id}\nResultado: invalidación técnica.\n⚠ Solo alerta. No orden.")

    async def _handle_hits_short(
        self,
        plan_id: int,
        symbol: str,
        price: float,
        tp1: float,
        tp2: float,
        tp3: float,
        sl: float,
        *,
        created_at: str,
    ) -> None:
        reached_levels = self._tp_reached_levels_short(price=price, tp1=tp1, tp2=tp2, tp3=tp3)
        if reached_levels and price >= sl:
            resolution, resolved_levels, reason = await self._resolve_tp_sl_order_with_candles(
                symbol=symbol,
                direction="SHORT",
                tp1=tp1,
                tp2=tp2,
                tp3=tp3,
                sl=sl,
                created_at=created_at,
            )
            if resolution == "tp_first":
                await self._process_tp_hits(
                    plan_id=plan_id,
                    symbol=symbol,
                    direction="SHORT",
                    price=price,
                    reached_levels=resolved_levels or reached_levels,
                )
                return
            if resolution == "sl_first":
                if await self._storage.record_trade_plan_event(plan_id, "SL_HIT", price, {}):
                    self._logger.info("Plan monitor event detected | plan=%s event=SL_HIT", plan_id)
                    await self._storage.update_trade_plan_status(plan_id, "SL_HIT")
                    await self._send_plan_alert(symbol, f"🔴🛑 {symbol} — SL alcanzado\n\nPlan: #{plan_id}\nResultado: invalidación técnica.\n⚠ Solo alerta. No orden.")
                return
            await self._handle_ambiguous_tp_sl(
                plan_id=plan_id,
                symbol=symbol,
                direction="SHORT",
                price=price,
                reached_levels=reached_levels,
                sl=sl,
                reason=reason,
            )
            return
        await self._process_tp_hits(
            plan_id=plan_id,
            symbol=symbol,
            direction="SHORT",
            price=price,
            reached_levels=reached_levels,
        )
        if price >= sl and await self._storage.record_trade_plan_event(plan_id, "SL_HIT", price, {}):
            self._logger.info("Plan monitor event detected | plan=%s event=SL_HIT", plan_id)
            await self._storage.update_trade_plan_status(plan_id, "SL_HIT")
            await self._send_plan_alert(symbol, f"🔴🛑 {symbol} — SL alcanzado\n\nPlan: #{plan_id}\nResultado: invalidación técnica.\n⚠ Solo alerta. No orden.")

    async def _handle_ambiguous_tp_sl(
        self,
        *,
        plan_id: int,
        symbol: str,
        direction: str,
        price: float,
        reached_levels: list[tuple[str, int]],
        sl: float,
        reason: str,
    ) -> None:
        inserted = await self._storage.record_trade_plan_event(
            plan_id,
            "AMBIGUOUS_TP_SL",
            price,
            {
                "policy": "unknown_ambiguous",
                "resolution_tf": "3m",
                "reason": reason,
                "direction": direction,
                "reached_tp_levels": [level for _, level in reached_levels],
                "stop_loss": sl,
            },
        )
        if not inserted:
            return
        self._logger.info("Plan monitor event detected | plan=%s event=AMBIGUOUS_TP_SL", plan_id)
        await self._storage.update_trade_plan_status(plan_id, "TP_SL_AMBIGUOUS")
        max_tp = reached_levels[-1][1]
        crossed = ", ".join(f"TP{level}" for _, level in reached_levels)
        await self._send_plan_alert(
            symbol,
            (
                f"⚪⚠️ {symbol} {direction} — TP/SL ambiguo en mismo ciclo\n\n"
                f"Plan: #{plan_id}\n"
                f"Precio: {price:.6f}\n"
                f"TP detectados: {crossed} (máximo TP{max_tp})\n"
                f"SL detectado: {sl:.6f}\n"
                f"Reason: {reason}\n"
                "Política: unknown_ambiguous (sin orden observable).\n"
                "⚠ Solo alerta. No orden."
            ),
        )

    async def _resolve_tp_sl_order_with_candles(
        self,
        *,
        symbol: str,
        direction: str,
        tp1: float,
        tp2: float,
        tp3: float,
        sl: float,
        created_at: str,
    ) -> tuple[str, list[tuple[str, int]], str]:
        try:
            candles = await self._binance_client.get_klines(symbol, interval="3m", limit=200)
        except Exception:
            return "resolution_error", [], "m3_fetch_error"

        if not candles:
            return "insufficient_data", [], "insufficient_3m_data"

        try:
            created_at_dt = datetime.fromisoformat(created_at)
            if created_at_dt.tzinfo is None:
                created_at_dt = created_at_dt.replace(tzinfo=timezone.utc)
            created_at_ms = int(created_at_dt.timestamp() * 1000)
        except ValueError:
            created_at_ms = 0

        now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        closed_all = [c for c in candles if int(c.close_time) <= now_ms]
        closed_all.sort(key=lambda candle: candle.open_time)
        closed = [c for c in closed_all if int(c.close_time) >= created_at_ms] if created_at_ms > 0 else list(closed_all)
        if not closed:
            closed = closed_all
        if not closed:
            return "insufficient_data", [], "insufficient_3m_data"

        for candle in closed:
            if direction == "LONG":
                reached_levels = self._tp_reached_levels_long(price=float(candle.high), tp1=tp1, tp2=tp2, tp3=tp3)
                sl_hit = float(candle.low) <= sl
            else:
                reached_levels = self._tp_reached_levels_short(price=float(candle.low), tp1=tp1, tp2=tp2, tp3=tp3)
                sl_hit = float(candle.high) >= sl

            if reached_levels and not sl_hit:
                return "tp_first", reached_levels, "tp_first_resolved_3m"
            if sl_hit and not reached_levels:
                return "sl_first", [], "sl_first_resolved_3m"
            if reached_levels and sl_hit:
                return "ambiguous_same_candle", reached_levels, "same_3m_candle"

        return "insufficient_data", [], "insufficient_3m_data"

    @staticmethod
    def _tp_reached_levels_long(*, price: float, tp1: float, tp2: float, tp3: float) -> list[tuple[str, int]]:
        if price >= tp3:
            return [("TP1_HIT", 1), ("TP2_HIT", 2), ("TP3_HIT", 3)]
        if price >= tp2:
            return [("TP1_HIT", 1), ("TP2_HIT", 2)]
        if price >= tp1:
            return [("TP1_HIT", 1)]
        return []

    @staticmethod
    def _tp_reached_levels_short(*, price: float, tp1: float, tp2: float, tp3: float) -> list[tuple[str, int]]:
        if price <= tp3:
            return [("TP1_HIT", 1), ("TP2_HIT", 2), ("TP3_HIT", 3)]
        if price <= tp2:
            return [("TP1_HIT", 1), ("TP2_HIT", 2)]
        if price <= tp1:
            return [("TP1_HIT", 1)]
        return []

    async def _process_tp_hits(
        self,
        *,
        plan_id: int,
        symbol: str,
        direction: str,
        price: float,
        reached_levels: list[tuple[str, int]],
    ) -> None:
        if not reached_levels:
            return

        inserted_levels: list[tuple[str, int]] = []
        for event_type, level in reached_levels:
            if await self._storage.record_trade_plan_event(plan_id, event_type, price, {}):
                self._logger.info("Plan monitor event detected | plan=%s event=%s", plan_id, event_type)
                inserted_levels.append((event_type, level))

        if not inserted_levels:
            return

        max_event_type, max_level = inserted_levels[-1]
        await self._storage.update_trade_plan_status(plan_id, max_event_type)

        if len(inserted_levels) == 1:
            await self._send_plan_alert(symbol, self._single_tp_alert_text(plan_id=plan_id, symbol=symbol, price=price, level=max_level))
            return

        crossed = ", ".join(f"TP{level}" for _, level in inserted_levels)
        await self._send_plan_alert(
            symbol,
            (
                f"🎯 {symbol} {direction} — TP{max_level} alcanzado\n\n"
                f"Plan: #{plan_id}\n"
                f"Precio: {price:.6f}\n"
                f"Cruces en este ciclo: {crossed}\n"
                "⚠ Solo alerta. No orden."
            ),
        )

    @staticmethod
    def _single_tp_alert_text(*, plan_id: int, symbol: str, price: float, level: int) -> str:
        if level == 1:
            return f"🟢🎯 {symbol} — TP1 alcanzado\n\nPlan: #{plan_id}\nTP1 HIT | Precio: {price:.6f}\n⚠ Solo alerta. No orden."
        if level == 2:
            return f"🟢🚀 {symbol} — TP2 alcanzado\n\nPlan: #{plan_id}\nTP2 HIT | Precio: {price:.6f}\n⚠ Solo alerta."
        return f"🏆🔥 {symbol} — TP3 alcanzado\n\nPlan: #{plan_id}\nResultado: plan completado.\n⚠ Solo alerta."

    async def _send_plan_alert(self, symbol: str, text: str) -> None:
        event = AlertEvent(
            key=f"plan::{symbol}::{hash(text)}",
            priority=AlertPriority.INFO,
            reason="Trade plan monitor",
            symbol=symbol,
            note=text,
            cooldown_seconds=0,
        )
        sent = await self._dispatch_alerts([event])
        self._events_sent += sent

    def get_status(self) -> dict[str, object]:
        return {
            "enabled": not self._stop_event.is_set(),
            "interval_seconds": self._interval_seconds,
            "open_plans_count": self._last_open_plans_count,
            "events_sent": self._events_sent,
            "last_cycle_at": self._last_cycle_at,
            "price_source": "mark_price",
            "alerts_enabled": True,
            "last_error": self._last_error,
        }

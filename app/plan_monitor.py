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

    async def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        self._logger.info("Plan monitor start | interval=%ss", self._interval_seconds)
        while not self._stop_event.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.exception("Plan monitor error: %s", exc)
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except asyncio.TimeoutError:
                continue
        self._logger.info("Plan monitor stop")

    async def _tick(self) -> None:
        plans = await self._storage.list_open_trade_plans()
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
            entry_low = float(plan["entry_low"])
            entry_high = float(plan["entry_high"])
            tp1 = float(plan["tp1"])
            tp2 = float(plan["tp2"])
            tp3 = float(plan["tp3"])
            sl = float(plan["stop_loss"])

            in_entry = entry_low <= price <= entry_high
            if in_entry and status == "CREATED":
                if await self._storage.record_trade_plan_event(plan_id, "ENTRY_TOUCHED", price, {"status": status}):
                    await self._storage.update_trade_plan_status(plan_id, "ENTRY_TOUCHED")
                    await self._send_plan_alert(symbol, f"🟡📍 {symbol} — Zona de entrada tocada\n\nPlan: #{plan_id}\nPrecio: {price:.6f}\nEntrada: {entry_low:.6f}-{entry_high:.6f}\n\n⚠ No orden automática.")
                    status = "ENTRY_TOUCHED"

            if direction == "LONG":
                await self._handle_hits_long(plan_id, symbol, price, tp1, tp2, tp3, sl)
            else:
                await self._handle_hits_short(plan_id, symbol, price, tp1, tp2, tp3, sl)

    async def _handle_hits_long(self, plan_id: int, symbol: str, price: float, tp1: float, tp2: float, tp3: float, sl: float) -> None:
        if price >= tp1 and await self._storage.record_trade_plan_event(plan_id, "TP1_HIT", price, {}):
            await self._storage.update_trade_plan_status(plan_id, "TP1_HIT")
            await self._send_plan_alert(symbol, f"🟢🎯 {symbol} — TP1 alcanzado\n\nPlan: #{plan_id}\nTP1 HIT | Precio: {price:.6f}\n⚠ Solo alerta. No orden.")
        if price >= tp2 and await self._storage.record_trade_plan_event(plan_id, "TP2_HIT", price, {}):
            await self._storage.update_trade_plan_status(plan_id, "TP2_HIT")
            await self._send_plan_alert(symbol, f"🟢🚀 {symbol} — TP2 alcanzado\n\nPlan: #{plan_id}\nTP2 HIT | Precio: {price:.6f}\n⚠ Solo alerta.")
        if price >= tp3 and await self._storage.record_trade_plan_event(plan_id, "TP3_HIT", price, {}):
            await self._storage.update_trade_plan_status(plan_id, "TP3_HIT")
            await self._send_plan_alert(symbol, f"🏆🔥 {symbol} — TP3 alcanzado\n\nPlan: #{plan_id}\nResultado: plan completado.\n⚠ Solo alerta.")
        if price <= sl and await self._storage.record_trade_plan_event(plan_id, "SL_HIT", price, {}):
            await self._storage.update_trade_plan_status(plan_id, "SL_HIT")
            await self._send_plan_alert(symbol, f"🔴🛑 {symbol} — SL alcanzado\n\nPlan: #{plan_id}\nResultado: invalidación técnica.\n⚠ Solo alerta. No orden.")

    async def _handle_hits_short(self, plan_id: int, symbol: str, price: float, tp1: float, tp2: float, tp3: float, sl: float) -> None:
        if price <= tp1 and await self._storage.record_trade_plan_event(plan_id, "TP1_HIT", price, {}):
            await self._storage.update_trade_plan_status(plan_id, "TP1_HIT")
            await self._send_plan_alert(symbol, f"🟢🎯 {symbol} — TP1 alcanzado\n\nPlan: #{plan_id}\nTP1 HIT | Precio: {price:.6f}\n⚠ Solo alerta. No orden.")
        if price <= tp2 and await self._storage.record_trade_plan_event(plan_id, "TP2_HIT", price, {}):
            await self._storage.update_trade_plan_status(plan_id, "TP2_HIT")
            await self._send_plan_alert(symbol, f"🟢🚀 {symbol} — TP2 alcanzado\n\nPlan: #{plan_id}\nTP2 HIT | Precio: {price:.6f}\n⚠ Solo alerta.")
        if price <= tp3 and await self._storage.record_trade_plan_event(plan_id, "TP3_HIT", price, {}):
            await self._storage.update_trade_plan_status(plan_id, "TP3_HIT")
            await self._send_plan_alert(symbol, f"🏆🔥 {symbol} — TP3 alcanzado\n\nPlan: #{plan_id}\nResultado: plan completado.\n⚠ Solo alerta.")
        if price >= sl and await self._storage.record_trade_plan_event(plan_id, "SL_HIT", price, {}):
            await self._storage.update_trade_plan_status(plan_id, "SL_HIT")
            await self._send_plan_alert(symbol, f"🔴🛑 {symbol} — SL alcanzado\n\nPlan: #{plan_id}\nResultado: invalidación técnica.\n⚠ Solo alerta. No orden.")

    async def _send_plan_alert(self, symbol: str, text: str) -> None:
        event = AlertEvent(
            key=f"plan::{symbol}::{hash(text)}",
            priority=AlertPriority.INFO,
            reason="Trade plan monitor",
            symbol=symbol,
            note=text,
            cooldown_seconds=0,
        )
        await self._dispatch_alerts([event])

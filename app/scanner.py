from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Awaitable, Callable

from app.binance_client import BinanceAPIError, BinanceFuturesClient
from app.models import AlertEvent, AlertPriority, Candle, TradeSide
from app.storage import Storage
from app.strategy_engine import (
    analyze_adx_dmi,
    analyze_ema_signal,
    analyze_koncorde_lite,
    analyze_macd,
    analyze_sqzmom,
    evaluate_signal_quality,
)
from app.structure import analyze_structure
from app.trade_manager import TradeManager

DispatchAlerts = Callable[[list[AlertEvent]], Awaitable[int]]
SignalSentCallback = Callable[[AlertEvent], Awaitable[None]]


@dataclass(slots=True)
class ScannerSignal:
    event: AlertEvent
    direction: str
    level: str
    trigger_tf: str
    closed_candle_time: int


class SignalScanner:
    def __init__(
        self,
        *,
        binance_client: BinanceFuturesClient,
        trade_manager: TradeManager,
        storage: Storage,
        dispatch_alerts: DispatchAlerts,
        logger: logging.Logger,
        interval_seconds: int = 60,
        alerts_enabled: bool = True,
        structure_enabled: bool = True,
        structure_timeframes: tuple[str, ...] = ("15m", "5m", "3m"),
        pivot_window: int = 3,
        pullback_tolerance_mode: str = "atr",
        pullback_atr_mult: float = 0.25,
        pullback_pct: float = 0.15,
        on_signal_sent: SignalSentCallback | None = None,
    ) -> None:
        self._binance_client = binance_client
        self._trade_manager = trade_manager
        self._storage = storage
        self._dispatch_alerts = dispatch_alerts
        self._logger = logger
        self._interval_seconds = max(10, int(interval_seconds))
        self._alerts_enabled = alerts_enabled
        self._structure_enabled = structure_enabled
        self._structure_timeframes = tuple(structure_timeframes)
        self._pivot_window = max(2, int(pivot_window))
        self._pullback_tolerance_mode = pullback_tolerance_mode
        self._pullback_atr_mult = pullback_atr_mult
        self._pullback_pct = pullback_pct
        self._on_signal_sent = on_signal_sent
        self._stop_event = asyncio.Event()
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running and not self._stop_event.is_set()

    async def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        self._running = True
        self._logger.info(
            "Scanner start | alerts=%s | interval=%ss",
            self._alerts_enabled,
            self._interval_seconds,
        )
        try:
            while not self._stop_event.is_set():
                try:
                    await self.scan_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self._logger.exception("Scanner error")

                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
                except asyncio.TimeoutError:
                    continue
        finally:
            self._running = False
            self._logger.info("Scanner stop")

    async def scan_once(self) -> None:
        symbols = self._trade_manager.get_watchlist_symbols()
        self._logger.info("Scanner WL cargada: %s", ", ".join(symbols) if symbols else "vacia")
        if not symbols:
            return

        for symbol in symbols:
            try:
                signals = await self._scan_symbol(symbol)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._logger.warning("Scanner error %s: %s", symbol, exc)
                continue

            for signal in signals:
                if await self._storage.has_scanner_alert_state(signal.event.key):
                    self._logger.info("Scanner skip cooldown: %s", signal.event.key)
                    continue

                self._logger.info(
                    "Scanner señal detectada: %s %s %s",
                    symbol,
                    signal.direction,
                    signal.level,
                )
                if not self._alerts_enabled:
                    continue

                sent_count = await self._dispatch_alerts([signal.event])
                if sent_count <= 0:
                    continue

                await self._storage.record_scanner_alert_state(
                    key=signal.event.key,
                    symbol=symbol,
                    direction=signal.direction,
                    level=signal.level,
                    trigger_tf=signal.trigger_tf,
                    closed_candle_time=str(signal.closed_candle_time),
                )
                self._logger.info("Scanner alerta enviada: %s", signal.event.key)
                if self._on_signal_sent is not None:
                    try:
                        await self._on_signal_sent(signal.event)
                    except Exception as exc:
                        self._logger.warning("plan callback error %s: %s", symbol, exc)

    async def _scan_symbol(self, symbol: str) -> list[ScannerSignal]:
        candles_by_tf: dict[str, list[Candle]] = {}
        tf_directions: dict[str, str] = {}
        tf_rows: dict[str, str] = {}
        m15_ema = None
        koncorde_m15 = None
        structure_by_tf: dict[str, dict[str, str | bool | float | None]] = {}
        adx_by_tf: dict[str, dict[str, float]] = {}
        macd_by_tf: dict[str, dict[str, float]] = {}
        sqz_by_tf: dict[str, dict[str, float | bool]] = {}

        for interval, label in (("15m", "M15"), ("5m", "M5"), ("3m", "M3"), ("1m", "M1")):
            candles = await self._binance_client.get_klines(symbol, interval=interval, limit=210)
            candles_by_tf[interval] = candles
            ema_result = analyze_ema_signal(candles, fast_period=55, slow_period=200)
            adx = analyze_adx_dmi(candles)
            macd = analyze_macd([candle.close for candle in candles[:-1]])
            sqz = analyze_sqzmom(candles)
            adx_by_tf[interval] = adx
            macd_by_tf[interval] = macd
            sqz_by_tf[interval] = sqz

            direction = "NONE"
            if ema_result["trend"] == "BULL":
                direction = "LONG"
            elif ema_result["trend"] == "BEAR":
                direction = "SHORT"
            tf_directions[interval] = direction

            trend_icon = {"LONG": "🟢", "SHORT": "🔴", "NONE": "⚪"}[direction]
            sqz_icon = "⚪"
            if sqz["strong_bull"]:
                sqz_icon = "🟢"
            elif sqz["strong_bear"]:
                sqz_icon = "🔴"
            macd_icon = "⚪"
            if macd["histogram"] > 0:
                macd_icon = "🟢"
            elif macd["histogram"] < 0:
                macd_icon = "🔴"
            tf_rows[interval] = (
                f"{label} {trend_icon} ADX {adx['adx']:.0f} | SQZ {sqz_icon} | MACD {macd_icon}"
                if direction != "NONE"
                else f"{label} ⚪ neutral"
            )

            if interval == "15m":
                m15_ema = ema_result
                koncorde_m15 = analyze_koncorde_lite(candles)
            if self._structure_enabled and interval in self._structure_timeframes:
                try:
                    structure_by_tf[interval] = analyze_structure(
                        candles,
                        pivot_window=self._pivot_window,
                        pullback_tolerance_mode=self._pullback_tolerance_mode,
                        pullback_atr_mult=self._pullback_atr_mult,
                        pullback_pct=self._pullback_pct,
                        logger=self._logger,
                    )
                    self._logger.info(
                        "Structure score | %s %s bias=%s bos=%s choch=%s pullback=%s",
                        symbol,
                        interval,
                        structure_by_tf[interval].get("bias"),
                        structure_by_tf[interval].get("bos"),
                        structure_by_tf[interval].get("choch"),
                        structure_by_tf[interval].get("pullback"),
                    )
                except Exception as exc:
                    self._logger.warning("Structure error %s %s: %s", symbol, interval, exc)
                    structure_by_tf[interval] = {
                        "bias": "MIX",
                        "bos": "NONE",
                        "choch": "NONE",
                        "pullback": "NONE",
                        "summary": "error estructura",
                    }

        assert m15_ema is not None and koncorde_m15 is not None
        quality = evaluate_signal_quality(
            tf_directions=tf_directions,
            m15_ema=m15_ema,
            koncorde_m15=koncorde_m15,
            adx_m15=adx_by_tf["15m"],
            macd_m15=macd_by_tf["15m"],
            sqzmom_m15=sqz_by_tf["15m"],
            structure_by_tf=structure_by_tf if self._structure_enabled else None,
        )

        if not bool(quality.get("alert_allowed", False)):
            self._logger.info(
                "Scanner bloqueado %s | signal_type=%s blockers=%s degraders=%s score=%s reason=%s",
                symbol,
                quality.get("signal_type", "SIN_SEÑAL"),
                quality.get("blockers", []),
                quality.get("degraders", []),
                quality.get("score_total", quality.get("score", 0)),
                quality.get("no_signal_reason", ""),
            )
            return []

        result = str(quality["result"])
        signals: list[ScannerSignal] = []
        if result not in {"LONG", "SHORT"}:
            return []

        full_sync = tf_directions["1m"] == result
        if full_sync:
            signals.append(
                self._build_signal(
                    symbol=symbol,
                    direction=result,
                    level="FULL_4_4",
                    trigger_tf="1m",
                    closed_candle_time=candles_by_tf["1m"][-2].close_time,
                    tf_rows=tf_rows,
                    m15_ema=m15_ema,
                    koncorde_m15=koncorde_m15,
                    quality=quality,
                    structure_m15=structure_by_tf.get("15m", {}),
                )
            )
        else:
            signals.append(
                self._build_signal(
                    symbol=symbol,
                    direction=result,
                    level="SYNC_3_4",
                    trigger_tf="5m",
                    closed_candle_time=candles_by_tf["5m"][-2].close_time,
                    tf_rows=tf_rows,
                    m15_ema=m15_ema,
                    koncorde_m15=koncorde_m15,
                    quality=quality,
                    structure_m15=structure_by_tf.get("15m", {}),
                )
            )
        return self._maybe_add_cross_signal(
            symbol=symbol,
            m15_ema=m15_ema,
            candles_by_tf=candles_by_tf,
            tf_rows=tf_rows,
            koncorde_m15=koncorde_m15,
            quality=quality,
            structure_m15=structure_by_tf.get("15m", {}),
            signals=signals,
        )

    def _maybe_add_cross_signal(
        self,
        *,
        symbol: str,
        m15_ema: dict[str, float | str | None],
        candles_by_tf: dict[str, list[Candle]],
        tf_rows: dict[str, str],
        koncorde_m15: dict[str, float | str | bool | None],
        quality: dict[str, int | str | dict | list],
        structure_m15: dict[str, str | bool | float | None],
        signals: list[ScannerSignal],
    ) -> list[ScannerSignal]:
        cross = str(m15_ema["cross"])
        if cross == "bull_cross":
            signals.append(
                self._build_signal(
                    symbol=symbol,
                    direction="LONG",
                    level="EMA_CROSS_M15",
                    trigger_tf="15m",
                    closed_candle_time=candles_by_tf["15m"][-2].close_time,
                    tf_rows=tf_rows,
                    m15_ema=m15_ema,
                    koncorde_m15=koncorde_m15,
                    quality=quality,
                    structure_m15=structure_m15,
                )
            )
        elif cross == "bear_cross":
            signals.append(
                self._build_signal(
                    symbol=symbol,
                    direction="SHORT",
                    level="EMA_CROSS_M15",
                    trigger_tf="15m",
                    closed_candle_time=candles_by_tf["15m"][-2].close_time,
                    tf_rows=tf_rows,
                    m15_ema=m15_ema,
                    koncorde_m15=koncorde_m15,
                    quality=quality,
                    structure_m15=structure_m15,
                )
            )
        return signals

    def _build_signal(
        self,
        *,
        symbol: str,
        direction: str,
        level: str,
        trigger_tf: str,
        closed_candle_time: int,
        tf_rows: dict[str, str],
        m15_ema: dict[str, float | str | None],
        koncorde_m15: dict[str, float | str | bool | None],
        quality: dict[str, int | str | dict | list],
        structure_m15: dict[str, str | bool | float | None],
    ) -> ScannerSignal:
        side = TradeSide.LONG if direction == "LONG" else TradeSide.SHORT
        header_icon = "🟢🔥" if direction == "LONG" else "🔴🔥"
        signal_type = str(quality.get("signal_type", f"{direction}_CONTINUATION"))
        level_text = {
            "SYNC_3_4": f"{signal_type} sync 3/4",
            "FULL_4_4": f"{signal_type} sync 4/4",
            "EMA_CROSS_M15": f"{signal_type} EMA cross M15",
        }[level]
        cross_text = "none"
        if m15_ema["cross"] == "bull_cross":
            cross_text = "🟢 bull"
        elif m15_ema["cross"] == "bear_cross":
            cross_text = "🔴 bear"
        expected_bias = "BULL" if direction == "LONG" else "BEAR"

        note = "\n".join(
            [
                f"{header_icon} {symbol} — {level_text}",
                "",
                f"Precio: {float(m15_ema['close']):.6f} USDT",
                "",
                "Estructura M15:",
                f"{'🟢' if structure_m15.get('bias') == 'BULL' else ('🔴' if structure_m15.get('bias') == 'BEAR' else '🟡')} {structure_m15.get('bias', 'MIX')} | "
                f"{'HH/HL' if structure_m15.get('bias') == 'BULL' else ('LL/LH' if structure_m15.get('bias') == 'BEAR' else 'MIX')} | "
                f"BOS {'✅' if str(structure_m15.get('bos')) == expected_bias else '❌'} | "
                f"Pullback {'✅' if str(structure_m15.get('pullback')) == direction else '❌'}",
                "",
                "Momentum:",
                f"M15 {tf_rows['15m'].split()[1]} | M5 {tf_rows['5m'].split()[1]} | M3 {tf_rows['3m'].split()[1]} | M1 {tf_rows['1m'].split()[1]}",
                "",
                "MAs:",
                f"{m15_ema['icon']} {m15_ema['relation']}",
                "",
                "📐 ADX:",
                f"{quality.get('adx_human', {}).get('status', '🟡 sin lectura')} | {quality.get('adx_human', {}).get('data', '-')}",
                "",
                "🧬 Flujo:",
                f"{quality.get('koncorde_human', {}).get('status', '🟡 flujo neutral')} | {quality.get('koncorde_human', {}).get('data', '-')}",
                "",
                "🧠 Lectura:",
                *[str(line) for line in quality.get("human_report", [])[:3]],
                "",
                f"🎯 {quality['quality']} | score {quality.get('score_total', quality['score'])}",
                "⚠ Solo alerta. No orden.",
            ]
        )
        event = AlertEvent(
            key=f"scanner::{symbol}::{direction}::{level}::{trigger_tf}::{closed_candle_time}",
            priority=(
                AlertPriority.CRITICAL
                if level == "FULL_4_4"
                else AlertPriority.WARNING
            ),
            reason=level_text,
            symbol=symbol,
            side=side,
            current_price=float(m15_ema["close"]),
            note=note,
            cooldown_seconds=0,
            metadata={
                "scanner": True,
                "direction": direction,
                "signal_type": signal_type,
                "level": level,
                "trigger_tf": trigger_tf,
                "closed_candle_time": closed_candle_time,
                "quality_payload": quality,
                "structure_m15": structure_m15,
            },
        )
        return ScannerSignal(
            event=event,
            direction=direction,
            level=level,
            trigger_tf=trigger_tf,
            closed_candle_time=closed_candle_time,
        )

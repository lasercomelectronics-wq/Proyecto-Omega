from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

import websockets
from websockets.exceptions import ConnectionClosed

from app.binance_client import BinanceAPIError, BinanceFuturesClient
from app.models import PriceUpdate

PriceCallback = Callable[[PriceUpdate], Awaitable[None]]
UserEventCallback = Callable[[dict], Awaitable[None]]
StatusCallback = Callable[[str, dict], Awaitable[None]]


class BinanceMarketStream:
    def __init__(
        self,
        client: BinanceFuturesClient,
        symbols: list[str],
        *,
        stream_suffix: str,
        reconnect_backoff: list[int],
        logger: logging.Logger,
    ) -> None:
        self._client = client
        self._symbols = sorted(set(symbol.upper() for symbol in symbols))
        self._stream_suffix = stream_suffix
        self._reconnect_backoff = reconnect_backoff or [1, 3, 5, 10, 20]
        self._logger = logger
        self._stop_event = asyncio.Event()
        self._restart_event = asyncio.Event()

    async def stop(self) -> None:
        self._stop_event.set()
        self._restart_event.set()

    async def update_symbols(self, symbols: list[str]) -> None:
        normalized = sorted(set(symbol.upper() for symbol in symbols))
        if normalized == self._symbols:
            return
        self._symbols = normalized
        self._restart_event.set()

    async def run(self, on_price: PriceCallback, on_status: StatusCallback) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            if not self._symbols:
                self._logger.info("Sin símbolos activos para market stream. Esperando cambios.")
                self._restart_event.clear()
                try:
                    await asyncio.wait_for(self._restart_event.wait(), timeout=5)
                except asyncio.TimeoutError:
                    continue
                continue

            url = self._client.build_market_stream_url(self._symbols, self._stream_suffix)
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=1000,
                ) as websocket:
                    self._logger.info("Market stream conectado para %s", ", ".join(self._symbols))
                    attempt = 0
                    self._restart_event.clear()
                    while not self._stop_event.is_set():
                        receive_task = asyncio.create_task(websocket.recv())
                        restart_task = asyncio.create_task(self._restart_event.wait())
                        done, pending = await asyncio.wait(
                            {receive_task, restart_task},
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                        for task in pending:
                            task.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)

                        if restart_task in done and self._restart_event.is_set():
                            self._logger.info("Reiniciando market stream por cambio de símbolos.")
                            await websocket.close()
                            break
                        if receive_task not in done:
                            continue

                        raw_message = receive_task.result()
                        payload = json.loads(raw_message)
                        data = payload.get("data", payload)
                        if data.get("e") != "markPriceUpdate":
                            continue
                        await on_price(
                            PriceUpdate(
                                symbol=str(data["s"]).upper(),
                                price=float(data["p"]),
                                event_time=int(data["E"]),
                                raw=data,
                            )
                        )
            except asyncio.CancelledError:
                raise
            except (ConnectionClosed, OSError, ValueError) as exc:
                if self._restart_event.is_set() or self._stop_event.is_set():
                    continue
                delay = self._reconnect_backoff[min(attempt, len(self._reconnect_backoff) - 1)]
                attempt += 1
                await on_status(
                    "market_ws_disconnected",
                    {"error": str(exc), "retry_in_seconds": delay},
                )
                await asyncio.sleep(delay)


class BinanceUserDataStream:
    def __init__(
        self,
        client: BinanceFuturesClient,
        *,
        keepalive_minutes: int,
        reconnect_backoff: list[int],
        logger: logging.Logger,
    ) -> None:
        self._client = client
        self._keepalive_minutes = keepalive_minutes
        self._reconnect_backoff = reconnect_backoff or [1, 3, 5, 10, 20]
        self._logger = logger
        self._stop_event = asyncio.Event()
        self._listen_key: str | None = None
        self._listen_key_expires_at: datetime | None = None

    async def stop(self) -> None:
        self._stop_event.set()

    async def _wait_or_stop(self, timeout_seconds: int) -> bool:
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=timeout_seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def _keepalive_loop(self, on_status: StatusCallback) -> None:
        interval_seconds = max(60, self._keepalive_minutes * 60)
        while not self._stop_event.is_set():
            if await self._wait_or_stop(interval_seconds):
                return

            try:
                new_key = await self._client.keepalive_user_data_stream()
                self._listen_key = new_key or self._listen_key
                self._listen_key_expires_at = datetime.now(tz=timezone.utc) + timedelta(minutes=60)
                await on_status(
                    "listenkey_renewed",
                    {
                        "expires_in_minutes": 60,
                        "listen_key_tail": (self._listen_key or "")[-6:],
                    },
                )
            except BinanceAPIError as exc:
                remaining_seconds = 0
                if self._listen_key_expires_at is not None:
                    remaining_seconds = max(
                        0,
                        int(
                            (self._listen_key_expires_at - datetime.now(tz=timezone.utc)).total_seconds()
                        ),
                    )

                await on_status(
                    "listenkey_keepalive_failed",
                    {
                        "error": str(exc),
                        "remaining_seconds": remaining_seconds,
                    },
                )
                if remaining_seconds <= 600:
                    await on_status(
                        "listenkey_expiring",
                        {"remaining_seconds": remaining_seconds},
                    )
                if await self._wait_or_stop(60):
                    return

    async def run(self, on_event: UserEventCallback, on_status: StatusCallback) -> None:
        attempt = 0
        while not self._stop_event.is_set():
            keepalive_task: asyncio.Task[None] | None = None
            try:
                self._listen_key = await self._client.start_user_data_stream()
                self._listen_key_expires_at = datetime.now(tz=timezone.utc) + timedelta(minutes=60)
                await on_status(
                    "listenkey_renewed",
                    {
                        "expires_in_minutes": 60,
                        "listen_key_tail": self._listen_key[-6:],
                    },
                )

                keepalive_task = asyncio.create_task(self._keepalive_loop(on_status))
                async with websockets.connect(
                    self._client.build_user_stream_url(self._listen_key),
                    ping_interval=20,
                    ping_timeout=20,
                    max_queue=1000,
                ) as websocket:
                    self._logger.info("User data stream conectado.")
                    attempt = 0
                    async for raw_message in websocket:
                        if self._stop_event.is_set():
                            break
                        payload = json.loads(raw_message)
                        if payload.get("e") == "listenKeyExpired":
                            await on_status("listenkey_expired", payload)
                            break
                        await on_event(payload)
            except asyncio.CancelledError:
                raise
            except (BinanceAPIError, ConnectionClosed, OSError, ValueError) as exc:
                delay = self._reconnect_backoff[min(attempt, len(self._reconnect_backoff) - 1)]
                attempt += 1
                await on_status(
                    "user_ws_disconnected",
                    {"error": str(exc), "retry_in_seconds": delay},
                )
                await asyncio.sleep(delay)
            finally:
                if keepalive_task is not None:
                    keepalive_task.cancel()
                    await asyncio.gather(keepalive_task, return_exceptions=True)
                try:
                    if self._listen_key:
                        await self._client.close_user_data_stream()
                except BinanceAPIError:
                    pass

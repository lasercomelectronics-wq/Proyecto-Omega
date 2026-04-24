from __future__ import annotations

import hashlib
import hmac
import time
from typing import Any
from urllib.parse import urlencode

import httpx

from app.models import Candle, PositionSnapshot, TradeSide


class BinanceAPIError(RuntimeError):
    def __init__(self, message: str, *, code: int | None = None, status_code: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


class BinanceFuturesClient:
    """
    Cliente read-only para Binance USDⓈ-M Futures.
    Solo expone endpoints de lectura y user stream; no existen rutas de órdenes aquí.
    """

    LIVE_BASE_URL = "https://fapi.binance.com"
    TESTNET_BASE_URL = "https://demo-fapi.binance.com"
    LIVE_WS_BASE_URL = "wss://fstream.binance.com"
    TESTNET_WS_BASE_URL = "wss://fstream.binancefuture.com"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        *,
        testnet: bool = False,
        recv_window: int = 5000,
        timeout_seconds: float = 15.0,
    ) -> None:
        self._api_key = api_key
        self._api_secret = api_secret
        self._recv_window = recv_window
        self._base_url = self.TESTNET_BASE_URL if testnet else self.LIVE_BASE_URL
        self._market_ws_base = self.TESTNET_WS_BASE_URL if testnet else self.LIVE_WS_BASE_URL
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(timeout_seconds),
        )

    @property
    def market_ws_base_url(self) -> str:
        return self._market_ws_base

    def build_market_stream_url(self, symbols: list[str], stream_suffix: str) -> str:
        streams = "/".join(f"{symbol.lower()}{stream_suffix}" for symbol in sorted(set(symbols)))
        return f"{self.market_ws_base_url}/stream?streams={streams}"

    def build_user_stream_url(self, listen_key: str) -> str:
        return f"{self.market_ws_base_url}/ws/{listen_key}"

    async def close(self) -> None:
        await self._client.aclose()

    def _require_credentials(self, *, need_secret: bool) -> None:
        if not self._api_key:
            raise BinanceAPIError("BINANCE_API_KEY no configurada.")
        if need_secret and not self._api_secret:
            raise BinanceAPIError("BINANCE_API_SECRET no configurada.")

    def _sign_params(self, params: dict[str, Any]) -> str:
        # Firma HMAC SHA256 requerida por USER_DATA/SIGNED endpoints.
        encoded = urlencode(params, doseq=True)
        return hmac.new(
            self._api_secret.encode("utf-8"),
            encoded.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        signed: bool = False,
        api_key_only: bool = False,
    ) -> Any:
        payload = dict(params or {})
        headers: dict[str, str] = {}

        if signed:
            self._require_credentials(need_secret=True)
            payload.setdefault("recvWindow", self._recv_window)
            payload["timestamp"] = int(time.time() * 1000)
            payload["signature"] = self._sign_params(payload)
            headers["X-MBX-APIKEY"] = self._api_key
        elif api_key_only:
            self._require_credentials(need_secret=False)
            headers["X-MBX-APIKEY"] = self._api_key

        request_kwargs: dict[str, Any] = {"headers": headers}
        if method.upper() == "GET":
            request_kwargs["params"] = payload
        else:
            request_kwargs["data"] = payload

        try:
            response = await self._client.request(method=method, url=path, **request_kwargs)
        except httpx.HTTPError as exc:
            raise BinanceAPIError(f"Fallo HTTP contra Binance: {exc}") from exc

        data: Any
        try:
            data = response.json()
        except ValueError:
            data = response.text

        if response.status_code >= 400:
            if isinstance(data, dict):
                raise BinanceAPIError(
                    data.get("msg", f"HTTP {response.status_code}"),
                    code=data.get("code"),
                    status_code=response.status_code,
                )
            raise BinanceAPIError(str(data), status_code=response.status_code)

        if isinstance(data, dict) and "code" in data and isinstance(data["code"], int) and data["code"] < 0:
            raise BinanceAPIError(
                data.get("msg", "Error de Binance"),
                code=data.get("code"),
                status_code=response.status_code,
            )

        return data

    @staticmethod
    def _derive_trade_side(position_side: str | None, position_amount: float) -> TradeSide:
        if position_side == "LONG":
            return TradeSide.LONG
        if position_side == "SHORT":
            return TradeSide.SHORT
        return TradeSide.LONG if position_amount > 0 else TradeSide.SHORT

    @staticmethod
    def _as_float_or_none(value: Any) -> float | None:
        if value in (None, "", "0", "0.0"):
            return None
        return float(value)

    async def get_positions(self, symbol: str | None = None) -> list[PositionSnapshot]:
        params = {"symbol": symbol} if symbol else None
        response = await self._request(
            "GET",
            "/fapi/v3/positionRisk",
            params=params,
            signed=True,
        )

        positions: list[PositionSnapshot] = []
        for item in response:
            quantity = float(item["positionAmt"])
            if quantity == 0:
                continue

            side = self._derive_trade_side(item.get("positionSide"), quantity)
            positions.append(
                PositionSnapshot(
                    symbol=item["symbol"].upper(),
                    side=side,
                    quantity=abs(quantity),
                    entry_price=float(item["entryPrice"]),
                    mark_price=float(item["markPrice"]),
                    unrealized_pnl=float(item.get("unRealizedProfit", 0.0)),
                    break_even_price=self._as_float_or_none(item.get("breakEvenPrice")),
                    liquidation_price=self._as_float_or_none(item.get("liquidationPrice")),
                    update_time=int(item.get("updateTime") or 0),
                    raw=item,
                )
            )
        return positions

    async def get_klines(self, symbol: str, interval: str, limit: int) -> list[Candle]:
        response = await self._request(
            "GET",
            "/fapi/v1/klines",
            params={"symbol": symbol.upper(), "interval": interval, "limit": limit},
        )
        return [
            Candle(
                open_time=int(row[0]),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                close_time=int(row[6]),
                volume=float(row[5]),
            )
            for row in response
        ]

    async def get_mark_price(self, symbol: str) -> tuple[float, int | None]:
        response = await self._request(
            "GET",
            "/fapi/v1/premiumIndex",
            params={"symbol": symbol.upper()},
        )
        if not isinstance(response, dict) or "markPrice" not in response:
            raise BinanceAPIError(f"Respuesta inesperada al consultar mark price para {symbol.upper()}.")
        event_time = response.get("time")
        return float(response["markPrice"]), int(event_time) if event_time is not None else None

    async def start_user_data_stream(self) -> str:
        response = await self._request("POST", "/fapi/v1/listenKey", api_key_only=True)
        return str(response["listenKey"])

    async def keepalive_user_data_stream(self) -> str:
        response = await self._request("PUT", "/fapi/v1/listenKey", api_key_only=True)
        return str(response["listenKey"])

    async def close_user_data_stream(self) -> None:
        await self._request("DELETE", "/fapi/v1/listenKey", api_key_only=True)

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
from typing import Any
from urllib.parse import urlencode

import httpx

from app.binance_guard import BinanceRateLimitGuard, TTLCache
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
        guard_enabled: bool = True,
        cache_enabled: bool = True,
        backoff_enabled: bool = True,
        rate_limit_pause_seconds: int = 60,
        mark_price_cache_ttl: int = 2,
        klines_1m_cache_ttl: int = 15,
        klines_3m_cache_ttl: int = 30,
        klines_5m_cache_ttl: int = 45,
        klines_15m_cache_ttl: int = 90,
        position_cache_ttl: int = 10,
        exchange_info_cache_ttl: int = 3600,
    ) -> None:
        self._logger = logging.getLogger("app.binance_client")
        self._api_key = api_key
        self._api_secret = api_secret
        self._recv_window = recv_window
        self._base_url = self.TESTNET_BASE_URL if testnet else self.LIVE_BASE_URL
        self._market_ws_base = self.TESTNET_WS_BASE_URL if testnet else self.LIVE_WS_BASE_URL
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=httpx.Timeout(timeout_seconds),
        )
        self._guard_enabled = guard_enabled
        self._cache_enabled = cache_enabled
        self._backoff_enabled = backoff_enabled
        self._guard = BinanceRateLimitGuard(pause_seconds=rate_limit_pause_seconds)
        self._cache = TTLCache()
        self._mark_price_cache_ttl = mark_price_cache_ttl
        self._klines_ttls = {"1m": klines_1m_cache_ttl, "3m": klines_3m_cache_ttl, "5m": klines_5m_cache_ttl, "15m": klines_15m_cache_ttl}
        self._position_cache_ttl = position_cache_ttl
        self._exchange_info_cache_ttl = exchange_info_cache_ttl

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
        if self._guard_enabled and self._backoff_enabled:
            allowed, reason = self._guard.can_request()
            if not allowed:
                raise BinanceAPIError(f"Binance guard activo: {reason}", status_code=429)
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
            if self._guard_enabled:
                self._guard.register_request(path)
            response = await self._client.request(method=method, url=path, **request_kwargs)
            self._logger.debug("Binance request registered: %s %s -> %s", method.upper(), path, response.status_code)
        except httpx.HTTPError as exc:
            self._logger.warning("Binance HTTP error on %s %s: %s", method.upper(), path, exc)
            raise BinanceAPIError(f"Fallo HTTP contra Binance: {exc}") from exc

        data: Any
        try:
            data = response.json()
        except ValueError:
            data = response.text

        if response.status_code >= 400:
            if self._guard_enabled:
                self._guard.register_http_error(response.status_code, dict(response.headers), str(data))
            if response.status_code in {429, 418}:
                self._logger.warning(
                    "Binance throttling/ban status=%s retry-after=%s endpoint=%s",
                    response.status_code,
                    dict(response.headers).get("retry-after"),
                    path,
                )
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

        if self._guard_enabled:
            self._guard.register_success(response.status_code, dict(response.headers))
        return data

    @staticmethod
    def _cache_key(path: str, params: dict[str, Any] | None = None) -> str:
        return json.dumps({"path": path, "params": params or {}}, sort_keys=True, default=str)

    def _cache_get(self, key: str) -> Any | None:
        if not self._cache_enabled:
            return None
        value = self._cache.get(key)
        self._logger.debug("Binance cache %s key=%s", "hit" if value is not None else "miss", key)
        return value

    def _cache_set(self, key: str, value: Any, ttl: int) -> None:
        if not self._cache_enabled:
            return
        self._cache.set(key, value, ttl)

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
        cache_key = self._cache_key("/fapi/v3/positionRisk", params)
        cached = self._cache_get(cache_key)
        if cached is not None:
            response = cached
        else:
            response = await self._request(
                "GET",
                "/fapi/v3/positionRisk",
                params=params,
                signed=True,
            )
            self._cache_set(cache_key, response, self._position_cache_ttl)

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
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        cache_key = self._cache_key("/fapi/v1/klines", params)
        cached = self._cache_get(cache_key)
        if cached is not None:
            response = cached
        else:
            response = await self._request("GET", "/fapi/v1/klines", params=params)
            self._cache_set(cache_key, response, self._klines_ttls.get(interval, 15))
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
        params = {"symbol": symbol.upper()}
        cache_key = self._cache_key("/fapi/v1/premiumIndex", params)
        cached = self._cache_get(cache_key)
        if cached is not None:
            response = cached
        else:
            response = await self._request("GET", "/fapi/v1/premiumIndex", params=params)
            self._cache_set(cache_key, response, self._mark_price_cache_ttl)
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

    def get_api_status(self) -> dict[str, Any]:
        self._guard.set_cache_stats(
            enabled=self._cache_enabled,
            hits=self._cache.hits,
            misses=self._cache.misses,
            size=self._cache.size,
        )
        snapshot = self._guard.snapshot()
        cache_total = self._cache.hits + self._cache.misses
        hit_rate = (self._cache.hits / cache_total * 100.0) if cache_total > 0 else 0.0
        snapshot.update(
            {
                "guard_enabled": self._guard_enabled,
                "cache_enabled": self._cache_enabled,
                "cache_hits": self._cache.hits,
                "cache_misses": self._cache.misses,
                "cache_size": self._cache.size,
                "cache_hit_rate": round(hit_rate, 2),
                "scanner_paused": bool(snapshot.get("scanner_paused", False))
                or snapshot["status"] in {"RATE_LIMITED", "IP_BANNED", "WAF_OR_FORBIDDEN"},
            }
        )
        return snapshot

    def is_rest_paused(self) -> bool:
        status = self.get_api_status().get("status")
        return status in {"RATE_LIMITED", "IP_BANNED", "WAF_OR_FORBIDDEN"}

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class RateLimitState:
    count_429: int = 0
    count_418: int = 0
    count_403: int = 0
    last_retry_after: int | None = None
    banned_until: float | None = None
    used_weight: str | None = None


@dataclass(slots=True)
class BinanceApiHealth:
    total_requests: int = 0
    requests_last_60s: int = 0
    requests_last_5m: int = 0
    last_status_code: int | None = None
    last_error: str | None = None
    last_success_at: float | None = None
    degraded_mode: bool = False
    is_banned: bool = False
    scanner_paused: bool = False
    used_weight_1m: str | None = None
    used_weight_raw_headers: str | None = None
    cache_enabled: bool = False
    cache_hits: int = 0
    cache_misses: int = 0
    cache_size: int = 0


class TTLCache:
    def __init__(self) -> None:
        self._store: dict[str, tuple[float, Any]] = {}
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Any | None:
        item = self._store.get(key)
        if item is None:
            self.misses += 1
            return None
        expires_at, value = item
        if time.time() > expires_at:
            self._store.pop(key, None)
            self.misses += 1
            return None
        self.hits += 1
        return value

    def set(self, key: str, value: Any, ttl_seconds: int) -> None:
        self._store[key] = (time.time() + max(1, ttl_seconds), value)

    @property
    def size(self) -> int:
        return len(self._store)


class BinanceRateLimitGuard:
    def __init__(self, pause_seconds: int = 60) -> None:
        self.health = BinanceApiHealth()
        self.rate = RateLimitState()
        self._pause_seconds = max(5, int(pause_seconds))
        self._last_status_alert: str | None = None
        self._request_timestamps = deque()

    def register_request(self, endpoint: str) -> None:
        _ = endpoint
        now = time.time()
        self._request_timestamps.append(now)
        self.health.total_requests += 1
        self._update_request_windows(now)

    def _update_request_windows(self, now: float | None = None) -> None:
        current = now if now is not None else time.time()
        window_5m_start = current - 300
        while self._request_timestamps and self._request_timestamps[0] < window_5m_start:
            self._request_timestamps.popleft()
        self.health.requests_last_5m = len(self._request_timestamps)
        window_60s_start = current - 60
        self.health.requests_last_60s = sum(1 for ts in self._request_timestamps if ts >= window_60s_start)

    def set_cache_stats(self, *, enabled: bool, hits: int, misses: int, size: int) -> None:
        self.health.cache_enabled = enabled
        self.health.cache_hits = hits
        self.health.cache_misses = misses
        self.health.cache_size = size

    def can_request(self) -> tuple[bool, str | None]:
        now = time.time()
        if self.rate.banned_until is not None and now < self.rate.banned_until:
            self.health.is_banned = True
            self.health.degraded_mode = True
            self.health.scanner_paused = True
            return False, "IP_BANNED_OR_RATE_LIMITED"
        self.health.is_banned = False
        self.health.scanner_paused = False
        return True, None

    def register_success(self, status_code: int, headers: dict[str, str]) -> None:
        self.health.last_status_code = status_code
        self.health.last_error = None
        self.health.last_success_at = time.time()
        self.health.degraded_mode = False
        self.health.is_banned = False
        self.health.scanner_paused = False
        weight = headers.get("x-mbx-used-weight-1m") or headers.get("x-mbx-used-weight")
        if weight:
            self.rate.used_weight = str(weight)
            self.health.used_weight_1m = str(weight)
        self.health.used_weight_raw_headers = str(
            {key: value for key, value in headers.items() if key.lower().startswith("x-mbx-used-weight")}
        )

    def register_http_error(self, status_code: int, headers: dict[str, str], message: str) -> None:
        self.health.last_status_code = status_code
        self.health.last_error = message
        retry_after = headers.get("retry-after")
        retry_seconds = int(retry_after) if retry_after and retry_after.isdigit() else self._pause_seconds
        self.rate.last_retry_after = retry_seconds
        if status_code == 429:
            self.rate.count_429 += 1
            self.health.degraded_mode = True
            self.rate.banned_until = time.time() + retry_seconds
            self.health.scanner_paused = True
        elif status_code == 418:
            self.rate.count_418 += 1
            self.health.degraded_mode = True
            self.health.is_banned = True
            self.rate.banned_until = time.time() + retry_seconds
            self.health.scanner_paused = True
        elif status_code == 403:
            self.rate.count_403 += 1
            self.health.degraded_mode = True
            self.rate.banned_until = time.time() + retry_seconds
            self.health.scanner_paused = True
        self.health.used_weight_raw_headers = str(
            {key: value for key, value in headers.items() if key.lower().startswith("x-mbx-used-weight")}
        )

    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        self._update_request_windows(now)
        banned_until = self.rate.banned_until
        retry_after = None
        if banned_until is not None:
            retry_after = max(0, int(banned_until - now))
        status = "OK"
        if self.health.last_status_code == 429:
            status = "RATE_LIMITED"
        elif self.health.last_status_code == 418:
            status = "IP_BANNED"
        elif self.health.last_status_code == 403:
            status = "WAF_OR_FORBIDDEN"
        elif self.health.degraded_mode:
            status = "DEGRADED"
        return {
            "status": status,
            "last_status_code": self.health.last_status_code,
            "last_error": self.health.last_error,
            "retry_after": retry_after,
            "banned_until": banned_until,
            "used_weight": self.rate.used_weight,
            "used_weight_1m": self.health.used_weight_1m,
            "used_weight_raw_headers": self.health.used_weight_raw_headers,
            "count_429": self.rate.count_429,
            "count_418": self.rate.count_418,
            "count_403": self.rate.count_403,
            "degraded_mode": self.health.degraded_mode,
            "is_banned": self.health.is_banned,
            "last_success_at": self.health.last_success_at,
            "total_requests_session": self.health.total_requests,
            "requests_last_60s": self.health.requests_last_60s,
            "requests_last_5m": self.health.requests_last_5m,
            "scanner_paused": self.health.scanner_paused,
            "cache_enabled": self.health.cache_enabled,
            "cache_hits": self.health.cache_hits,
            "cache_misses": self.health.cache_misses,
            "cache_size": self.health.cache_size,
        }

import time

from app.binance_guard import BinanceRateLimitGuard, TTLCache


def test_ttl_cache_hit_and_expire() -> None:
    cache = TTLCache()
    cache.set("k", 1, ttl_seconds=1)
    assert cache.get("k") == 1
    assert cache.hits == 1
    time.sleep(1.05)
    assert cache.get("k") is None
    assert cache.misses >= 1


def test_guard_429_sets_rate_limited() -> None:
    guard = BinanceRateLimitGuard(pause_seconds=10)
    guard.register_http_error(429, {"retry-after": "5"}, "rate")
    snapshot = guard.snapshot()
    assert snapshot["status"] == "RATE_LIMITED"
    assert snapshot["count_429"] == 1
    assert snapshot["retry_after"] is not None


def test_guard_418_sets_banned_until() -> None:
    guard = BinanceRateLimitGuard(pause_seconds=10)
    guard.register_http_error(418, {"retry-after": "7"}, "ban")
    snapshot = guard.snapshot()
    assert snapshot["status"] == "IP_BANNED"
    assert snapshot["count_418"] == 1
    assert snapshot["banned_until"] is not None


def test_guard_registers_request_windows() -> None:
    guard = BinanceRateLimitGuard(pause_seconds=10)
    guard.register_request("/fapi/v1/premiumIndex")
    snapshot = guard.snapshot()
    assert snapshot["total_requests_session"] == 1
    assert snapshot["requests_last_60s"] == 1
    assert snapshot["requests_last_5m"] == 1


def test_guard_cache_stats_and_hit_rate_inputs() -> None:
    guard = BinanceRateLimitGuard(pause_seconds=10)
    guard.set_cache_stats(enabled=True, hits=4, misses=1, size=2)
    snapshot = guard.snapshot()
    assert snapshot["cache_enabled"] is True
    assert snapshot["cache_hits"] == 4
    assert snapshot["cache_misses"] == 1
    assert snapshot["cache_size"] == 2

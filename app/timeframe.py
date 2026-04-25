from __future__ import annotations


def normalize_timeframe(timeframe: str | None) -> str:
    raw = str(timeframe or "").strip().lower()
    if not raw:
        return "unknown"
    aliases = {
        "m1": "1m",
        "1m": "1m",
        "m3": "3m",
        "3m": "3m",
        "m5": "5m",
        "5m": "5m",
        "m15": "15m",
        "15m": "15m",
        "m30": "30m",
        "30m": "30m",
        "h1": "1h",
        "1h": "1h",
        "h4": "4h",
        "4h": "4h",
    }
    return aliases.get(raw, raw)


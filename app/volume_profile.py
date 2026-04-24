from __future__ import annotations

from typing import Any


def fixed_range_volume_profile(candles: list[dict[str, float]], bins: int = 24) -> dict[str, Any]:
    if not candles:
        return {"poc": None, "vah": None, "val": None, "hvn": [], "lvn": []}
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    vols = [max(c.get("volume", 0.0), 0.0) for c in candles]

    lo = min(lows)
    hi = max(highs)
    if hi <= lo:
        return {"poc": lo, "vah": hi, "val": lo, "hvn": [lo], "lvn": [lo]}

    step = (hi - lo) / max(1, bins)
    bucket_vols = [0.0 for _ in range(bins)]
    for candle, vol in zip(candles, vols):
        mid = (candle["high"] + candle["low"] + candle["close"]) / 3
        idx = int((mid - lo) / step)
        idx = max(0, min(bins - 1, idx))
        bucket_vols[idx] += vol

    poc_idx = max(range(bins), key=lambda i: bucket_vols[i])
    poc = lo + (step * (poc_idx + 0.5))

    sorted_idx = sorted(range(bins), key=lambda i: bucket_vols[i], reverse=True)
    total = sum(bucket_vols)
    acc = 0.0
    selected: set[int] = set()
    for idx in sorted_idx:
        selected.add(idx)
        acc += bucket_vols[idx]
        if total == 0 or (acc / total) >= 0.7:
            break

    vah = lo + (step * (max(selected) + 1))
    val = lo + (step * min(selected))
    hvn = [lo + (step * (idx + 0.5)) for idx in sorted_idx[:3]]
    lvn = [lo + (step * (idx + 0.5)) for idx in sorted(range(bins), key=lambda i: bucket_vols[i])[:3]]
    return {"poc": poc, "vah": vah, "val": val, "hvn": hvn, "lvn": lvn}

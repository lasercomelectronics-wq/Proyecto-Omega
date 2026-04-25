from app.timeframe import normalize_timeframe


def test_normalize_timeframe_aliases() -> None:
    assert normalize_timeframe("M1") == "1m"
    assert normalize_timeframe("M3") == "3m"
    assert normalize_timeframe("M5") == "5m"
    assert normalize_timeframe("M15") == "15m"
    assert normalize_timeframe("15m") == "15m"
    assert normalize_timeframe("1H") == "1h"
    assert normalize_timeframe("H1") == "1h"
    assert normalize_timeframe("1h") == "1h"

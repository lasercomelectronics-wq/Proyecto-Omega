from app.alert_engine import AlertEngine
from app.models import (
    AlertEngineSettings,
    InvalidationZone,
    MarketContext,
    PositionSnapshot,
    TradeAlertConfig,
    TradeConfig,
    TradeRuntimeState,
    TradeSide,
    TradeStrategyConfig,
)


def build_trade() -> TradeConfig:
    return TradeConfig(
        symbol="WLDUSDT",
        side=TradeSide.SHORT,
        entry=0.8912,
        leverage=20,
        stop_loss=0.9050,
        take_profits=[0.8820, 0.8740, 0.8600],
        invalidation_zone=InvalidationZone(min=0.9020, max=0.9060),
        alerts=TradeAlertConfig(cooldown_seconds=300),
        strategy=TradeStrategyConfig(),
    )


def test_evaluate_trade_generates_invalidation_and_tp_alerts() -> None:
    engine = AlertEngine(AlertEngineSettings())
    trade = build_trade()
    runtime = TradeRuntimeState(
        symbol="WLDUSDT",
        side=TradeSide.SHORT,
        highest_price=0.9040,
        lowest_price=0.8700,
        last_price=0.8840,
        entry_seen_profit=True,
        tp_hits=[],
    )
    context = MarketContext(symbol="WLDUSDT", structure="bearish")

    events = engine.evaluate_trade(
        trade,
        current_price=0.8815,
        runtime=runtime,
        previous_price=0.8840,
        position=None,
        market_context=context,
    )

    reasons = {event.reason for event in events}
    assert any("TP1" in reason for reason in reasons)


def test_evaluate_position_alignment_detects_missing_and_undeclared() -> None:
    engine = AlertEngine(AlertEngineSettings())
    trade = build_trade()
    undeclared_position = PositionSnapshot(
        symbol="BTCUSDT",
        side=TradeSide.LONG,
        quantity=0.01,
        entry_price=100000,
        mark_price=101000,
    )

    events = engine.evaluate_position_alignment([trade], [undeclared_position])
    keys = {event.key for event in events}
    assert "system::missing_position::WLDUSDT::SHORT" in keys
    assert "system::undeclared::BTCUSDT::LONG" in keys

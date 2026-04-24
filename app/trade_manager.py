from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from app.models import (
    InvalidationZone,
    MarketContext,
    PositionSnapshot,
    TradeConfig,
    TradeRuntimeState,
    TradeSide,
    TradeStatus,
    make_trade_key,
)
from app.storage import Storage


class TradeManager:
    def __init__(self, storage: Storage) -> None:
        self._storage = storage
        self._lock = asyncio.Lock()
        self._trades_by_key: dict[str, TradeConfig] = {}
        self._trades_by_symbol: dict[str, list[TradeConfig]] = {}
        self._watchlist_symbols: list[str] = []
        self._positions_by_key: dict[str, PositionSnapshot] = {}
        self._runtime_cache: dict[str, TradeRuntimeState] = {}
        self._context_by_key: dict[str, MarketContext] = {}

    async def bootstrap_trades(self, yaml_trades: list[TradeConfig]) -> None:
        await self._storage.import_trades_if_empty(yaml_trades)
        await self.reload_trades()
        await self.reload_watchlist()

    async def reload_trades(self) -> list[TradeConfig]:
        trades = await self._storage.list_trades(
            statuses=(TradeStatus.ACTIVE, TradeStatus.PAUSED, TradeStatus.CLOSED)
        )
        async with self._lock:
            self._trades_by_key = {}
            self._trades_by_symbol.clear()
            for trade in trades:
                self._trades_by_key.setdefault(make_trade_key(trade.symbol, trade.side), trade)
                self._trades_by_symbol.setdefault(trade.symbol, []).append(trade)
        return trades

    def get_declared_trades(
        self,
        statuses: tuple[TradeStatus, ...] | None = None,
    ) -> list[TradeConfig]:
        trades = list(self._trades_by_key.values())
        if statuses is None:
            return trades
        return [trade for trade in trades if trade.status in statuses]

    def get_trade_by_symbol(
        self,
        symbol: str,
        statuses: tuple[TradeStatus, ...] | None = None,
    ) -> TradeConfig | None:
        trades = self._trades_by_symbol.get(symbol.upper(), [])
        if statuses is None:
            return trades[0] if trades else None
        for trade in trades:
            if trade.status in statuses:
                return trade
        return None

    def get_trades_for_symbol(
        self,
        symbol: str,
        statuses: tuple[TradeStatus, ...] = (TradeStatus.ACTIVE,),
    ) -> list[TradeConfig]:
        return [
            trade
            for trade in self._trades_by_symbol.get(symbol.upper(), [])
            if trade.status in statuses
        ]

    def count_trades(self, statuses: tuple[TradeStatus, ...] = (TradeStatus.ACTIVE,)) -> int:
        return len(self.get_declared_trades(statuses=statuses))

    async def reload_watchlist(self) -> list[str]:
        symbols = await self._storage.list_watchlist_symbols()
        async with self._lock:
            self._watchlist_symbols = sorted({symbol.upper() for symbol in symbols})
        return list(self._watchlist_symbols)

    def get_watchlist_symbols(self) -> list[str]:
        return list(self._watchlist_symbols)

    async def add_watchlist_symbol(self, symbol: str) -> list[str]:
        await self._storage.add_watchlist_symbol(symbol)
        return await self.reload_watchlist()

    async def remove_watchlist_symbol(self, symbol: str) -> list[str]:
        removed = await self._storage.remove_watchlist_symbol(symbol)
        if not removed:
            raise ValueError(f"{symbol.upper()} no estaba en watchlist.")
        return await self.reload_watchlist()

    def get_monitored_symbols(self) -> list[str]:
        symbols = {
            trade.symbol
            for trade in self.get_declared_trades(statuses=(TradeStatus.ACTIVE,))
        }
        symbols.update(position.symbol for position in self._positions_by_key.values())
        symbols.update(self._watchlist_symbols)
        return sorted(symbols)

    def get_open_positions(self) -> list[PositionSnapshot]:
        return list(self._positions_by_key.values())

    def get_position_for_trade(self, trade: TradeConfig) -> PositionSnapshot | None:
        return self._positions_by_key.get(make_trade_key(trade.symbol, trade.side))

    def get_market_context(self, trade: TradeConfig) -> MarketContext | None:
        return self._context_by_key.get(make_trade_key(trade.symbol, trade.side))

    async def set_market_context(self, trade: TradeConfig, context: MarketContext) -> None:
        self._context_by_key[make_trade_key(trade.symbol, trade.side)] = context

    async def sync_positions(self, positions: list[PositionSnapshot]) -> None:
        async with self._lock:
            previous_keys = set(self._positions_by_key)
            new_positions = {
                make_trade_key(position.symbol, position.side): position for position in positions
            }
            self._positions_by_key = new_positions

            removed = previous_keys - set(new_positions)
            for key in removed:
                symbol, side_text = key.split("::", maxsplit=1)
                await self._storage.delete_position(symbol, TradeSide(side_text))

            for position in positions:
                await self._storage.upsert_position(position)

    async def apply_account_update(self, payload: dict) -> None:
        account_data = payload.get("a", {})
        positions_data = account_data.get("P", [])
        if not positions_data:
            return

        async with self._lock:
            for item in positions_data:
                symbol = str(item.get("s", "")).upper()
                raw_amount = float(item.get("pa", 0))
                position_side = str(item.get("ps", "BOTH"))
                if raw_amount == 0:
                    if position_side in {"LONG", "SHORT"}:
                        side = TradeSide(position_side)
                        self._positions_by_key.pop(make_trade_key(symbol, side), None)
                        await self._storage.delete_position(symbol, side)
                    else:
                        for side in (TradeSide.LONG, TradeSide.SHORT):
                            self._positions_by_key.pop(make_trade_key(symbol, side), None)
                            await self._storage.delete_position(symbol, side)
                    continue

                side = (
                    TradeSide(position_side)
                    if position_side in {"LONG", "SHORT"}
                    else (TradeSide.LONG if raw_amount > 0 else TradeSide.SHORT)
                )
                key = make_trade_key(symbol, side)
                current_mark = (
                    self._positions_by_key.get(key).mark_price
                    if key in self._positions_by_key
                    else float(item.get("ep", 0))
                )
                position = PositionSnapshot(
                    symbol=symbol,
                    side=side,
                    quantity=abs(raw_amount),
                    entry_price=float(item.get("ep", 0)),
                    mark_price=current_mark,
                    unrealized_pnl=float(item.get("up", 0)),
                    break_even_price=float(item.get("bep", 0)) if item.get("bep") else None,
                    update_time=int(payload.get("E", 0)),
                    raw=item,
                )
                self._positions_by_key[key] = position
                await self._storage.upsert_position(position)

    async def create_trade(self, trade: TradeConfig) -> TradeConfig:
        created = await self._storage.create_trade(trade)
        await self.reload_trades()
        return created

    async def pause_trade(self, symbol: str) -> TradeConfig:
        trade = await self._storage.set_trade_status(symbol, TradeStatus.PAUSED)
        await self.reload_trades()
        return trade

    async def resume_trade(self, symbol: str) -> TradeConfig:
        trade = await self._storage.set_trade_status(symbol, TradeStatus.ACTIVE)
        await self.reload_trades()
        return trade

    async def close_trade(self, symbol: str) -> TradeConfig:
        trade = await self._storage.set_trade_status(symbol, TradeStatus.CLOSED)
        await self.reload_trades()
        return trade

    async def delete_trade(self, symbol: str) -> TradeConfig:
        trade = await self._storage.set_trade_status(symbol, TradeStatus.DELETED)
        await self.reload_trades()
        return trade

    async def set_trade_stop_loss(self, symbol: str, stop_loss: float) -> TradeConfig:
        trade = await self._storage.update_trade_stop_loss(symbol, stop_loss)
        await self.reload_trades()
        return trade

    async def set_trade_entry(self, symbol: str, entry: float) -> TradeConfig:
        trade = await self._storage.update_trade_entry(symbol, entry)
        await self.reload_trades()
        return trade

    async def set_trade_note(self, symbol: str, note: str) -> TradeConfig:
        trade = await self._storage.update_trade_note(symbol, note)
        await self.reload_trades()
        return trade

    async def add_trade_take_profit(self, symbol: str, price: float) -> TradeConfig:
        trade = await self._storage.append_take_profit(symbol, price)
        await self.reload_trades()
        return trade

    async def set_trade_take_profits(self, symbol: str, prices: list[float]) -> TradeConfig:
        trade = await self._storage.replace_take_profits(symbol, prices)
        await self.reload_trades()
        return trade

    async def set_trade_invalidation(
        self,
        symbol: str,
        zone: InvalidationZone | None,
    ) -> TradeConfig:
        trade = await self._storage.update_invalidation_zone(symbol, zone)
        await self.reload_trades()
        return trade

    async def update_runtime_with_price(
        self,
        trade: TradeConfig,
        current_price: float,
    ) -> tuple[TradeRuntimeState, float | None]:
        key = make_trade_key(trade.symbol, trade.side)
        runtime = self._runtime_cache.get(key)
        if runtime is None:
            runtime = await self._storage.load_trade_runtime(trade.symbol, trade.side)
        if runtime is None:
            runtime = TradeRuntimeState(symbol=trade.symbol, side=trade.side)

        previous_price = runtime.last_price
        runtime.last_price = current_price
        runtime.highest_price = (
            current_price
            if runtime.highest_price is None
            else max(runtime.highest_price, current_price)
        )
        runtime.lowest_price = (
            current_price
            if runtime.lowest_price is None
            else min(runtime.lowest_price, current_price)
        )
        if trade.side == TradeSide.LONG and current_price > trade.entry:
            runtime.entry_seen_profit = True
        elif trade.side == TradeSide.SHORT and current_price < trade.entry:
            runtime.entry_seen_profit = True

        runtime.updated_at = datetime.now(tz=timezone.utc)
        self._runtime_cache[key] = runtime
        await self._storage.upsert_trade_runtime(runtime)
        return runtime, previous_price

    async def mark_take_profit_hit(self, trade: TradeConfig, level: float) -> None:
        key = make_trade_key(trade.symbol, trade.side)
        runtime = self._runtime_cache.get(key)
        if runtime is None:
            runtime = await self._storage.load_trade_runtime(trade.symbol, trade.side)
        if runtime is None:
            runtime = TradeRuntimeState(symbol=trade.symbol, side=trade.side)

        if level not in runtime.tp_hits:
            runtime.tp_hits.append(level)
            runtime.updated_at = datetime.now(tz=timezone.utc)
            self._runtime_cache[key] = runtime
            await self._storage.upsert_trade_runtime(runtime)
        if trade.id is not None:
            await self._storage.mark_take_profit_hit(trade.id, level)

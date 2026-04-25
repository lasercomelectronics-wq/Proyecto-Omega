from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

from app.models import (
    AddZone,
    AlertEvent,
    InvalidationZone,
    PositionSnapshot,
    TradeAlertConfig,
    TradeConfig,
    TradeRuntimeState,
    TradeSide,
    TradeStatus,
    TradeStrategyConfig,
    make_trade_key,
)

ACTIVE_TRADE_PLAN_STATUSES = (
    "CREATED",
    "ENTRY_TOUCHED",
    "ACTIVE_ASSUMED",
    "TP1_HIT",
    "TP2_HIT",
)

TERMINAL_TRADE_PLAN_STATUSES = (
    "TP3_HIT",
    "SL_HIT",
    "EXPIRED",
    "CANCELLED",
    "INVALIDATED",
    "RESOLVED",
)


class Storage:
    def __init__(self, db_path: Path) -> None:
        self._logger = logging.getLogger("trading_alert_bot.storage")
        self._db_path = db_path
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self._db_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._thread_lock = threading.Lock()

    async def close(self) -> None:
        await asyncio.to_thread(self._connection.close)

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        with self._thread_lock:
            cursor = self._connection.cursor()
            cursor.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS alert_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_key TEXT NOT NULL,
                    symbol TEXT,
                    side TEXT,
                    priority TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    sent_at TEXT NOT NULL,
                    payload_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_alert_history_key_sent_at
                ON alert_history(alert_key, sent_at DESC);

                CREATE TABLE IF NOT EXISTS positions (
                    trade_key TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    quantity REAL NOT NULL,
                    entry_price REAL NOT NULL,
                    mark_price REAL NOT NULL,
                    unrealized_pnl REAL NOT NULL,
                    update_time INTEGER,
                    payload_json TEXT
                );

                CREATE TABLE IF NOT EXISTS trade_runtime (
                    trade_key TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    highest_price REAL,
                    lowest_price REAL,
                    last_price REAL,
                    entry_seen_profit INTEGER NOT NULL DEFAULT 0,
                    tp_hits_json TEXT NOT NULL DEFAULT '[]',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry REAL NOT NULL,
                    leverage INTEGER NOT NULL,
                    stop_loss REAL NOT NULL,
                    status TEXT NOT NULL,
                    timeframe TEXT,
                    note TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_trades_symbol_status
                ON trades(symbol, status, updated_at DESC);

                CREATE TABLE IF NOT EXISTS trade_take_profits (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER NOT NULL,
                    price REAL NOT NULL,
                    hit INTEGER NOT NULL DEFAULT 0,
                    sort_order INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (trade_id) REFERENCES trades(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS trade_invalidation_zones (
                    trade_id INTEGER PRIMARY KEY,
                    min_price REAL NOT NULL,
                    max_price REAL NOT NULL,
                    FOREIGN KEY (trade_id) REFERENCES trades(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS trade_alert_settings (
                    trade_id INTEGER PRIMARY KEY,
                    cooldown_seconds INTEGER NOT NULL,
                    sl_distance_threshold_pct REAL,
                    notify_on_tp INTEGER NOT NULL,
                    notify_on_sl_distance INTEGER NOT NULL,
                    notify_on_invalidation_zone INTEGER NOT NULL,
                    notify_on_break_even INTEGER NOT NULL,
                    notify_on_add_zone INTEGER NOT NULL,
                    FOREIGN KEY (trade_id) REFERENCES trades(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS trade_strategy_settings (
                    trade_id INTEGER PRIMARY KEY,
                    ema_fast INTEGER NOT NULL,
                    ema_slow INTEGER NOT NULL,
                    use_structure INTEGER NOT NULL,
                    pivot_length INTEGER NOT NULL,
                    use_squeeze_momentum_placeholder INTEGER NOT NULL,
                    FOREIGN KEY (trade_id) REFERENCES trades(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS trade_add_zones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_id INTEGER NOT NULL,
                    price REAL NOT NULL,
                    size_usdt REAL NOT NULL,
                    note TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (trade_id) REFERENCES trades(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS watchlist_symbols (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS scanner_alert_state (
                    key TEXT PRIMARY KEY,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    level TEXT NOT NULL,
                    trigger_tf TEXT NOT NULL,
                    closed_candle_time TEXT NOT NULL,
                    sent_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS trade_plans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    quality TEXT NOT NULL,
                    entry_low REAL NOT NULL,
                    entry_high REAL NOT NULL,
                    stop_loss REAL NOT NULL,
                    tp1 REAL NOT NULL,
                    tp2 REAL NOT NULL,
                    tp3 REAL NOT NULL,
                    rr_tp1 REAL NOT NULL,
                    rr_tp2 REAL NOT NULL,
                    rr_tp3 REAL NOT NULL,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    source_alert_id TEXT,
                    plan_fingerprint TEXT,
                    raw_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_trade_plans_symbol_status
                ON trade_plans(symbol, status, created_at DESC);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_plans_plan_fingerprint
                ON trade_plans(plan_fingerprint);
                CREATE TABLE IF NOT EXISTS trade_plan_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    plan_id INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    price REAL,
                    created_at TEXT NOT NULL,
                    sent_to_telegram INTEGER NOT NULL DEFAULT 0,
                    raw_json TEXT,
                    UNIQUE(plan_id, event_type),
                    FOREIGN KEY (plan_id) REFERENCES trade_plans(id) ON DELETE CASCADE
                );
                """
            )
            columns = {
                row["name"]
                for row in self._connection.execute("PRAGMA table_info(trade_plans)").fetchall()
            }
            if "plan_fingerprint" not in columns:
                self._connection.execute("ALTER TABLE trade_plans ADD COLUMN plan_fingerprint TEXT")
                self._connection.execute(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_trade_plans_plan_fingerprint ON trade_plans(plan_fingerprint)"
                )
            try:
                self._repair_duplicate_active_trade_plans_sync()
                self._create_active_plan_unique_index_sync()
            except Exception as exc:
                self._logger.exception("failed_trade_plan_active_index_migration error=%s", exc)
                self._connection.rollback()
                raise
            self._connection.commit()

    def _create_active_plan_unique_index_sync(self) -> None:
        statuses_sql = ", ".join(f"'{status}'" for status in ACTIVE_TRADE_PLAN_STATUSES)
        create_sql = (
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_one_active_plan_per_symbol_side "
            "ON trade_plans(symbol, direction) "
            f"WHERE status IN ({statuses_sql})"
        )
        try:
            self._connection.execute("DROP INDEX IF EXISTS idx_one_active_plan_per_symbol_side")
            self._connection.execute(create_sql)
            self._logger.info("trade_plan_active_index_status index_created=%s", True)
        except sqlite3.DatabaseError as exc:
            self._logger.error("trade_plan_active_index_status index_created=%s error=%s", False, exc)
            raise RuntimeError("failed_to_create_idx_one_active_plan_per_symbol_side") from exc

    def _repair_duplicate_active_trade_plans_sync(self) -> None:
        placeholders = ", ".join("?" for _ in ACTIVE_TRADE_PLAN_STATUSES)
        duplicates = self._connection.execute(
            f"""
            SELECT symbol, direction, COUNT(*) AS total
            FROM trade_plans
            WHERE status IN ({placeholders})
            GROUP BY symbol, direction
            HAVING COUNT(*) > 1
            """,
            ACTIVE_TRADE_PLAN_STATUSES,
        ).fetchall()
        for row in duplicates:
            symbol = str(row["symbol"])
            direction = str(row["direction"])
            plan_rows = self._connection.execute(
                f"""
                SELECT id
                FROM trade_plans
                WHERE symbol = ? AND direction = ? AND status IN ({placeholders})
                ORDER BY datetime(created_at) DESC, id DESC
                """,
                (symbol, direction, *ACTIVE_TRADE_PLAN_STATUSES),
            ).fetchall()
            plan_ids = [int(plan["id"]) for plan in plan_rows]
            if not plan_ids:
                continue
            kept_plan_id = plan_ids[0]
            cancelled_plan_ids = plan_ids[1:]
            if cancelled_plan_ids:
                cancelled_placeholders = ", ".join("?" for _ in cancelled_plan_ids)
                self._connection.execute(
                    f"UPDATE trade_plans SET status = 'CANCELLED' WHERE id IN ({cancelled_placeholders})",
                    tuple(cancelled_plan_ids),
                )
            self._logger.warning(
                "duplicate_active_plans_detected symbol=%s direction=%s kept_plan_id=%s cancelled_plan_ids=%s",
                symbol,
                direction,
                kept_plan_id,
                cancelled_plan_ids,
            )

    @staticmethod
    def _now_iso() -> str:
        return datetime.now(tz=timezone.utc).isoformat()

    @staticmethod
    def _serialize_payload(raw: dict) -> str:
        return json.dumps(raw)

    @staticmethod
    def _bool_to_int(value: bool) -> int:
        return 1 if value else 0

    def _hydrate_trade_sync(self, row: sqlite3.Row) -> TradeConfig:
        trade_id = int(row["id"])

        tp_rows = self._connection.execute(
            """
            SELECT price, hit
            FROM trade_take_profits
            WHERE trade_id = ?
            ORDER BY sort_order ASC, id ASC
            """,
            (trade_id,),
        ).fetchall()
        invalidation_row = self._connection.execute(
            """
            SELECT min_price, max_price
            FROM trade_invalidation_zones
            WHERE trade_id = ?
            """,
            (trade_id,),
        ).fetchone()
        alerts_row = self._connection.execute(
            """
            SELECT *
            FROM trade_alert_settings
            WHERE trade_id = ?
            """,
            (trade_id,),
        ).fetchone()
        strategy_row = self._connection.execute(
            """
            SELECT *
            FROM trade_strategy_settings
            WHERE trade_id = ?
            """,
            (trade_id,),
        ).fetchone()
        add_zone_rows = self._connection.execute(
            """
            SELECT price, size_usdt, note
            FROM trade_add_zones
            WHERE trade_id = ?
            ORDER BY id ASC
            """,
            (trade_id,),
        ).fetchall()

        alert_config = (
            TradeAlertConfig(
                cooldown_seconds=int(alerts_row["cooldown_seconds"]),
                sl_distance_threshold_pct=alerts_row["sl_distance_threshold_pct"],
                notify_on_tp=bool(alerts_row["notify_on_tp"]),
                notify_on_sl_distance=bool(alerts_row["notify_on_sl_distance"]),
                notify_on_invalidation_zone=bool(alerts_row["notify_on_invalidation_zone"]),
                notify_on_break_even=bool(alerts_row["notify_on_break_even"]),
                notify_on_add_zone=bool(alerts_row["notify_on_add_zone"]),
            )
            if alerts_row is not None
            else TradeAlertConfig()
        )

        invalidation_zone = (
            InvalidationZone(
                min=float(invalidation_row["min_price"]),
                max=float(invalidation_row["max_price"]),
            )
            if invalidation_row is not None
            else None
        )

        return TradeConfig(
            id=trade_id,
            symbol=str(row["symbol"]).upper(),
            side=TradeSide(row["side"]),
            entry=float(row["entry"]),
            leverage=int(row["leverage"]),
            stop_loss=float(row["stop_loss"]),
            status=TradeStatus(row["status"]),
            take_profits=[float(item["price"]) for item in tp_rows],
            invalidation_zone=invalidation_zone,
            add_zones=[
                AddZone(
                    price=float(add_zone["price"]),
                    size_usdt=float(add_zone["size_usdt"]),
                    note=str(add_zone["note"] or ""),
                )
                for add_zone in add_zone_rows
            ],
            alerts=alert_config,
            strategy=TradeStrategyConfig(
                timeframe=str(row["timeframe"]).strip() if row["timeframe"] else None,
                ema_fast=int(strategy_row["ema_fast"]) if strategy_row is not None else 55,
                ema_slow=int(strategy_row["ema_slow"]) if strategy_row is not None else 200,
                use_structure=bool(strategy_row["use_structure"]) if strategy_row is not None else True,
                pivot_length=int(strategy_row["pivot_length"]) if strategy_row is not None else 5,
                use_squeeze_momentum_placeholder=(
                    bool(strategy_row["use_squeeze_momentum_placeholder"])
                    if strategy_row is not None
                    else False
                ),
            ),
            note=str(row["note"] or ""),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    def _find_trade_row_sync(
        self,
        symbol: str,
        statuses: tuple[TradeStatus, ...] | None,
    ) -> sqlite3.Row | None:
        params: list[object] = [symbol.upper()]
        query = "SELECT * FROM trades WHERE symbol = ?"
        if statuses:
            placeholders = ", ".join("?" for _ in statuses)
            query += f" AND status IN ({placeholders})"
            params.extend(status.value for status in statuses)
        query += " ORDER BY updated_at DESC, id DESC LIMIT 1"
        return self._connection.execute(query, tuple(params)).fetchone()

    def _write_trade_children_sync(self, trade_id: int, trade: TradeConfig) -> None:
        now_iso = self._now_iso()
        self._connection.execute(
            "DELETE FROM trade_take_profits WHERE trade_id = ?",
            (trade_id,),
        )
        self._connection.execute(
            "DELETE FROM trade_invalidation_zones WHERE trade_id = ?",
            (trade_id,),
        )
        self._connection.execute(
            "DELETE FROM trade_alert_settings WHERE trade_id = ?",
            (trade_id,),
        )
        self._connection.execute(
            "DELETE FROM trade_strategy_settings WHERE trade_id = ?",
            (trade_id,),
        )
        self._connection.execute(
            "DELETE FROM trade_add_zones WHERE trade_id = ?",
            (trade_id,),
        )

        for index, price in enumerate(trade.take_profits):
            self._connection.execute(
                """
                INSERT INTO trade_take_profits (trade_id, price, hit, sort_order, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (trade_id, float(price), 0, index, now_iso),
            )

        if trade.invalidation_zone is not None:
            self._connection.execute(
                """
                INSERT INTO trade_invalidation_zones (trade_id, min_price, max_price)
                VALUES (?, ?, ?)
                """,
                (
                    trade_id,
                    float(trade.invalidation_zone.min),
                    float(trade.invalidation_zone.max),
                ),
            )

        self._connection.execute(
            """
            INSERT INTO trade_alert_settings (
                trade_id, cooldown_seconds, sl_distance_threshold_pct, notify_on_tp,
                notify_on_sl_distance, notify_on_invalidation_zone, notify_on_break_even,
                notify_on_add_zone
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_id,
                int(trade.alerts.cooldown_seconds),
                trade.alerts.sl_distance_threshold_pct,
                self._bool_to_int(trade.alerts.notify_on_tp),
                self._bool_to_int(trade.alerts.notify_on_sl_distance),
                self._bool_to_int(trade.alerts.notify_on_invalidation_zone),
                self._bool_to_int(trade.alerts.notify_on_break_even),
                self._bool_to_int(trade.alerts.notify_on_add_zone),
            ),
        )

        self._connection.execute(
            """
            INSERT INTO trade_strategy_settings (
                trade_id, ema_fast, ema_slow, use_structure, pivot_length,
                use_squeeze_momentum_placeholder
            )
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                trade_id,
                int(trade.strategy.ema_fast),
                int(trade.strategy.ema_slow),
                self._bool_to_int(trade.strategy.use_structure),
                int(trade.strategy.pivot_length),
                self._bool_to_int(trade.strategy.use_squeeze_momentum_placeholder),
            ),
        )

        for add_zone in trade.add_zones:
            self._connection.execute(
                """
                INSERT INTO trade_add_zones (trade_id, price, size_usdt, note, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    trade_id,
                    float(add_zone.price),
                    float(add_zone.size_usdt),
                    add_zone.note,
                    now_iso,
                ),
            )

    async def count_trades(self, statuses: tuple[TradeStatus, ...] | None = None) -> int:
        return await asyncio.to_thread(self._count_trades_sync, statuses)

    def _count_trades_sync(self, statuses: tuple[TradeStatus, ...] | None = None) -> int:
        with self._thread_lock:
            if statuses:
                placeholders = ", ".join("?" for _ in statuses)
                row = self._connection.execute(
                    f"SELECT COUNT(*) AS total FROM trades WHERE status IN ({placeholders})",
                    tuple(status.value for status in statuses),
                ).fetchone()
            else:
                row = self._connection.execute("SELECT COUNT(*) AS total FROM trades").fetchone()
        return int(row["total"])

    async def list_trades(
        self,
        statuses: tuple[TradeStatus, ...] | None = None,
    ) -> list[TradeConfig]:
        return await asyncio.to_thread(self._list_trades_sync, statuses)

    def _list_trades_sync(
        self,
        statuses: tuple[TradeStatus, ...] | None = None,
    ) -> list[TradeConfig]:
        with self._thread_lock:
            params: tuple[object, ...] = ()
            query = "SELECT * FROM trades"
            if statuses:
                placeholders = ", ".join("?" for _ in statuses)
                query += f" WHERE status IN ({placeholders})"
                params = tuple(status.value for status in statuses)
            query += " ORDER BY updated_at DESC, id DESC"
            rows = self._connection.execute(query, params).fetchall()
            return [self._hydrate_trade_sync(row) for row in rows]

    async def get_trade_by_symbol(
        self,
        symbol: str,
        statuses: tuple[TradeStatus, ...] | None = None,
    ) -> TradeConfig | None:
        return await asyncio.to_thread(self._get_trade_by_symbol_sync, symbol, statuses)

    def _get_trade_by_symbol_sync(
        self,
        symbol: str,
        statuses: tuple[TradeStatus, ...] | None = None,
    ) -> TradeConfig | None:
        with self._thread_lock:
            row = self._find_trade_row_sync(symbol, statuses)
            return self._hydrate_trade_sync(row) if row is not None else None

    async def create_trade(self, trade: TradeConfig) -> TradeConfig:
        return await asyncio.to_thread(self._create_trade_sync, trade)

    def _create_trade_sync(self, trade: TradeConfig) -> TradeConfig:
        now_iso = self._now_iso()
        with self._thread_lock:
            existing = self._find_trade_row_sync(
                trade.symbol,
                (TradeStatus.ACTIVE, TradeStatus.PAUSED),
            )
            if existing is not None:
                raise ValueError(f"Ya existe un trade activo o pausado para {trade.symbol}.")

            cursor = self._connection.execute(
                """
                INSERT INTO trades (
                    symbol, side, entry, leverage, stop_loss, status, timeframe, note,
                    created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trade.symbol.upper(),
                    trade.side.value,
                    float(trade.entry),
                    int(trade.leverage),
                    float(trade.stop_loss),
                    trade.status.value,
                    trade.strategy.timeframe,
                    trade.note,
                    now_iso,
                    now_iso,
                ),
            )
            trade_id = int(cursor.lastrowid)
            self._write_trade_children_sync(trade_id, trade)
            self._connection.commit()
            row = self._connection.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
            return self._hydrate_trade_sync(row)

    async def import_trades_if_empty(self, trades: list[TradeConfig]) -> bool:
        return await asyncio.to_thread(self._import_trades_if_empty_sync, trades)

    def _import_trades_if_empty_sync(self, trades: list[TradeConfig]) -> bool:
        with self._thread_lock:
            row = self._connection.execute("SELECT COUNT(*) AS total FROM trades").fetchone()
            if int(row["total"]) > 0:
                return False

            now_iso = self._now_iso()
            for trade in trades:
                cursor = self._connection.execute(
                    """
                    INSERT INTO trades (
                        symbol, side, entry, leverage, stop_loss, status, timeframe, note,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        trade.symbol.upper(),
                        trade.side.value,
                        float(trade.entry),
                        int(trade.leverage),
                        float(trade.stop_loss),
                        TradeStatus.ACTIVE.value,
                        trade.strategy.timeframe,
                        trade.note,
                        now_iso,
                        now_iso,
                    ),
                )
                self._write_trade_children_sync(int(cursor.lastrowid), trade)
            self._connection.commit()
            return True

    async def list_watchlist_symbols(self) -> list[str]:
        return await asyncio.to_thread(self._list_watchlist_symbols_sync)

    def _list_watchlist_symbols_sync(self) -> list[str]:
        with self._thread_lock:
            rows = self._connection.execute(
                """
                SELECT symbol
                FROM watchlist_symbols
                ORDER BY symbol ASC
                """
            ).fetchall()
        return [str(row["symbol"]).upper() for row in rows]

    async def add_watchlist_symbol(self, symbol: str) -> None:
        await asyncio.to_thread(self._add_watchlist_symbol_sync, symbol)

    def _add_watchlist_symbol_sync(self, symbol: str) -> None:
        normalized = symbol.upper()
        now_iso = self._now_iso()
        with self._thread_lock:
            self._connection.execute(
                """
                INSERT INTO watchlist_symbols (symbol, created_at, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    updated_at = excluded.updated_at
                """,
                (normalized, now_iso, now_iso),
            )
            self._connection.commit()

    async def remove_watchlist_symbol(self, symbol: str) -> bool:
        return await asyncio.to_thread(self._remove_watchlist_symbol_sync, symbol)

    def _remove_watchlist_symbol_sync(self, symbol: str) -> bool:
        normalized = symbol.upper()
        with self._thread_lock:
            cursor = self._connection.execute(
                "DELETE FROM watchlist_symbols WHERE symbol = ?",
                (normalized,),
            )
            self._connection.execute(
                "DELETE FROM scanner_alert_state WHERE symbol = ? AND level = 'ARMED_M15_SETUP'",
                (normalized,),
            )
            self._connection.commit()
        return cursor.rowcount > 0

    async def has_scanner_alert_state(self, key: str) -> bool:
        return await asyncio.to_thread(self._has_scanner_alert_state_sync, key)

    def _has_scanner_alert_state_sync(self, key: str) -> bool:
        with self._thread_lock:
            row = self._connection.execute(
                "SELECT 1 FROM scanner_alert_state WHERE key = ? LIMIT 1",
                (key,),
            ).fetchone()
        return row is not None

    async def has_scanner_symbol_state(self, symbol: str) -> bool:
        return await asyncio.to_thread(self._has_scanner_symbol_state_sync, symbol)

    def _has_scanner_symbol_state_sync(self, symbol: str) -> bool:
        with self._thread_lock:
            row = self._connection.execute(
                "SELECT 1 FROM scanner_alert_state WHERE symbol = ? LIMIT 1",
                (symbol.upper(),),
            ).fetchone()
        return row is not None

    async def record_scanner_alert_state(
        self,
        *,
        key: str,
        symbol: str,
        direction: str,
        level: str,
        trigger_tf: str,
        closed_candle_time: str,
    ) -> None:
        await asyncio.to_thread(
            self._record_scanner_alert_state_sync,
            key,
            symbol,
            direction,
            level,
            trigger_tf,
            closed_candle_time,
        )

    def _record_scanner_alert_state_sync(
        self,
        key: str,
        symbol: str,
        direction: str,
        level: str,
        trigger_tf: str,
        closed_candle_time: str,
    ) -> None:
        with self._thread_lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO scanner_alert_state (
                    key, symbol, direction, level, trigger_tf, closed_candle_time, sent_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    key,
                    symbol.upper(),
                    direction,
                    level,
                    trigger_tf,
                    closed_candle_time,
                    self._now_iso(),
                ),
            )
            self._connection.commit()

    async def delete_scanner_alert_state(self, key: str) -> None:
        await asyncio.to_thread(self._delete_scanner_alert_state_sync, key)

    def _delete_scanner_alert_state_sync(self, key: str) -> None:
        with self._thread_lock:
            self._connection.execute("DELETE FROM scanner_alert_state WHERE key = ?", (key,))
            self._connection.commit()

    async def set_trade_status(self, symbol: str, status: TradeStatus) -> TradeConfig:
        return await asyncio.to_thread(self._set_trade_status_sync, symbol, status)

    def _set_trade_status_sync(self, symbol: str, status: TradeStatus) -> TradeConfig:
        with self._thread_lock:
            row = self._find_trade_row_sync(
                symbol,
                (
                    TradeStatus.ACTIVE,
                    TradeStatus.PAUSED,
                    TradeStatus.CLOSED,
                ),
            )
            if row is None:
                raise ValueError(f"No se encontró trade para {symbol}.")

            self._connection.execute(
                "UPDATE trades SET status = ?, updated_at = ? WHERE id = ?",
                (status.value, self._now_iso(), int(row["id"])),
            )
            self._connection.commit()
            updated = self._connection.execute(
                "SELECT * FROM trades WHERE id = ?",
                (int(row["id"]),),
            ).fetchone()
            return self._hydrate_trade_sync(updated)

    async def update_trade_stop_loss(self, symbol: str, stop_loss: float) -> TradeConfig:
        return await asyncio.to_thread(self._update_trade_stop_loss_sync, symbol, stop_loss)

    def _update_trade_stop_loss_sync(self, symbol: str, stop_loss: float) -> TradeConfig:
        with self._thread_lock:
            row = self._find_trade_row_sync(symbol, (TradeStatus.ACTIVE, TradeStatus.PAUSED))
            if row is None:
                raise ValueError(f"No se encontró trade activo/pausado para {symbol}.")
            self._connection.execute(
                "UPDATE trades SET stop_loss = ?, updated_at = ? WHERE id = ?",
                (float(stop_loss), self._now_iso(), int(row["id"])),
            )
            self._connection.commit()
            updated = self._connection.execute(
                "SELECT * FROM trades WHERE id = ?",
                (int(row["id"]),),
            ).fetchone()
            return self._hydrate_trade_sync(updated)

    async def update_trade_entry(self, symbol: str, entry: float) -> TradeConfig:
        return await asyncio.to_thread(self._update_trade_entry_sync, symbol, entry)

    def _update_trade_entry_sync(self, symbol: str, entry: float) -> TradeConfig:
        with self._thread_lock:
            row = self._find_trade_row_sync(symbol, (TradeStatus.ACTIVE, TradeStatus.PAUSED))
            if row is None:
                raise ValueError(f"No se encontró trade activo/pausado para {symbol}.")
            self._connection.execute(
                "UPDATE trades SET entry = ?, updated_at = ? WHERE id = ?",
                (float(entry), self._now_iso(), int(row["id"])),
            )
            self._connection.commit()
            updated = self._connection.execute(
                "SELECT * FROM trades WHERE id = ?",
                (int(row["id"]),),
            ).fetchone()
            return self._hydrate_trade_sync(updated)

    async def update_trade_note(self, symbol: str, note: str) -> TradeConfig:
        return await asyncio.to_thread(self._update_trade_note_sync, symbol, note)

    def _update_trade_note_sync(self, symbol: str, note: str) -> TradeConfig:
        with self._thread_lock:
            row = self._find_trade_row_sync(
                symbol,
                (TradeStatus.ACTIVE, TradeStatus.PAUSED, TradeStatus.CLOSED),
            )
            if row is None:
                raise ValueError(f"No se encontró trade para {symbol}.")
            self._connection.execute(
                "UPDATE trades SET note = ?, updated_at = ? WHERE id = ?",
                (note, self._now_iso(), int(row["id"])),
            )
            self._connection.commit()
            updated = self._connection.execute(
                "SELECT * FROM trades WHERE id = ?",
                (int(row["id"]),),
            ).fetchone()
            return self._hydrate_trade_sync(updated)

    async def replace_take_profits(self, symbol: str, prices: list[float]) -> TradeConfig:
        return await asyncio.to_thread(self._replace_take_profits_sync, symbol, prices)

    def _replace_take_profits_sync(self, symbol: str, prices: list[float]) -> TradeConfig:
        with self._thread_lock:
            row = self._find_trade_row_sync(symbol, (TradeStatus.ACTIVE, TradeStatus.PAUSED))
            if row is None:
                raise ValueError(f"No se encontró trade activo/pausado para {symbol}.")
            trade = self._hydrate_trade_sync(row)
            trade.take_profits = [float(price) for price in prices]
            self._write_trade_children_sync(int(row["id"]), trade)
            self._connection.execute(
                "UPDATE trades SET updated_at = ? WHERE id = ?",
                (self._now_iso(), int(row["id"])),
            )
            self._connection.commit()
            updated = self._connection.execute(
                "SELECT * FROM trades WHERE id = ?",
                (int(row["id"]),),
            ).fetchone()
            return self._hydrate_trade_sync(updated)

    async def append_take_profit(self, symbol: str, price: float) -> TradeConfig:
        return await asyncio.to_thread(self._append_take_profit_sync, symbol, price)

    def _append_take_profit_sync(self, symbol: str, price: float) -> TradeConfig:
        with self._thread_lock:
            row = self._find_trade_row_sync(symbol, (TradeStatus.ACTIVE, TradeStatus.PAUSED))
            if row is None:
                raise ValueError(f"No se encontró trade activo/pausado para {symbol}.")
            trade = self._hydrate_trade_sync(row)
            trade.take_profits.append(float(price))
            self._write_trade_children_sync(int(row["id"]), trade)
            self._connection.execute(
                "UPDATE trades SET updated_at = ? WHERE id = ?",
                (self._now_iso(), int(row["id"])),
            )
            self._connection.commit()
            updated = self._connection.execute(
                "SELECT * FROM trades WHERE id = ?",
                (int(row["id"]),),
            ).fetchone()
            return self._hydrate_trade_sync(updated)

    async def update_invalidation_zone(
        self,
        symbol: str,
        zone: InvalidationZone | None,
    ) -> TradeConfig:
        return await asyncio.to_thread(self._update_invalidation_zone_sync, symbol, zone)

    def _update_invalidation_zone_sync(
        self,
        symbol: str,
        zone: InvalidationZone | None,
    ) -> TradeConfig:
        with self._thread_lock:
            row = self._find_trade_row_sync(symbol, (TradeStatus.ACTIVE, TradeStatus.PAUSED))
            if row is None:
                raise ValueError(f"No se encontró trade activo/pausado para {symbol}.")
            trade = self._hydrate_trade_sync(row)
            trade.invalidation_zone = zone
            self._write_trade_children_sync(int(row["id"]), trade)
            self._connection.execute(
                "UPDATE trades SET updated_at = ? WHERE id = ?",
                (self._now_iso(), int(row["id"])),
            )
            self._connection.commit()
            updated = self._connection.execute(
                "SELECT * FROM trades WHERE id = ?",
                (int(row["id"]),),
            ).fetchone()
            return self._hydrate_trade_sync(updated)

    async def mark_take_profit_hit(self, trade_id: int, price: float) -> None:
        await asyncio.to_thread(self._mark_take_profit_hit_sync, trade_id, price)

    def _mark_take_profit_hit_sync(self, trade_id: int, price: float) -> None:
        with self._thread_lock:
            self._connection.execute(
                """
                UPDATE trade_take_profits
                SET hit = 1
                WHERE trade_id = ? AND ABS(price - ?) < 0.0000001
                """,
                (trade_id, float(price)),
            )
            self._connection.commit()

    async def should_send_alert(self, alert_key: str, cooldown_seconds: int) -> bool:
        return await asyncio.to_thread(self._should_send_alert_sync, alert_key, cooldown_seconds)

    def _should_send_alert_sync(self, alert_key: str, cooldown_seconds: int) -> bool:
        with self._thread_lock:
            cursor = self._connection.execute(
                """
                SELECT sent_at
                FROM alert_history
                WHERE alert_key = ?
                ORDER BY sent_at DESC
                LIMIT 1
                """,
                (alert_key,),
            )
            row = cursor.fetchone()

        if row is None:
            return True

        last_sent = datetime.fromisoformat(row["sent_at"])
        elapsed = datetime.now(tz=timezone.utc) - last_sent
        return elapsed.total_seconds() >= cooldown_seconds

    async def record_alert(self, event: AlertEvent) -> None:
        await asyncio.to_thread(self._record_alert_sync, event)

    def _record_alert_sync(self, event: AlertEvent) -> None:
        payload = json.dumps(
            {
                "reason": event.reason,
                "symbol": event.symbol,
                "side": event.side.value if event.side else None,
                "current_price": event.current_price,
                "entry": event.entry,
                "stop_loss": event.stop_loss,
                "take_profits": event.take_profits,
                "metadata": event.metadata,
            }
        )
        with self._thread_lock:
            self._connection.execute(
                """
                INSERT INTO alert_history (
                    alert_key, symbol, side, priority, reason, sent_at, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.key,
                    event.symbol,
                    event.side.value if event.side else None,
                    event.priority.value,
                    event.reason,
                    datetime.now(tz=timezone.utc).isoformat(),
                    payload,
                ),
            )
            self._connection.commit()

    async def upsert_position(self, position: PositionSnapshot) -> None:
        await asyncio.to_thread(self._upsert_position_sync, position)

    def _upsert_position_sync(self, position: PositionSnapshot) -> None:
        trade_key = make_trade_key(position.symbol, position.side)
        with self._thread_lock:
            self._connection.execute(
                """
                INSERT INTO positions (
                    trade_key, symbol, side, quantity, entry_price, mark_price,
                    unrealized_pnl, update_time, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_key) DO UPDATE SET
                    quantity = excluded.quantity,
                    entry_price = excluded.entry_price,
                    mark_price = excluded.mark_price,
                    unrealized_pnl = excluded.unrealized_pnl,
                    update_time = excluded.update_time,
                    payload_json = excluded.payload_json
                """,
                (
                    trade_key,
                    position.symbol,
                    position.side.value,
                    position.quantity,
                    position.entry_price,
                    position.mark_price,
                    position.unrealized_pnl,
                    position.update_time,
                    self._serialize_payload(position.raw),
                ),
            )
            self._connection.commit()

    async def delete_position(self, symbol: str, side: TradeSide) -> None:
        await asyncio.to_thread(self._delete_position_sync, symbol, side)

    def _delete_position_sync(self, symbol: str, side: TradeSide) -> None:
        with self._thread_lock:
            self._connection.execute(
                "DELETE FROM positions WHERE trade_key = ?",
                (make_trade_key(symbol, side),),
            )
            self._connection.commit()

    async def load_trade_runtime(self, symbol: str, side: TradeSide) -> TradeRuntimeState | None:
        return await asyncio.to_thread(self._load_trade_runtime_sync, symbol, side)

    def _load_trade_runtime_sync(self, symbol: str, side: TradeSide) -> TradeRuntimeState | None:
        with self._thread_lock:
            cursor = self._connection.execute(
                "SELECT * FROM trade_runtime WHERE trade_key = ?",
                (make_trade_key(symbol, side),),
            )
            row = cursor.fetchone()

        if row is None:
            return None

        return TradeRuntimeState(
            symbol=row["symbol"],
            side=TradeSide(row["side"]),
            highest_price=row["highest_price"],
            lowest_price=row["lowest_price"],
            last_price=row["last_price"],
            entry_seen_profit=bool(row["entry_seen_profit"]),
            tp_hits=list(json.loads(row["tp_hits_json"])),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    async def upsert_trade_runtime(self, runtime: TradeRuntimeState) -> None:
        await asyncio.to_thread(self._upsert_trade_runtime_sync, runtime)

    def _upsert_trade_runtime_sync(self, runtime: TradeRuntimeState) -> None:
        updated_at = runtime.updated_at or datetime.now(tz=timezone.utc)
        with self._thread_lock:
            self._connection.execute(
                """
                INSERT INTO trade_runtime (
                    trade_key, symbol, side, highest_price, lowest_price, last_price,
                    entry_seen_profit, tp_hits_json, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_key) DO UPDATE SET
                    highest_price = excluded.highest_price,
                    lowest_price = excluded.lowest_price,
                    last_price = excluded.last_price,
                    entry_seen_profit = excluded.entry_seen_profit,
                    tp_hits_json = excluded.tp_hits_json,
                    updated_at = excluded.updated_at
                """,
                (
                    make_trade_key(runtime.symbol, runtime.side),
                    runtime.symbol,
                    runtime.side.value,
                    runtime.highest_price,
                    runtime.lowest_price,
                    runtime.last_price,
                    int(runtime.entry_seen_profit),
                    json.dumps(runtime.tp_hits),
                    updated_at.isoformat(),
                ),
            )
            self._connection.commit()

    async def create_trade_plan(self, plan: dict) -> int:
        return await asyncio.to_thread(self._create_trade_plan_sync, plan)

    def _create_trade_plan_sync(self, plan: dict) -> int:
        fingerprint = str(plan.get("plan_fingerprint", "") or "").strip()
        if not fingerprint:
            raise ValueError("missing_plan_fingerprint")
        with self._thread_lock:
            cursor = self._connection.execute(
                """
                INSERT INTO trade_plans (
                    symbol, direction, signal_type, quality,
                    entry_low, entry_high, stop_loss, tp1, tp2, tp3,
                    rr_tp1, rr_tp2, rr_tp3, status,
                    created_at, expires_at, source_alert_id, plan_fingerprint, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plan["symbol"],
                    plan["direction"],
                    plan["signal_type"],
                    plan["quality"],
                    float(plan["entry_zone_low"]),
                    float(plan["entry_zone_high"]),
                    float(plan["stop_loss"]),
                    float(plan["tp1"]),
                    float(plan["tp2"]),
                    float(plan["tp3"]),
                    float(plan["rr_tp1"]),
                    float(plan["rr_tp2"]),
                    float(plan["rr_tp3"]),
                    plan.get("status", "CREATED"),
                    plan["created_at"],
                    plan["expires_at"],
                    plan.get("source_alert_id"),
                    fingerprint,
                    json.dumps(plan),
                ),
            )
            self._connection.commit()
            return int(cursor.lastrowid)

    async def has_trade_plan_fingerprint(self, fingerprint: str) -> bool:
        return await asyncio.to_thread(self._has_trade_plan_fingerprint_sync, fingerprint)

    def _has_trade_plan_fingerprint_sync(self, fingerprint: str) -> bool:
        with self._thread_lock:
            row = self._connection.execute(
                "SELECT 1 FROM trade_plans WHERE plan_fingerprint = ? LIMIT 1",
                (fingerprint,),
            ).fetchone()
        return row is not None

    async def list_trade_plans(self, statuses: tuple[str, ...] | None = None) -> list[dict]:
        return await asyncio.to_thread(self._list_trade_plans_sync, statuses)

    def _list_trade_plans_sync(self, statuses: tuple[str, ...] | None = None) -> list[dict]:
        with self._thread_lock:
            query = "SELECT * FROM trade_plans"
            params: tuple[object, ...] = ()
            if statuses:
                placeholders = ", ".join("?" for _ in statuses)
                query += f" WHERE status IN ({placeholders})"
                params = tuple(statuses)
            query += " ORDER BY created_at DESC"
            rows = self._connection.execute(query, params).fetchall()
            return [dict(row) for row in rows]

    async def get_trade_plan(self, plan_id: int) -> dict | None:
        return await asyncio.to_thread(self._get_trade_plan_sync, plan_id)

    def _get_trade_plan_sync(self, plan_id: int) -> dict | None:
        with self._thread_lock:
            row = self._connection.execute("SELECT * FROM trade_plans WHERE id = ?", (plan_id,)).fetchone()
            return dict(row) if row is not None else None

    async def update_trade_plan_status(self, plan_id: int, status: str) -> None:
        await asyncio.to_thread(self._update_trade_plan_status_sync, plan_id, status)

    def _update_trade_plan_status_sync(self, plan_id: int, status: str) -> None:
        with self._thread_lock:
            self._connection.execute("UPDATE trade_plans SET status = ? WHERE id = ?", (status, plan_id))
            self._connection.commit()

    async def record_trade_plan_event(self, plan_id: int, event_type: str, price: float | None, raw: dict) -> bool:
        return await asyncio.to_thread(self._record_trade_plan_event_sync, plan_id, event_type, price, raw)

    def _record_trade_plan_event_sync(self, plan_id: int, event_type: str, price: float | None, raw: dict) -> bool:
        with self._thread_lock:
            existing = self._connection.execute(
                "SELECT id FROM trade_plan_events WHERE plan_id = ? AND event_type = ?",
                (plan_id, event_type),
            ).fetchone()
            if existing is not None:
                return False
            self._connection.execute(
                """
                INSERT INTO trade_plan_events (plan_id, event_type, price, created_at, sent_to_telegram, raw_json)
                VALUES (?, ?, ?, ?, 0, ?)
                """,
                (plan_id, event_type, price, self._now_iso(), json.dumps(raw)),
            )
            self._connection.commit()
            return True

    async def list_open_trade_plans(self) -> list[dict]:
        return await self.list_trade_plans(statuses=ACTIVE_TRADE_PLAN_STATUSES)

    async def active_plan_exists(self, symbol: str, direction: str) -> bool:
        return await asyncio.to_thread(self._active_plan_exists_sync, symbol, direction)

    def _active_plan_exists_sync(self, symbol: str, direction: str) -> bool:
        placeholders = ", ".join("?" for _ in ACTIVE_TRADE_PLAN_STATUSES)
        with self._thread_lock:
            row = self._connection.execute(
                f"""
                SELECT 1
                FROM trade_plans
                WHERE symbol = ? AND direction = ? AND status IN ({placeholders})
                LIMIT 1
                """,
                (symbol.upper(), direction.upper(), *ACTIVE_TRADE_PLAN_STATUSES),
            ).fetchone()
        return row is not None

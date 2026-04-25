import asyncio
import logging
import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.storage import ACTIVE_TRADE_PLAN_STATUSES, Storage, TERMINAL_TRADE_PLAN_STATUSES


def run(coro):
    return asyncio.run(coro)


def test_active_statuses_canonical_set() -> None:
    assert set(ACTIVE_TRADE_PLAN_STATUSES) == {
        "CREATED",
        "ENTRY_TOUCHED",
        "ACTIVE_ASSUMED",
        "TP1_HIT",
        "TP2_HIT",
    }
    terminal_expected = {"TP3_HIT", "SL_HIT", "EXPIRED", "CANCELLED", "INVALIDATED", "RESOLVED"}
    assert set(TERMINAL_TRADE_PLAN_STATUSES) == terminal_expected
    assert terminal_expected.isdisjoint(set(ACTIVE_TRADE_PLAN_STATUSES))


def _create_legacy_trade_plans_table(db_path: Path) -> None:
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE trade_plans (
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
            """
        )
        now = datetime.now(tz=timezone.utc)
        rows = [
            ("BTCUSDT", "LONG", "CREATED", (now - timedelta(minutes=2)).isoformat(), "fp-old"),
            ("BTCUSDT", "LONG", "TP1_HIT", (now - timedelta(minutes=1)).isoformat(), "fp-new"),
        ]
        for symbol, direction, status, created_at, fp in rows:
            conn.execute(
                """
                INSERT INTO trade_plans (
                    symbol, direction, signal_type, quality, entry_low, entry_high, stop_loss,
                    tp1, tp2, tp3, rr_tp1, rr_tp2, rr_tp3, status, created_at, expires_at,
                    source_alert_id, plan_fingerprint, raw_json
                ) VALUES (?, ?, 'LONG_PULLBACK', 'ALTA', 99, 100, 95, 101, 102, 103, 1, 2, 3, ?, ?, ?, 'legacy', ?, '{}')
                """,
                (
                    symbol,
                    direction,
                    status,
                    created_at,
                    (now + timedelta(hours=1)).isoformat(),
                    fp,
                ),
            )
        conn.commit()
    finally:
        conn.close()


def test_partial_unique_index_migration_repairs_existing_duplicates(tmp_path: Path) -> None:
    async def scenario() -> None:
        db_path = tmp_path / "duplicates.sqlite"
        _create_legacy_trade_plans_table(db_path)
        storage = Storage(db_path)
        try:
            await storage.initialize()
            plans = await storage.list_trade_plans()
            active = [p for p in plans if p["status"] in ACTIVE_TRADE_PLAN_STATUSES]
            cancelled = [p for p in plans if p["status"] == "CANCELLED"]
            assert len(active) == 1
            assert len(cancelled) == 1

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            try:
                index_sql = conn.execute(
                    "SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_one_active_plan_per_symbol_side'"
                ).fetchone()["sql"]
            finally:
                conn.close()
            assert "TP3_HIT" not in index_sql
            assert "TP2_HIT" in index_sql

            with pytest.raises(sqlite3.IntegrityError):
                await storage.create_trade_plan(
                    {
                        "symbol": "BTCUSDT",
                        "direction": "LONG",
                        "signal_type": "LONG_PULLBACK",
                        "quality": "ALTA",
                        "entry_zone_low": 99.0,
                        "entry_zone_high": 100.0,
                        "stop_loss": 95.0,
                        "tp1": 101.0,
                        "tp2": 102.0,
                        "tp3": 103.0,
                        "rr_tp1": 1.0,
                        "rr_tp2": 2.0,
                        "rr_tp3": 3.0,
                        "status": "CREATED",
                        "created_at": datetime.now(tz=timezone.utc).isoformat(),
                        "expires_at": (datetime.now(tz=timezone.utc) + timedelta(minutes=120)).isoformat(),
                        "source_alert_id": "new",
                        "plan_fingerprint": "new-active",
                    }
                )
        finally:
            await storage.close()

    run(scenario())


def test_partial_unique_index_creation_logs_or_aborts_on_unrepairable_duplicates(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "broken.sqlite")
        storage._repair_duplicate_active_trade_plans_sync = lambda: (_ for _ in ()).throw(RuntimeError("forced_migration_error"))  # type: ignore[method-assign]  # noqa: SLF001,E501
        with pytest.raises(RuntimeError, match="forced_migration_error"):
            await storage.initialize()
        await storage.close()

    caplog.set_level(logging.ERROR)
    run(scenario())
    assert "failed_trade_plan_active_index_migration" in caplog.text


def test_tp3_hit_is_terminal_not_active(tmp_path: Path) -> None:
    async def scenario() -> None:
        storage = Storage(tmp_path / "tp3-terminal.sqlite")
        await storage.initialize()
        try:
            await storage.create_trade_plan(
                {
                    "symbol": "BTCUSDT",
                    "direction": "LONG",
                    "signal_type": "LONG_PULLBACK",
                    "quality": "ALTA",
                    "entry_zone_low": 99.0,
                    "entry_zone_high": 100.0,
                    "stop_loss": 95.0,
                    "tp1": 101.0,
                    "tp2": 102.0,
                    "tp3": 103.0,
                    "rr_tp1": 1.0,
                    "rr_tp2": 2.0,
                    "rr_tp3": 3.0,
                    "status": "TP3_HIT",
                    "created_at": datetime.now(tz=timezone.utc).isoformat(),
                    "expires_at": (datetime.now(tz=timezone.utc) + timedelta(minutes=120)).isoformat(),
                    "source_alert_id": "tp3",
                    "plan_fingerprint": "tp3-fp",
                }
            )
            assert await storage.active_plan_exists("BTCUSDT", "LONG") is False
            await storage.create_trade_plan(
                {
                    "symbol": "BTCUSDT",
                    "direction": "LONG",
                    "signal_type": "LONG_PULLBACK",
                    "quality": "ALTA",
                    "entry_zone_low": 99.0,
                    "entry_zone_high": 100.0,
                    "stop_loss": 95.0,
                    "tp1": 101.0,
                    "tp2": 102.0,
                    "tp3": 103.0,
                    "rr_tp1": 1.0,
                    "rr_tp2": 2.0,
                    "rr_tp3": 3.0,
                    "status": "CREATED",
                    "created_at": datetime.now(tz=timezone.utc).isoformat(),
                    "expires_at": (datetime.now(tz=timezone.utc) + timedelta(minutes=120)).isoformat(),
                    "source_alert_id": "new",
                    "plan_fingerprint": "new-fp",
                }
            )
        finally:
            await storage.close()

    run(scenario())

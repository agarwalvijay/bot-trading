"""
SQLite persistence layer.

Schema
------
opportunities
    id             INTEGER  PRIMARY KEY AUTOINCREMENT
    detected_at    TEXT     ISO-8601 timestamp (UTC)
    ticker         TEXT     Kalshi market ticker (e.g. "FED-25MAR-T4.50")
    event_ticker   TEXT     Parent event ticker (empty for standalone binary)
    title          TEXT     Human-readable market title
    outcomes       TEXT     JSON array of outcome labels
    ask_prices     TEXT     JSON array of best-ask prices (same order as outcomes)
    ask_sizes      TEXT     JSON array of available contracts at best ask
    sum_asks       REAL     Sum of best-ask prices across all outcomes
    gross_profit   REAL     1 – sum_asks
    total_fees     REAL     Sum of parabolic taker fees per leg
    net_profit     REAL     gross_profit – total_fees
    taker_fee_coeff REAL    Fee coefficient used (0.07 = standard taker)
    source         TEXT     "REST" | "WS"
    category       TEXT     "opportunity" | "near_miss"
    has_zero_size  INTEGER  1 if any leg has size <= 0
    close_time     TEXT     Market close datetime
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any

from config import DB_PATH


@contextmanager
def _conn():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    try:
        yield con
        con.commit()
    finally:
        con.close()


def init_db() -> None:
    """Create tables and apply any missing column migrations."""
    with _conn() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS opportunities (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at     TEXT    NOT NULL,
                ticker          TEXT    NOT NULL,
                event_ticker    TEXT    NOT NULL DEFAULT '',
                title           TEXT    NOT NULL,
                outcomes        TEXT    NOT NULL,
                ask_prices      TEXT    NOT NULL,
                ask_sizes       TEXT    NOT NULL,
                sum_asks        REAL    NOT NULL,
                gross_profit    REAL    NOT NULL,
                total_fees      REAL    NOT NULL,
                net_profit      REAL    NOT NULL,
                taker_fee_coeff REAL    NOT NULL,
                source          TEXT    NOT NULL DEFAULT 'REST',
                category        TEXT    NOT NULL DEFAULT 'opportunity',
                has_zero_size   INTEGER NOT NULL DEFAULT 0,
                close_time      TEXT
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_opp_detected
            ON opportunities (detected_at DESC)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_opp_ticker
            ON opportunities (ticker)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_opp_category
            ON opportunities (category)
        """)
        # Migration: add columns that may be absent in existing databases
        _add_column_if_missing(con, "opportunities", "event_ticker",    "TEXT NOT NULL DEFAULT ''")
        _add_column_if_missing(con, "opportunities", "close_time",      "TEXT")
        _add_column_if_missing(con, "opportunities", "taker_fee_coeff", "REAL NOT NULL DEFAULT 0.07")


def _add_column_if_missing(con: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def save_opportunity(opp: dict[str, Any]) -> int:
    """Insert one opportunity or near-miss row; returns the new row id."""
    with _conn() as con:
        cur = con.execute("""
            INSERT INTO opportunities
                (detected_at, ticker, event_ticker, title, outcomes,
                 ask_prices, ask_sizes, sum_asks, gross_profit, total_fees,
                 net_profit, taker_fee_coeff, source, category, has_zero_size, close_time)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            opp["ticker"],
            opp.get("event_ticker", ""),
            opp["title"],
            json.dumps(opp["outcomes"]),
            json.dumps(opp["ask_prices"]),
            json.dumps(opp["ask_sizes"]),
            opp["sum_asks"],
            opp["gross_profit"],
            opp["total_fees"],
            opp["net_profit"],
            opp.get("taker_fee_coeff", 0.07),
            opp.get("source", "REST"),
            opp.get("category", "opportunity"),
            1 if opp.get("has_zero_size") else 0,
            opp.get("close_time"),
        ))
        return cur.lastrowid


def update_opportunity(row_id: int, opp: dict[str, Any]) -> None:
    """Update prices and timestamp on an existing opportunity row (price changed)."""
    with _conn() as con:
        con.execute("""
            UPDATE opportunities SET
                detected_at   = ?,
                ask_prices    = ?,
                ask_sizes     = ?,
                sum_asks      = ?,
                gross_profit  = ?,
                total_fees    = ?,
                net_profit    = ?,
                has_zero_size = ?
            WHERE id = ?
        """, (
            datetime.now(timezone.utc).isoformat(),
            json.dumps(opp["ask_prices"]),
            json.dumps(opp["ask_sizes"]),
            opp["sum_asks"],
            opp["gross_profit"],
            opp["total_fees"],
            opp["net_profit"],
            1 if opp.get("has_zero_size") else 0,
            row_id,
        ))


def get_recent_opportunities(limit: int = 50) -> list[dict]:
    """Return the most-recent logged opportunities as plain dicts."""
    with _conn() as con:
        rows = con.execute("""
            SELECT * FROM opportunities
            ORDER BY detected_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
    return [dict(r) for r in rows]


def opportunity_count() -> int:
    with _conn() as con:
        return con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]

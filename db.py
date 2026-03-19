"""
SQLite persistence layer.

Schema
------
opportunities
    id             INTEGER  PRIMARY KEY AUTOINCREMENT
    detected_at    TEXT     ISO-8601 timestamp (UTC)
    condition_id   TEXT     Polymarket condition/market id
    question       TEXT     Human-readable market question
    token_ids      TEXT     JSON array of outcome token ids
    outcomes       TEXT     JSON array of outcome labels
    ask_prices     TEXT     JSON array of best-ask prices (same order as tokens)
    ask_sizes      TEXT     JSON array of available size at best ask
    sum_asks       REAL     Sum of best-ask prices across all outcomes
    gross_profit   REAL     1 – sum_asks
    total_fees     REAL     fee_rate * sum_asks
    net_profit     REAL     gross_profit – total_fees
    fee_rate       REAL     Fee rate used for calculation
    source         TEXT     "REST" | "WS"
    category       TEXT     "opportunity" | "near_miss"
    has_zero_size  INTEGER  1 if any leg has size <= 0 (seed-price ghost)
    end_date_iso   TEXT     Market end date (only for net_profit > HIGH_PROFIT_THRESHOLD)
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
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                detected_at    TEXT    NOT NULL,
                condition_id   TEXT    NOT NULL,
                question       TEXT    NOT NULL,
                token_ids      TEXT    NOT NULL,
                outcomes       TEXT    NOT NULL,
                ask_prices     TEXT    NOT NULL,
                ask_sizes      TEXT    NOT NULL,
                sum_asks       REAL    NOT NULL,
                gross_profit   REAL    NOT NULL,
                total_fees     REAL    NOT NULL,
                net_profit     REAL    NOT NULL,
                fee_rate       REAL    NOT NULL,
                source         TEXT    NOT NULL DEFAULT 'REST',
                category       TEXT    NOT NULL DEFAULT 'opportunity',
                has_zero_size  INTEGER NOT NULL DEFAULT 0,
                end_date_iso   TEXT
            )
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_opp_detected
            ON opportunities (detected_at DESC)
        """)
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_opp_condition
            ON opportunities (condition_id)
        """)
        # Migration: add columns that may be absent in existing databases.
        # Must run before any index that references these columns.
        _add_column_if_missing(con, "opportunities", "category",      "TEXT NOT NULL DEFAULT 'opportunity'")
        _add_column_if_missing(con, "opportunities", "has_zero_size", "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(con, "opportunities", "end_date_iso",  "TEXT")
        con.execute("""
            CREATE INDEX IF NOT EXISTS idx_opp_category
            ON opportunities (category)
        """)


def _add_column_if_missing(con: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def save_opportunity(opp: dict[str, Any]) -> int:
    """Insert one opportunity or near-miss row; returns the new row id."""
    with _conn() as con:
        cur = con.execute("""
            INSERT INTO opportunities
                (detected_at, condition_id, question, token_ids, outcomes,
                 ask_prices, ask_sizes, sum_asks, gross_profit, total_fees,
                 net_profit, fee_rate, source, category, has_zero_size, end_date_iso)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            datetime.now(timezone.utc).isoformat(),
            opp["condition_id"],
            opp["question"],
            json.dumps(opp["token_ids"]),
            json.dumps(opp["outcomes"]),
            json.dumps(opp["ask_prices"]),
            json.dumps(opp["ask_sizes"]),
            opp["sum_asks"],
            opp["gross_profit"],
            opp["total_fees"],
            opp["net_profit"],
            opp["fee_rate"],
            opp.get("source", "REST"),
            opp.get("category", "opportunity"),
            1 if opp.get("has_zero_size") else 0,
            opp.get("end_date_iso"),
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

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

markets_cache
    ticker         TEXT     PRIMARY KEY — Kalshi market ticker
    event_ticker   TEXT
    title          TEXT
    close_time     TEXT
    seed_yes_ask   REAL
    seed_no_ask    REAL
    seed_yes_ask_size REAL
    seed_no_ask_size  REAL
    volume_24h     REAL
    cached_at      TEXT     ISO-8601 timestamp when this row was last written
"""

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
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
            CREATE TABLE IF NOT EXISTS markets_cache (
                ticker            TEXT PRIMARY KEY,
                event_ticker      TEXT NOT NULL DEFAULT '',
                title             TEXT NOT NULL DEFAULT '',
                close_time        TEXT,
                seed_yes_ask      REAL,
                seed_no_ask       REAL,
                seed_yes_ask_size REAL NOT NULL DEFAULT 0,
                seed_no_ask_size  REAL NOT NULL DEFAULT 0,
                volume_24h        REAL NOT NULL DEFAULT 0,
                cached_at         TEXT NOT NULL
            )
        """)
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


# ---------------------------------------------------------------------------
# Markets cache
# ---------------------------------------------------------------------------

def save_markets_cache(markets: dict) -> None:
    """Bulk-upsert the current markets_by_ticker dict into markets_cache."""
    now = datetime.now(timezone.utc).isoformat()
    rows = [
        (
            m.get("ticker", ""),
            m.get("event_ticker", ""),
            m.get("title", ""),
            m.get("close_time"),
            m.get("seed_yes_ask"),
            m.get("seed_no_ask"),
            m.get("seed_yes_ask_size", 0.0),
            m.get("seed_no_ask_size", 0.0),
            m.get("volume_24h", 0.0),
            now,
        )
        for m in markets.values()
        if m.get("ticker")
    ]
    with _conn() as con:
        con.executemany("""
            INSERT OR REPLACE INTO markets_cache
                (ticker, event_ticker, title, close_time,
                 seed_yes_ask, seed_no_ask, seed_yes_ask_size, seed_no_ask_size,
                 volume_24h, cached_at)
            VALUES (?,?,?,?,?,?,?,?,?,?)
        """, rows)
        # Prune rows that have been closed for more than 2 hours
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        con.execute("""
            DELETE FROM markets_cache
            WHERE close_time IS NOT NULL AND close_time < ?
        """, (cutoff,))


def load_markets_from_cache() -> list[dict]:
    """
    Return cached markets that haven't closed yet.
    Markets with no close_time are always included.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        rows = con.execute("""
            SELECT ticker, event_ticker, title, close_time,
                   seed_yes_ask, seed_no_ask, seed_yes_ask_size, seed_no_ask_size,
                   volume_24h, cached_at
            FROM markets_cache
            WHERE close_time IS NULL OR close_time > ?
            ORDER BY volume_24h DESC
        """, (now_iso,)).fetchall()
    return [dict(r) for r in rows]


def markets_cache_count() -> int:
    with _conn() as con:
        return con.execute("SELECT COUNT(*) FROM markets_cache").fetchone()[0]

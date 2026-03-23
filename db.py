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
    authorized     INTEGER  1 if user has authorized this opportunity for trading
    trade_id       INTEGER  FK → trades.id once a trade is in progress/complete

trades
    id                  INTEGER  PRIMARY KEY AUTOINCREMENT
    opportunity_id      INTEGER  FK → opportunities.id
    status              TEXT     preflight_failed|phase1_placed|phase1_filled|
                                 phase2_placed|complete|
                                 unwind_retry|unwind_limit|unwind_market|
                                 unwind_hold|unwind_failed|aborted
    demo_mode           INTEGER  1 if placed on demo API
    started_at          TEXT     ISO-8601
    completed_at        TEXT     ISO-8601
    leg_tickers         TEXT     JSON array of market tickers (leg order = execution order)
    leg_counts          TEXT     JSON array of contract counts requested
    target_prices       TEXT     JSON array of prices at execution time (floats 0-1)
    client_order_ids    TEXT     JSON array of UUIDs (for idempotency)
    order_ids           TEXT     JSON array of Kalshi order IDs
    fill_prices         TEXT     JSON array of actual fill prices
    fill_counts         TEXT     JSON array of actual filled contract counts
    gross_pnl           REAL
    fee_pnl             REAL
    net_pnl             REAL
    unwind_reason       TEXT
    notes               TEXT

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
from typing import Any, Optional

from config import DB_PATH
from market_key import canonical_market_key


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
        con.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                opportunity_id   INTEGER,
                status           TEXT NOT NULL DEFAULT 'pending',
                demo_mode        INTEGER NOT NULL DEFAULT 1,
                started_at       TEXT,
                completed_at     TEXT,
                leg_tickers      TEXT NOT NULL DEFAULT '[]',
                leg_counts       TEXT NOT NULL DEFAULT '[]',
                target_prices    TEXT NOT NULL DEFAULT '[]',
                client_order_ids TEXT NOT NULL DEFAULT '[]',
                order_ids        TEXT NOT NULL DEFAULT '[]',
                fill_prices      TEXT NOT NULL DEFAULT '[]',
                fill_counts      TEXT NOT NULL DEFAULT '[]',
                gross_pnl        REAL,
                fee_pnl          REAL,
                net_pnl          REAL,
                unwind_reason    TEXT,
                notes            TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS authorized_markets (
                market_key          TEXT PRIMARY KEY,
                active              INTEGER NOT NULL DEFAULT 1,
                authorized_at       TEXT NOT NULL,
                authorized_by_row_id INTEGER,
                successful_trade_id INTEGER,
                successful_order_id TEXT,
                successful_at       TEXT,
                disabled_at         TEXT,
                disabled_reason     TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS ws_counter (
                id         INTEGER PRIMARY KEY CHECK (id = 1),
                count      INTEGER NOT NULL DEFAULT 0,
                started_at TEXT,
                updated_at TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS ws_counter_watched (
                id         INTEGER PRIMARY KEY CHECK (id = 1),
                count      INTEGER NOT NULL DEFAULT 0,
                started_at TEXT,
                updated_at TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS metric_counters (
                metric     TEXT PRIMARY KEY,
                count      INTEGER NOT NULL DEFAULT 0,
                started_at TEXT,
                updated_at TEXT
            )
        """)
        con.execute("""
            CREATE TABLE IF NOT EXISTS trade_attempts (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                opportunity_id   INTEGER,
                market_key       TEXT NOT NULL DEFAULT '',
                triggered_by     TEXT NOT NULL DEFAULT 'event',   -- event | poll
                triggered_at     TEXT NOT NULL,
                preflight_ok     INTEGER,
                preflight_reason TEXT,
                order_attempted  INTEGER NOT NULL DEFAULT 0,
                first_order_id   TEXT,
                trade_id         INTEGER,
                final_status     TEXT
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_attempts_triggered_at ON trade_attempts (triggered_at DESC)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_attempts_market_key ON trade_attempts (market_key)")
        con.execute("""
            INSERT OR IGNORE INTO ws_counter (id, count, started_at, updated_at)
            VALUES (1, 0, NULL, NULL)
        """)
        con.execute("""
            INSERT OR IGNORE INTO ws_counter_watched (id, count, started_at, updated_at)
            VALUES (1, 0, NULL, NULL)
        """)
        _add_column_if_missing(con, "trades",        "exit_prices",      "TEXT NOT NULL DEFAULT '[]'")
        _add_column_if_missing(con, "trades",        "exit_pnl",         "REAL")
        _add_column_if_missing(con, "trades",        "market_key",       "TEXT NOT NULL DEFAULT ''")
        _add_column_if_missing(con, "opportunities", "event_ticker",     "TEXT NOT NULL DEFAULT ''")
        _add_column_if_missing(con, "opportunities", "close_time",       "TEXT")
        _add_column_if_missing(con, "opportunities", "taker_fee_coeff",  "REAL NOT NULL DEFAULT 0.07")
        _add_column_if_missing(con, "opportunities", "outcome_tickers",  "TEXT NOT NULL DEFAULT '[]'")
        _add_column_if_missing(con, "opportunities", "authorized",       "INTEGER NOT NULL DEFAULT 0")
        _add_column_if_missing(con, "opportunities", "trade_id",         "INTEGER")
        _add_column_if_missing(con, "opportunities", "volume_24h",       "REAL NOT NULL DEFAULT 0")
        _add_column_if_missing(con, "opportunities", "event_slug",         "TEXT NOT NULL DEFAULT ''")
        _add_column_if_missing(con, "opportunities", "trader_invoked_at", "TEXT")
        _add_column_if_missing(con, "opportunities", "market_key",        "TEXT NOT NULL DEFAULT ''")
        _add_column_if_missing(con, "markets_cache", "subtitle",          "TEXT NOT NULL DEFAULT ''")
        con.execute("CREATE INDEX IF NOT EXISTS idx_opp_market_key ON opportunities (market_key)")

        # Backfill market_key for older rows.
        rows = con.execute("""
            SELECT id, ticker, event_ticker, outcome_tickers
            FROM opportunities
            WHERE market_key IS NULL OR market_key = ''
        """).fetchall()
        for r in rows:
            mkey = canonical_market_key(
                r["ticker"],
                r["event_ticker"],
                r["outcome_tickers"],
            )
            con.execute("UPDATE opportunities SET market_key = ? WHERE id = ?", (mkey, r["id"]))


def _add_column_if_missing(con: sqlite3.Connection, table: str, column: str, definition: str) -> None:
    existing = {row[1] for row in con.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


def save_opportunity(opp: dict[str, Any]) -> int:
    """Insert one opportunity or near-miss row; returns the new row id."""
    outcome_tickers = opp.get("outcome_tickers", [])
    market_key = canonical_market_key(
        opp.get("ticker", ""),
        opp.get("event_ticker", ""),
        outcome_tickers,
    )
    with _conn() as con:
        auth_row = con.execute("""
            SELECT 1 FROM authorized_markets
            WHERE market_key = ? AND active = 1 AND successful_at IS NULL
            LIMIT 1
        """, (market_key,)).fetchone()
        cur = con.execute("""
            INSERT INTO opportunities
                (detected_at, ticker, event_ticker, title, outcomes,
                 ask_prices, ask_sizes, sum_asks, gross_profit, total_fees,
                 net_profit, taker_fee_coeff, source, category, has_zero_size,
                 close_time, outcome_tickers, volume_24h, event_slug, market_key, authorized)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
            json.dumps(outcome_tickers),
            opp.get("volume_24h", 0.0),
            opp.get("event_slug", ""),
            market_key,
            1 if auth_row else 0,
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
            m.get("subtitle", ""),
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
                (ticker, event_ticker, title, subtitle, close_time,
                 seed_yes_ask, seed_no_ask, seed_yes_ask_size, seed_no_ask_size,
                 volume_24h, cached_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """, rows)
        # Prune rows that have been closed for more than 2 hours
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        con.execute("""
            DELETE FROM markets_cache
            WHERE close_time IS NOT NULL AND close_time < ?
        """, (cutoff,))


def load_markets_from_cache(min_volume_24h: float = 0.0) -> list[dict]:
    """
    Return cached markets that haven't closed yet.
    Markets with no close_time are always included.
    min_volume_24h: skip markets below this 24h volume threshold (0 = no filter).
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        rows = con.execute("""
            SELECT ticker, event_ticker, title, subtitle, close_time,
                   seed_yes_ask, seed_no_ask, seed_yes_ask_size, seed_no_ask_size,
                   volume_24h, cached_at
            FROM markets_cache
            WHERE (close_time IS NULL OR close_time > ?)
              AND (? <= 0 OR volume_24h >= ?)
            ORDER BY volume_24h DESC
        """, (now_iso, min_volume_24h, min_volume_24h)).fetchall()
    return [dict(r) for r in rows]


def markets_cache_count() -> int:
    with _conn() as con:
        return con.execute("SELECT COUNT(*) FROM markets_cache").fetchone()[0]


# ---------------------------------------------------------------------------
# Trading
# ---------------------------------------------------------------------------

def get_authorized_opportunities() -> list[dict]:
    """Return latest opportunity per authorized market eligible for trading."""
    now_iso = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        rows = con.execute("""
            SELECT o.* FROM opportunities o
            JOIN (
                SELECT market_key, MAX(detected_at) AS max_detected
                FROM opportunities
                WHERE category = 'opportunity'
                GROUP BY market_key
            ) latest
              ON latest.market_key = o.market_key
             AND latest.max_detected = o.detected_at
            JOIN authorized_markets am
              ON am.market_key = o.market_key
             AND am.active = 1
             AND am.successful_at IS NULL
            LEFT JOIN trades t ON o.trade_id = t.id
            WHERE o.category   = 'opportunity'
              AND o.market_key <> ''
              AND (o.close_time IS NULL OR o.close_time > ?)
              AND (o.trade_id IS NULL OR o.trade_id = 0 OR t.status = 'aborted')
            ORDER BY o.net_profit DESC
        """, (now_iso,)).fetchall()
    return [dict(r) for r in rows]


def set_authorized(row_id: int, authorized: bool) -> None:
    with _conn() as con:
        row = con.execute("""
            SELECT id, ticker, event_ticker, outcome_tickers
            FROM opportunities
            WHERE id = ?
        """, (row_id,)).fetchone()
        if not row:
            return
        market_key = canonical_market_key(
            row["ticker"],
            row["event_ticker"],
            row["outcome_tickers"],
        )
        now = datetime.now(timezone.utc).isoformat()
        if authorized:
            con.execute("""
                INSERT INTO authorized_markets
                    (market_key, active, authorized_at, authorized_by_row_id,
                     successful_trade_id, successful_order_id, successful_at, disabled_at, disabled_reason)
                VALUES (?, 1, ?, ?, NULL, NULL, NULL, NULL, NULL)
                ON CONFLICT(market_key) DO UPDATE SET
                    active = 1,
                    authorized_at = excluded.authorized_at,
                    authorized_by_row_id = excluded.authorized_by_row_id,
                    successful_trade_id = NULL,
                    successful_order_id = NULL,
                    successful_at = NULL,
                    disabled_at = NULL,
                    disabled_reason = NULL
            """, (market_key, now, row_id))
            con.execute("UPDATE opportunities SET authorized = 1 WHERE market_key = ?", (market_key,))
        else:
            con.execute("""
                UPDATE authorized_markets
                SET active = 0, disabled_at = ?, disabled_reason = 'manual'
                WHERE market_key = ?
            """, (now, market_key))
            con.execute("UPDATE opportunities SET authorized = 0 WHERE market_key = ?", (market_key,))


def is_market_authorized(market_key: str) -> bool:
    with _conn() as con:
        row = con.execute("""
            SELECT 1 FROM authorized_markets
            WHERE market_key = ?
              AND active = 1
              AND successful_at IS NULL
            LIMIT 1
        """, (market_key,)).fetchone()
    return row is not None


def get_authorized_market_keys() -> list[str]:
    with _conn() as con:
        rows = con.execute("""
            SELECT market_key
            FROM authorized_markets
            WHERE active = 1
              AND successful_at IS NULL
            ORDER BY authorized_at DESC
        """).fetchall()
    return [r["market_key"] for r in rows]


def mark_market_success(market_key: str, trade_id: int, order_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute("""
            UPDATE authorized_markets
            SET active = 0,
                successful_trade_id = ?,
                successful_order_id = ?,
                successful_at = ?,
                disabled_at = ?,
                disabled_reason = 'success'
            WHERE market_key = ?
        """, (trade_id, order_id, now, now, market_key))
        con.execute("UPDATE opportunities SET authorized = 0 WHERE market_key = ?", (market_key,))


def disable_market_authorization(market_key: str, reason: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        con.execute("""
            UPDATE authorized_markets
            SET active = 0,
                disabled_at = ?,
                disabled_reason = ?
            WHERE market_key = ?
              AND active = 1
              AND successful_at IS NULL
        """, (now, reason, market_key))
        con.execute("UPDATE opportunities SET authorized = 0 WHERE market_key = ?", (market_key,))


def increment_ws_counter(delta: int = 1, max_count: int = 10000) -> None:
    """
    Increment persistent WS notification counter.
    Resets to 0 and restarts timer when count reaches max_count.
    """
    if delta <= 0:
        return
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        row = con.execute(
            "SELECT count, started_at FROM ws_counter WHERE id = 1"
        ).fetchone()
        if not row:
            con.execute(
                "INSERT INTO ws_counter (id, count, started_at, updated_at) VALUES (1, 0, ?, ?)",
                (now, now),
            )
            curr = 0
            started_at = now
        else:
            curr = int(row["count"] or 0)
            started_at = row["started_at"] or now

        new_count = curr + delta
        if new_count >= max_count:
            con.execute(
                "UPDATE ws_counter SET count = 0, started_at = ?, updated_at = ? WHERE id = 1",
                (now, now),
            )
        else:
            con.execute(
                "UPDATE ws_counter SET count = ?, started_at = ?, updated_at = ? WHERE id = 1",
                (new_count, started_at, now),
            )


def increment_ws_watched_counter(delta: int = 1, max_count: int = 10000) -> None:
    """
    Increment persistent WS notification counter for authorized/watched markets.
    Resets to 0 and restarts timer when count reaches max_count.
    """
    if delta <= 0:
        return
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        row = con.execute(
            "SELECT count, started_at FROM ws_counter_watched WHERE id = 1"
        ).fetchone()
        if not row:
            con.execute(
                "INSERT INTO ws_counter_watched (id, count, started_at, updated_at) VALUES (1, 0, ?, ?)",
                (now, now),
            )
            curr = 0
            started_at = now
        else:
            curr = int(row["count"] or 0)
            started_at = row["started_at"] or now

        new_count = curr + delta
        if new_count >= max_count:
            con.execute(
                "UPDATE ws_counter_watched SET count = 0, started_at = ?, updated_at = ? WHERE id = 1",
                (now, now),
            )
        else:
            con.execute(
                "UPDATE ws_counter_watched SET count = ?, started_at = ?, updated_at = ? WHERE id = 1",
                (new_count, started_at, now),
            )


def increment_metric_counter(metric: str, delta: int = 1, max_count: int = 10000) -> None:
    """
    Increment a named persistent metric counter.
    Resets to 0 and restarts timer when count reaches max_count.
    """
    if delta <= 0:
        return
    m = (metric or "").strip()
    if not m:
        return
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        row = con.execute(
            "SELECT count, started_at FROM metric_counters WHERE metric = ?",
            (m,),
        ).fetchone()
        if not row:
            con.execute(
                "INSERT INTO metric_counters (metric, count, started_at, updated_at) VALUES (?, 0, ?, ?)",
                (m, now, now),
            )
            curr = 0
            started_at = now
        else:
            curr = int(row["count"] or 0)
            started_at = row["started_at"] or now

        new_count = curr + delta
        if new_count >= max_count:
            con.execute(
                "UPDATE metric_counters SET count = 0, started_at = ?, updated_at = ? WHERE metric = ?",
                (now, now, m),
            )
        else:
            con.execute(
                "UPDATE metric_counters SET count = ?, started_at = ?, updated_at = ? WHERE metric = ?",
                (new_count, started_at, now, m),
            )


def create_trade_attempt(opportunity_id: int, market_key: str, triggered_by: str) -> int:
    """Insert an attempt audit row and return attempt_id."""
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        cur = con.execute("""
            INSERT INTO trade_attempts
                (opportunity_id, market_key, triggered_by, triggered_at)
            VALUES (?, ?, ?, ?)
        """, (opportunity_id, market_key or "", triggered_by, now))
        return cur.lastrowid


def update_trade_attempt(attempt_id: int, **fields) -> None:
    """Update arbitrary fields on a trade_attempts row."""
    if not attempt_id or not fields:
        return
    sets = ", ".join(f"{k} = ?" for k in fields)
    vals = list(fields.values()) + [attempt_id]
    with _conn() as con:
        con.execute(f"UPDATE trade_attempts SET {sets} WHERE id = ?", vals)


def stamp_trader_invoked(opp_id: int) -> None:
    """Record that execute_trade was called for this opportunity (regardless of outcome)."""
    with _conn() as con:
        con.execute(
            "UPDATE opportunities SET trader_invoked_at = ? WHERE id = ?",
            (datetime.now(timezone.utc).isoformat(), opp_id),
        )


def has_fresh_detection(ticker: str, since_iso: str) -> bool:
    """Return True if a newer opportunity row exists for this ticker since the given timestamp."""
    with _conn() as con:
        row = con.execute(
            "SELECT 1 FROM opportunities WHERE ticker = ? AND detected_at > ? LIMIT 1",
            (ticker, since_iso),
        ).fetchone()
    return row is not None


def mark_likely_resolved(opp_id: int) -> None:
    """Deauthorize and recategorize an opportunity the trader detected as likely resolved."""
    with _conn() as con:
        row = con.execute("SELECT market_key FROM opportunities WHERE id = ?", (opp_id,)).fetchone()
        con.execute("UPDATE opportunities SET authorized = 0, category = 'likely_resolved' WHERE id = ?", (opp_id,))
        if row and row["market_key"]:
            con.execute("""
                UPDATE authorized_markets
                SET active = 0, disabled_at = ?, disabled_reason = 'likely_resolved'
                WHERE market_key = ?
                  AND active = 1
                  AND successful_at IS NULL
            """, (datetime.now(timezone.utc).isoformat(), row["market_key"]))


def create_trade(opportunity_id: int, leg_tickers: list, leg_counts: list,
                 target_prices: list, client_order_ids: list,
                 demo_mode: bool) -> int:
    """Insert a new trade row and link it to the opportunity. Returns trade id."""
    now = datetime.now(timezone.utc).isoformat()
    with _conn() as con:
        opp = con.execute("SELECT market_key FROM opportunities WHERE id = ?", (opportunity_id,)).fetchone()
        market_key = opp["market_key"] if opp and opp["market_key"] else ""
        cur = con.execute("""
            INSERT INTO trades
                (opportunity_id, status, demo_mode, started_at,
                 leg_tickers, leg_counts, target_prices, client_order_ids, market_key)
            VALUES (?, 'pending', ?, ?, ?, ?, ?, ?, ?)
        """, (
            opportunity_id,
            1 if demo_mode else 0,
            now,
            json.dumps(leg_tickers),
            json.dumps(leg_counts),
            json.dumps(target_prices),
            json.dumps(client_order_ids),
            market_key,
        ))
        trade_id = cur.lastrowid
        con.execute("UPDATE opportunities SET trade_id = ? WHERE id = ?",
                    (trade_id, opportunity_id))
    return trade_id


def update_trade(trade_id: int, **fields) -> None:
    """Update arbitrary fields on a trade row."""
    if not fields:
        return
    sets = ", ".join(f"{k} = ?" for k in fields)
    vals = list(fields.values()) + [trade_id]
    with _conn() as con:
        con.execute(f"UPDATE trades SET {sets} WHERE id = ?", vals)


def get_trade(trade_id: int) -> Optional[dict]:
    with _conn() as con:
        row = con.execute("SELECT * FROM trades WHERE id = ?", (trade_id,)).fetchone()
    return dict(row) if row else None


def get_complete_trades() -> list[dict]:
    """Return all trades with status='complete' that have not yet been settled."""
    with _conn() as con:
        rows = con.execute("""
            SELECT t.*, o.close_time
            FROM trades t
            LEFT JOIN opportunities o ON t.opportunity_id = o.id
            WHERE t.status = 'complete'
            ORDER BY t.completed_at DESC
        """).fetchall()
    return [dict(r) for r in rows]

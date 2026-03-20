"""
Kalshi Arbitrage Bot — Orchestrator & Scanner
==============================================

Detects underround opportunities on Kalshi: sum of best-ask prices across
all outcomes of an event < $1.00.  Buying all outcomes guarantees a $1
payout, so any sum < $1 minus fees is risk-free profit.

Binary markets  : YES ask + NO ask < 1.0
Categorical events : sum of YES ask across all sibling outcome markets < 1.0

Fee model (Kalshi parabolic):
    fee_per_leg = TAKER_FEE_COEFF × price × (1 − price)
    (highest at $0.50 = 1.75¢/contract; near-zero at $0.01 or $0.99)

Usage
-----
    python arb_bot.py [--no-ws]   # --no-ws disables WebSocket, REST-only mode
"""

import argparse
import collections
import logging
import math
import queue
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Optional

from alerter import alert, alert_near_miss, alert_expiring_actionable
from config import (
    EXPIRING_ACTIONABLE_MIN_PROFIT,
    EXPIRING_SOON_MINS,
    TAKER_FEE_COEFF,
    HIGH_PROFIT_THRESHOLD,
    MARKET_REFRESH_INTERVAL,
    MAX_REST_MARKETS,
    NEAR_TERM_HORIZON_HOURS,
    NEAR_TERM_SCAN_INTERVAL,
    MIN_LEG_PRICE,
    MIN_LEG_SIZE,
    MIN_MULTI_OUTCOME_SUM,
    MIN_NET_PROFIT,
    MIN_VOLUME_24H,
    NEAR_MISS_LOWER,
    REST_POLL_INTERVAL,
    SETTLE_ENABLED,
    TRADING_ENABLED,
    WS_ALERT_COOLDOWN_SECS,
    WS_FRESHNESS_SECS,
    WS_MIN_PROFIT_IMPROVEMENT,
)
from db import (
    init_db, opportunity_count, save_opportunity, update_opportunity,
    load_markets_from_cache, save_markets_cache, markets_cache_count,
)
from trader import settle_loop, trading_loop

# Set by WS/REST handlers when a fresh arb opportunity is detected.
# Wakes the trading loop immediately rather than waiting TRADE_POLL_INTERVAL.
_trade_trigger = threading.Event()
# opp_ids pushed here get sorted to the front of the next trading tick.
_trade_priority_ids: collections.deque = collections.deque(maxlen=20)
from fetcher import (
    KalshiWSClient,
    fetch_active_markets,
    fetch_event_market_count,
    fetch_market_prices,
    fetch_near_term_markets,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("arb_bot")

def _slugify(text: str) -> str:
    """Convert a string to a URL-friendly slug (lowercase, hyphens)."""
    return re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")


# ---------------------------------------------------------------------------
# Market state cache
# ---------------------------------------------------------------------------

# markets_by_ticker: ticker -> market dict (title, event_ticker, close_time, …)
markets_by_ticker: dict[str, dict] = {}

# live_prices: ticker -> {yes_ask, yes_ask_size, no_ask, no_ask_size}
# yes_ask / no_ask may be None when there is no live offer on that side.
live_prices: dict[str, dict] = {}
prices_lock = threading.Lock()

# Unified alert cooldown — shared by BOTH REST and WS paths.
# Keyed on ticker (or event_ticker for multi-outcome events).
# Schema: {last_alert_at: datetime, last_net_profit: float}
alert_cooldown: dict[str, dict] = {}
alert_cooldown_lock = threading.Lock()

# REST-only DB state — tracks the active DB row for each live opportunity
# so REST can update it in-place rather than inserting duplicates each cycle.
# Schema: {row_id: int, sum_asks: float}
rest_state: dict[str, dict] = {}
rest_state_lock = threading.Lock()

# WS activity tracking for Option-C REST scheduling.
# ws_activity: ticker -> datetime of most-recent WS price event.
# ws_event_log: deque of (datetime, ticker) for rolling 60s coverage metrics.
ws_activity: dict[str, datetime] = {}
ws_activity_lock = threading.Lock()
ws_event_log: collections.deque = collections.deque()  # type: ignore[type-arg]

# Rolling-chunk REST pointer
_rest_chunk_start: int = 0

# Module-level WS client reference — set in main() before threads start
_ws_client: Optional[KalshiWSClient] = None

# Priority WS tickers — REST-detected opportunity tickers are promoted to the
# front of the WS subscription so they survive the MAX_WS_MARKETS cap.
priority_tickers: set[str] = set()
priority_lock = threading.Lock()

# Cached event groupings — rebuilt after every market list update.
# Avoids O(n) re-grouping on every WS tick.
# {event_key: [market, ...]}  same semantics as _group_by_event()
_event_groups: dict[str, list] = {}
_event_groups_lock = threading.Lock()

# ticker -> event_key reverse index for fast WS lookup
_ticker_to_event_key: dict[str, str] = {}


# Cache of event_ticker -> (total_market_count, fetched_at_ts)
# Avoids re-querying the API for every scan cycle.
_event_size_cache: dict[str, tuple[int, float]] = {}
_EVENT_SIZE_CACHE_TTL = 3600.0  # re-verify once per hour


def _prewarm_event_size_cache(markets: list) -> None:
    """
    Populate _event_size_cache from the full (unfiltered) market list returned
    by fetch_active_markets().  Called once per full scan so that
    _event_is_complete / _event_total_count never need to hit the API during
    normal operation — they just read the pre-warmed cache instead.
    """
    from collections import Counter
    counts = Counter(m.get("event_ticker") for m in markets if m.get("event_ticker"))
    now_ts = time.time()
    for evt, cnt in counts.items():
        _event_size_cache[evt] = (cnt, now_ts)
    logger.debug("Event size cache pre-warmed: %d events", len(counts))


def _event_is_complete(event_ticker: str, tracked: int) -> bool:
    """
    Return True if `tracked` outcomes == all open outcomes for this event.

    Fetches the real count from the API on first call per event (then caches
    for 1 hour).  A mismatch means we're missing outcomes due to the
    volume_24h filter — the apparent underround is spurious.
    """
    now = time.time()
    cached = _event_size_cache.get(event_ticker)
    if cached and (now - cached[1]) < _EVENT_SIZE_CACHE_TTL:
        total = cached[0]
    else:
        total = fetch_event_market_count(event_ticker)
        if total > 0:
            _event_size_cache[event_ticker] = (total, now)

    if total > 0 and total != tracked:
        logger.debug(
            "Incomplete event  %s: tracking %d of %d total outcomes — "
            "sum is partial, not real arb",
            event_ticker, tracked, total,
        )
        return False
    return True


def _event_total_count(event_ticker: str) -> int:
    """Return the API total market count for this event (cached 1 h). 0 = unknown."""
    now = time.time()
    cached = _event_size_cache.get(event_ticker)
    if cached and (now - cached[1]) < _EVENT_SIZE_CACHE_TTL:
        return cached[0]
    total = fetch_event_market_count(event_ticker)
    if total > 0:
        _event_size_cache[event_ticker] = (total, now)
    return total


def _rebuild_event_groups() -> None:
    """Recompute and cache event groupings from the current markets_by_ticker."""
    groups = _group_by_event(list(markets_by_ticker.values()))
    reverse: dict[str, str] = {}
    for key, ms in groups.items():
        for m in ms:
            reverse[m["ticker"]] = key
    with _event_groups_lock:
        _event_groups.clear()
        _event_groups.update(groups)
        _ticker_to_event_key.clear()
        _ticker_to_event_key.update(reverse)
    logger.debug("Event groups rebuilt: %d groups, %d tickers indexed", len(groups), len(reverse))


# ---------------------------------------------------------------------------
# Fee helpers
# ---------------------------------------------------------------------------

def _kalshi_fee(price: float) -> float:
    """
    Kalshi taker fee for 1 contract at `price`.
    fee = TAKER_FEE_COEFF × price × (1 − price)
    """
    return TAKER_FEE_COEFF * price * (1.0 - price)


def _total_fees(ask_prices: list[float]) -> float:
    """Sum of taker fees across all legs (1 contract each)."""
    return sum(_kalshi_fee(p) for p in ask_prices)


# ---------------------------------------------------------------------------
# WS priority subscription helper
# ---------------------------------------------------------------------------

def _prioritize_opportunity_tickers(tickers: list[str]) -> None:
    """
    Promote tickers to the front of the WS subscription list.

    Called by REST when it detects a live opportunity so that subsequent
    price ticks arrive in real-time via WS rather than waiting for the next
    REST polling cycle.  Safe to call when WS is disabled (_ws_client=None).
    """
    global _ws_client
    with priority_lock:
        new_tickers = set(tickers) - priority_tickers
        if not new_tickers:
            return
        priority_tickers.update(new_tickers)

    if _ws_client is None:
        return

    all_t = list(markets_by_ticker.keys())
    all_t_set = set(all_t)
    with priority_lock:
        pri = [t for t in priority_tickers if t in all_t_set]
    remaining = [t for t in all_t if t not in priority_tickers]
    _ws_client.update_subscriptions(pri + remaining)
    logger.info(
        "WS priority bump: +%d tickers promoted  |  total priority=%d",
        len(new_tickers), len(pri),
    )


# ---------------------------------------------------------------------------
# Event grouping helpers
# ---------------------------------------------------------------------------

def _group_by_event(markets: list) -> dict:
    """
    Group markets by their Kalshi event_ticker.

    Markets sharing an event_ticker are sibling outcomes of the same
    categorical event (e.g. "Fed rate decision: 25bps / 50bps / hold").
    Markets with a unique or absent event_ticker are standalone binary markets.

    Returns: {event_key: [market, ...]}
      where event_key is event_ticker for multi-outcome groups
      and ticker for standalone binary markets.
    """
    event_counts: dict[str, int] = {}
    for m in markets:
        eid = m.get("event_ticker", "")
        if eid:
            event_counts[eid] = event_counts.get(eid, 0) + 1

    groups: dict[str, list] = {}
    for m in markets:
        eid = m.get("event_ticker", "")
        if eid and event_counts.get(eid, 0) > 1:
            groups.setdefault(eid, []).append(m)
        else:
            groups[m["ticker"]] = [m]

    return groups


def _log_market_composition() -> None:
    """Log a breakdown of binary vs multi-outcome markets."""
    groups = _group_by_event(list(markets_by_ticker.values()))
    n_binary = sum(1 for ms in groups.values() if len(ms) == 1)
    multi = {k: ms for k, ms in groups.items() if len(ms) > 1}
    n_multi_events  = len(multi)
    n_multi_markets = sum(len(ms) for ms in multi.values())

    n_cumulative = 0
    n_quarterly  = 0
    for ms in multi.values():
        if _is_cumulative_deadline_group(ms):
            n_cumulative += 1
        elif _quarterly_ambiguous_reason(ms):
            n_quarterly += 1
    n_valid_multi = n_multi_events - n_cumulative - n_quarterly

    logger.info(
        "Market composition: %d true binary  |  %d multi-outcome events (%d markets)"
        "  |  cumulative deadline skipped: %d  |  quarterly ambiguous: %d"
        "  |  valid multi-outcome (scannable): %d",
        n_binary, n_multi_events, n_multi_markets,
        n_cumulative, n_quarterly, n_valid_multi,
    )
    if multi:
        samples = sorted(multi.items(), key=lambda kv: len(kv[1]), reverse=True)[:5]
        for eid, ms in samples:
            tag = ""
            if _is_cumulative_deadline_group(ms):
                tag = " [CUMULATIVE-SKIP]"
            elif _quarterly_ambiguous_reason(ms):
                tag = " [QUARTERLY-AMBIGUOUS]"
            logger.info(
                "  Multi-outcome  eid=%s  n=%d%s  first_title=%s",
                eid, len(ms), tag, ms[0].get("title", "?")[:70],
            )


# ---------------------------------------------------------------------------
# Core arbitrage logic
# ---------------------------------------------------------------------------

def compute_opportunity(market: dict, source: str = "REST") -> Optional[dict]:
    """
    Evaluate one binary market for arb / near-miss.

    Checks: YES ask + NO ask < 1.0
    Both sides must have a live offer (not None) and price >= MIN_LEG_PRICE.
    """
    mins = _minutes_to_close(market.get("close_time"))
    if mins is not None and mins < -10:
        return None  # resolved

    ticker = market["ticker"]
    with prices_lock:
        entry = live_prices.get(ticker)

    if entry is None:
        return None  # never polled

    yes_ask      = entry.get("yes_ask")
    no_ask       = entry.get("no_ask")
    yes_ask_size = entry.get("yes_ask_size", 0.0)
    no_ask_size  = entry.get("no_ask_size", 0.0)

    if yes_ask is None or no_ask is None:
        return None  # no live book on one side

    ask_prices = [yes_ask, no_ask]
    ask_sizes  = [yes_ask_size, no_ask_size]
    outcomes   = ["Yes", "No"]

    if any(p <= MIN_LEG_PRICE for p in ask_prices):
        return None

    # Guard: if this market is one of several buckets in a multi-outcome event
    # (e.g. temperature ranges, score bands), the NO side bundles all other
    # outcomes — YES+NO < 1.0 is structural, not a real binary arb.
    event_ticker = market.get("event_ticker", "")
    if event_ticker:
        total = _event_total_count(event_ticker)
        if total > 1:
            return None

    sum_asks = sum(ask_prices)
    if sum_asks >= 1.0:
        return None

    gross_profit = 1.0 - sum_asks
    total_fees_val = _total_fees(ask_prices)
    net_profit   = gross_profit - total_fees_val
    has_zero_size = any(s <= 0 for s in ask_sizes)

    if net_profit >= MIN_NET_PROFIT and all(s >= MIN_LEG_SIZE for s in ask_sizes):
        category = "opportunity"
    elif gross_profit > 0 and NEAR_MISS_LOWER <= net_profit < 0:
        category = "near_miss"
    else:
        return None

    close_time = market.get("close_time", "") if category == "opportunity" else None

    return {
        "ticker":         ticker,
        "event_ticker":   market.get("event_ticker", ""),
        "title":          market.get("title", ticker),
        "outcomes":       outcomes,
        "ask_prices":     ask_prices,
        "ask_sizes":      ask_sizes,
        "sum_asks":       sum_asks,
        "gross_profit":   gross_profit,
        "total_fees":     total_fees_val,
        "net_profit":     net_profit,
        "taker_fee_coeff": TAKER_FEE_COEFF,
        "source":         source,
        "category":       category,
        "has_zero_size":  has_zero_size,
        "close_time":     close_time,
        "volume_24h":     market.get("volume_24h", 0.0),
        "event_slug":     _slugify(market.get("subtitle", "") or market.get("event_ticker", "") or ticker),
    }


# ── Cumulative/nested market detection patterns ───────────────────────────────
# -DDMMM  e.g. -26APR, -26MAY, -01JAN (optionally followed by 2-digit year)
_PAT_DDMMM   = re.compile(r"-\d{1,2}[A-Z]{3}(?:\d{2})?(?:H\d{2,4})?$")
# -MMMYY  e.g. -MAR26, -APR26
_PAT_MMMYY   = re.compile(r"-[A-Z]{3}\d{2}$")
# -QNYYYY e.g. -Q12026, -Q22026  (AMBIGUOUS: "by Q1" vs "in Q1")
_PAT_QTRLY   = re.compile(r"-Q[1-4]\d{4}$")


def _cumulative_deadline_reason(markets: list) -> Optional[str]:
    """
    Return a descriptive reason string if this group is cumulative/nested
    (NOT mutually exclusive), or None if it looks like a valid exhaustive set.

    Detection rules (any one fires → cumulative):
      1. OVER/ABOVE anywhere in 2+ tickers → threshold market ("price above $X")
      2. -DDMMM or -MMMYY date suffixes on 2+ tickers → deadline variants
         ("deal by April", "deal by May" …)
      3. 2+ market titles contain "before" followed by a date/year → deadline
         variants expressed in the title ("leave office before 2027-01-01 …")

    Quarterly (-QNYYYY) patterns are AMBIGUOUS and handled separately —
    they are NOT auto-skipped here.

    The check is COUNT >= 2, not ALL.  A group like the Iranian nuclear deal
    (4 deadline tickers + 1 year-only base ticker) still fires correctly.
    """
    tickers = [m["ticker"] for m in markets]

    # Rule 1: OVER / ABOVE threshold markets
    threshold = [t for t in tickers if "OVER" in t or "ABOVE" in t]
    if len(threshold) >= 2:
        return f"threshold(OVER/ABOVE) matched={threshold}"

    # Rule 2: date-deadline suffixes (-DDMMM or -MMMYY)
    dated = [t for t in tickers
             if _PAT_DDMMM.search(t) or _PAT_MMMYY.search(t)]
    if len(dated) >= 2:
        return f"deadline-dated matched={dated}"

    # Rule 3: titles containing "before <date>" — e.g. "leave office before 2027-01-01"
    _before_date = re.compile(r"\bbefore\b.{1,6}20\d{2}", re.IGNORECASE)
    titled = [m for m in markets if _before_date.search(m.get("title", ""))]
    if len(titled) >= 2:
        return f"deadline-title(before date) matched={[m['ticker'] for m in titled]}"

    # Rule 4: titles containing "above/over <number>" — numeric threshold markets
    # e.g. "Will above 100,000 jobs…" and "Will above 10,000 jobs…" are cumulative
    # (satisfying the higher threshold implies satisfying all lower ones).
    _above_number = re.compile(r"\b(above|over)\b[\s,]*\d", re.IGNORECASE)
    above_titled = [m for m in markets if _above_number.search(m.get("title", ""))]
    if len(above_titled) >= 2:
        return f"threshold-title(above/over number) matched={[m['ticker'] for m in above_titled]}"

    # Rule 5: time-snapshot markets (H0900, H1500, etc.) — e.g. NASDAQ hourly snapshots.
    # Different time slots are independent, not mutually exclusive outcomes.
    _PAT_HTIME = re.compile(r"H\d{3,4}$", re.IGNORECASE)
    time_snaps = [t for t in tickers if _PAT_HTIME.search(t)]
    if len(time_snaps) >= 2:
        return f"time-snapshot(H####) matched={time_snaps}"

    return None


def _quarterly_ambiguous_reason(markets: list) -> Optional[str]:
    """
    Return a warning string if this group has ambiguous quarterly suffixes
    (-Q12026, -Q22026 …).  Quarterly markets could be either cumulative
    ('by Q1') or exhaustive ('in Q1') so we log but do NOT auto-skip.
    """
    tickers = [m["ticker"] for m in markets]
    quarterly = [t for t in tickers if _PAT_QTRLY.search(t)]
    if len(quarterly) >= 2:
        return f"quarterly(ambiguous, not skipped) matched={quarterly}"
    return None


def _is_cumulative_deadline_group(markets: list) -> bool:
    return _cumulative_deadline_reason(markets) is not None


def _multi_outcome_result(
    markets: list,
    outcomes: list,
    ask_prices: list,
    ask_sizes: list,
    sum_asks: float,
    category: str,
    source: str,
) -> dict:
    """Build a result dict for a multi-outcome event (any category)."""
    event_ticker = markets[0].get("event_ticker") or markets[0]["ticker"]
    n = len(markets)
    # Read cached total only — never trigger an API call here (called on every scan)
    cached_entry = _event_size_cache.get(event_ticker)
    total = cached_entry[0] if cached_entry else 0
    way_label = f"[{total}-way]" if (total == n or total == 0) else f"[{n}/{total}-way]"
    gross_profit   = max(0.0, 1.0 - sum_asks)
    total_fees_val = _total_fees(ask_prices)
    net_profit     = gross_profit - total_fees_val if category not in ("cumulative", "non_exhaustive", "spread_market") else 0.0
    return {
        "ticker":           event_ticker,
        "event_ticker":     event_ticker,
        "title":            f"{way_label} {markets[0].get('title', event_ticker)}",
        "outcomes":         outcomes,
        "outcome_tickers":  [m["ticker"] for m in markets],
        "ask_prices":      ask_prices,
        "ask_sizes":       ask_sizes,
        "sum_asks":        sum_asks,
        "gross_profit":    gross_profit,
        "total_fees":      total_fees_val,
        "net_profit":      net_profit,
        "taker_fee_coeff": TAKER_FEE_COEFF,
        "source":          source,
        "category":        category,
        "has_zero_size":   any(s <= 0 for s in ask_sizes),
        "close_time":      markets[0].get("close_time", "") if category == "opportunity" else None,
        "volume_24h":      min(m.get("volume_24h", 0.0) for m in markets),
        "event_slug":      _slugify(markets[0].get("subtitle", "") or markets[0].get("event_ticker", "") or markets[0]["ticker"]),
    }


def compute_multi_outcome_opportunity(markets: list, source: str = "REST") -> Optional[dict]:
    """
    Evaluate a multi-outcome categorical event for arb.

    Returns a result dict for ALL detected cases (opportunity, near_miss,
    cumulative, non_exhaustive) so callers can log them with the right badge.
    Returns None only when there is no price data or the market has resolved.
    """
    for market in markets:
        mins = _minutes_to_close(market.get("close_time"))
        if mins is not None and mins < -10:
            return None  # resolved — nothing to log

    # Collect prices first so every logged result has real data
    ask_prices: list[float] = []
    ask_sizes:  list[float] = []
    outcomes:   list[str]   = []

    with prices_lock:
        for market in markets:
            t = market["ticker"]
            entry = live_prices.get(t)
            if entry is None:
                return None  # no price data yet — skip silently
            yes_ask = entry.get("yes_ask")
            if yes_ask is None:
                return None
            ask_prices.append(yes_ask)
            ask_sizes.append(entry.get("yes_ask_size", 0.0))
            outcomes.append(market.get("title", t)[:60])

    # Deduplicate outcome labels when multiple markets share the same title
    # (e.g. game markets where both legs say "UMBC at Ohio Winner?")
    if len(set(outcomes)) < len(outcomes):
        outcomes = [
            f"{market.get('title', market['ticker'])[:45]} ({market['ticker'].split('-')[-1]})"
            for market in markets
        ]

    if any(p <= MIN_LEG_PRICE for p in ask_prices):
        return None

    sum_asks = sum(ask_prices)

    # ── Guard 1: cumulative / nested deadline ────────────────────────────────
    # Multiple outcomes can resolve YES (e.g. "deal by Apr" + "deal by May").
    # Only log when sum < 1.0 — otherwise there's no apparent opportunity.
    cumulative_reason = _cumulative_deadline_reason(markets)
    if cumulative_reason:
        if sum_asks < 1.0:
            event_key = markets[0].get("event_ticker") or markets[0]["ticker"]
            logger.debug("Logged [cumulative]  event=%s  sum=%.4f  %s",
                         event_key, sum_asks, cumulative_reason)
            return _multi_outcome_result(markets, outcomes, ask_prices, ask_sizes,
                                         sum_asks, "cumulative", source)
        return None

    # ── Guard 2: spread markets ───────────────────────────────────────────────
    # SPREAD events (e.g. KXNHLSPREAD, KXNBAASPREAD) mix nested over/under
    # thresholds with team outcomes.  They are neither exhaustive nor mutually
    # exclusive: a 1-goal win resolves all four YES-asks to NO, and a blowout
    # can resolve two of them to YES.  Log as spread_market and skip arb check.
    event_key = markets[0].get("event_ticker") or markets[0]["ticker"]
    if "SPREAD" in event_key.upper() and sum_asks < 1.0:
        logger.debug("Logged [spread_market]  event=%s  sum=%.4f", event_key, sum_asks)
        return _multi_outcome_result(markets, outcomes, ask_prices, ask_sizes,
                                     sum_asks, "spread_market", source)

    # Warn on ambiguous quarterly patterns but still scan
    qtrly_reason = _quarterly_ambiguous_reason(markets)
    if qtrly_reason:
        logger.debug("Quarterly-ambiguous  event=%s  %s",
                     markets[0].get("event_ticker") or markets[0]["ticker"], qtrly_reason)

    if sum_asks >= 1.0:
        return None  # no underround — nothing to log

    n = len(markets)
    event_ticker = markets[0].get("event_ticker") or markets[0]["ticker"]

    # ── Guard 2: completeness check ──────────────────────────────────────────
    # Check FIRST whether we're missing outcomes due to the volume filter.
    # e.g. a 3-outcome soccer game (Win/Lose/Tie) where the Tie market has
    # low volume and was filtered — the 2-outcome sum looks like an arb but isn't.
    # Return None silently: this is a data-gap, not a structural non-exhaustive event.
    if not _event_is_complete(event_ticker, n):
        logger.debug("Skipping incomplete event=%s  tracked=%d  (missing outcomes)",
                     event_ticker, n)
        return None

    # ── Guard 3: non-exhaustive set ──────────────────────────────────────────
    # Only fires when we DO have all listed outcomes but the set doesn't cover
    # all possible outcomes (open-ended rankings, correlated threshold markets).
    # Small N (≤4): correlated threshold markets ("Over 135pts", "Over 156pts")
    # Large N (≥5): open-ended rankings where an unlisted outcome can win
    if (n <= 4 and sum_asks < 0.70) or (n >= 5 and sum_asks < MIN_MULTI_OUTCOME_SUM):
        logger.debug("Logged [non_exhaustive]  event=%s  n=%d  sum=%.4f",
                     event_ticker, n, sum_asks)
        return _multi_outcome_result(markets, outcomes, ask_prices, ask_sizes,
                                     sum_asks, "non_exhaustive", source)

    # ── Real arb / near-miss check ───────────────────────────────────────────
    gross_profit   = 1.0 - sum_asks
    total_fees_val = _total_fees(ask_prices)
    net_profit     = gross_profit - total_fees_val
    has_zero_size  = any(s <= 0 for s in ask_sizes)

    if net_profit >= MIN_NET_PROFIT and all(s >= MIN_LEG_SIZE for s in ask_sizes):
        category = "opportunity"
    elif gross_profit > 0 and NEAR_MISS_LOWER <= net_profit < 0:
        category = "near_miss"
    else:
        return None

    return _multi_outcome_result(markets, outcomes, ask_prices, ask_sizes,
                                 sum_asks, category, source)


def scan_all_markets(source: str = "REST") -> list[dict]:
    """Check every tracked market/event for arb and return all results."""
    with _event_groups_lock:
        groups = dict(_event_groups)
    opps = []
    for event_markets in groups.values():
        if len(event_markets) == 1:
            opp = compute_opportunity(event_markets[0], source=source)
        else:
            opp = compute_multi_outcome_opportunity(event_markets, source=source)
        if opp:
            opps.append(opp)
    return opps



# ---------------------------------------------------------------------------
# Alert helpers
# ---------------------------------------------------------------------------

def _fire_rest_alert(result: dict, row_id: int) -> None:
    if result["category"] == "opportunity":
        if result["has_zero_size"]:
            logger.debug("Opportunity #%d  net=%.3f%%  [zero-size, suppressed]",
                         row_id, result["net_profit"] * 100)
            return
        mins = _minutes_to_close(result.get("close_time"))
        if mins is not None and mins < EXPIRING_SOON_MINS:
            if (5 <= mins < EXPIRING_SOON_MINS
                    and result["net_profit"] >= EXPIRING_ACTIONABLE_MIN_PROFIT
                    and all(s >= MIN_LEG_SIZE for s in result["ask_sizes"])):
                alert_expiring_actionable(result, mins)
                logger.info("EXPIRING_ACTIONABLE #%d  %.0f min  net=%.3f%%",
                            row_id, mins, result["net_profit"] * 100)
            else:
                logger.info("Expiring [suppressed] #%d  %.0f min  net=%.3f%%",
                            row_id, mins, result["net_profit"] * 100)
        else:
            alert(result)
            logger.info("Opportunity #%d  net=%.3f%%", row_id, result["net_profit"] * 100)
    else:
        alert_near_miss(result)
        logger.info("Near-miss  #%d  gross=%.3f%%  net=%.3f%%  (fees ate the spread)",
                    row_id, result["gross_profit"] * 100, result["net_profit"] * 100)


def _rest_handle_result(result: dict) -> None:
    """
    Deduplicate REST scan results.

    DB (rest_state): first detection → INSERT; same prices → skip;
                     prices changed → UPDATE in-place.
    Alert (alert_cooldown): shared with WS; suppressed within cooldown window
                            unless profit improved by WS_MIN_PROFIT_IMPROVEMENT.
    """
    key = result["ticker"]
    now = datetime.now(timezone.utc)

    with rest_state_lock:
        state = rest_state.get(key)

    if state is not None and abs(result["sum_asks"] - state["sum_asks"]) < 0.0001:
        return  # price unchanged

    # Filtered categories: log once on first detection, no alert, no updates
    if result["category"] in ("cumulative", "non_exhaustive"):
        if state is None:
            row_id = save_opportunity(result)
            with rest_state_lock:
                rest_state[key] = {"row_id": row_id, "sum_asks": result["sum_asks"]}
        return

    if state is None:
        row_id = save_opportunity(result)
        with rest_state_lock:
            rest_state[key] = {"row_id": row_id, "sum_asks": result["sum_asks"]}
    else:
        update_opportunity(state["row_id"], result)
        row_id = state["row_id"]
        with rest_state_lock:
            rest_state[key]["sum_asks"] = result["sum_asks"]

    # WS priority: promote opportunity tickers for real-time tracking
    if result["category"] == "opportunity":
        _prioritize_opportunity_tickers([result["ticker"]])
        if row_id:
            _trade_priority_ids.append(row_id)
        _trade_trigger.set()  # wake trading loop immediately

    with alert_cooldown_lock:
        rec = alert_cooldown.get(key)
        if rec is not None:
            secs_elapsed       = (now - rec["last_alert_at"]).total_seconds()
            profit_improvement = result["net_profit"] - rec["last_net_profit"]
            if secs_elapsed < WS_ALERT_COOLDOWN_SECS and profit_improvement < WS_MIN_PROFIT_IMPROVEMENT:
                return
        alert_cooldown[key] = {"last_alert_at": now, "last_net_profit": result["net_profit"]}

    _fire_rest_alert(result, row_id)


# ---------------------------------------------------------------------------
# REST polling loop
# ---------------------------------------------------------------------------

def rest_poll_loop(stop_event: threading.Event) -> None:
    """
    Continuously refresh market prices via REST and scan for opportunities.

    Option C — rolling chunk with WS-skip:
      Each cycle takes a rolling slice of MAX_REST_MARKETS cold markets
      (those not recently updated by WS) and batch-refreshes their prices
      via GET /markets?tickers=...
    """
    global _rest_chunk_start
    logger.info("REST polling loop started (interval=%.0fs, freshness=%ds)",
                REST_POLL_INTERVAL, WS_FRESHNESS_SECS)

    while not stop_event.is_set():
        now_dt = datetime.now(timezone.utc)
        all_markets = list(markets_by_ticker.values())
        n_total = len(all_markets)

        # ── Identify cold markets ────────────────────────────────────────────
        with ws_activity_lock:
            fresh = {
                t for t, last in ws_activity.items()
                if (now_dt - last).total_seconds() <= WS_FRESHNESS_SECS
            }
        cold_markets = [m for m in all_markets if m["ticker"] not in fresh]
        total_cold   = len(cold_markets)
        n_ws_fresh   = n_total - total_cold

        # ── Rolling chunk ────────────────────────────────────────────────────
        if total_cold > 0:
            chunk_idx  = _rest_chunk_start % total_cold
            end        = chunk_idx + MAX_REST_MARKETS
            if end <= total_cold:
                poll_markets = cold_markets[chunk_idx:end]
            else:
                poll_markets = cold_markets[chunk_idx:] + cold_markets[:end - total_cold]
            _rest_chunk_start = end % total_cold
            chunk_num    = chunk_idx // MAX_REST_MARKETS + 1
            total_chunks = max(1, math.ceil(total_cold / MAX_REST_MARKETS))
        else:
            poll_markets  = []
            _rest_chunk_start = 0
            chunk_num    = 0
            total_chunks = 0

        tickers = [m["ticker"] for m in poll_markets]
        if not tickers:
            stop_event.wait(REST_POLL_INTERVAL)
            continue

        try:
            prices = fetch_market_prices(tickers)
        except Exception as exc:
            logger.error("REST price fetch error: %s", exc)
            stop_event.wait(REST_POLL_INTERVAL)
            continue

        with prices_lock:
            for ticker, p in prices.items():
                live_prices[ticker] = p

        # ── WS coverage metrics (last 60 s) ──────────────────────────────────
        cutoff = now_dt.timestamp() - 60.0
        with ws_activity_lock:
            while ws_event_log and ws_event_log[0][0].timestamp() < cutoff:
                ws_event_log.popleft()
            ws_events_60s = len(ws_event_log)
            ws_unique_60s = len({t for _, t in ws_event_log})

        # ── Scan summary ─────────────────────────────────────────────────────
        n_polled     = len(poll_markets)
        n_resolved   = 0
        n_full_book  = 0
        n_no_book    = 0
        n_underround = 0
        underround_markets: list[tuple[str, float]] = []
        resolved_to_evict: list[str] = []

        with prices_lock:
            for market in markets_by_ticker.values():
                mins_left = _minutes_to_close(market.get("close_time"))
                if mins_left is not None and mins_left < -10:
                    n_resolved += 1
                    resolved_to_evict.append(market["ticker"])
                    continue
                entry = live_prices.get(market["ticker"])
                if not entry:
                    continue
                if entry.get("yes_ask") is None or entry.get("no_ask") is None:
                    n_no_book += 1
                    continue
                n_full_book += 1
                s = entry["yes_ask"] + entry["no_ask"]
                if s < 1.0:
                    n_underround += 1
                    fees = _total_fees([entry["yes_ask"], entry["no_ask"]])
                    net  = 1.0 - s - fees
                    underround_markets.append((market["title"], net))

        # Evict resolved markets
        for t in resolved_to_evict:
            markets_by_ticker.pop(t, None)
            with alert_cooldown_lock:
                alert_cooldown.pop(t, None)
            with rest_state_lock:
                rest_state.pop(t, None)
            with ws_activity_lock:
                ws_activity.pop(t, None)
            with prices_lock:
                live_prices.pop(t, None)
            with priority_lock:
                priority_tickers.discard(t)

        if underround_markets:
            sub_detail = "  ".join(
                f"[{q[:50]} {net * 100:+.2f}%]" for q, net in underround_markets
            )
            sub_label = f"{n_underround} sub-100%: {sub_detail}"
        else:
            sub_label = "0 sub-100%"

        logger.info(
            "REST scan: %d total | %d polled (%d WS-fresh skipped) | "
            "%d resolved/stale (evicted) | %d full books | %d no-book | %s  |  "
            "WS events last 60s: %d  |  unique markets: %d  |  "
            "REST chunk: %d/%d",
            n_total, n_polled, n_ws_fresh,
            n_resolved, n_full_book, n_no_book, sub_label,
            ws_events_60s, ws_unique_60s,
            chunk_num, total_chunks,
        )

        for result in scan_all_markets(source="REST"):
            _rest_handle_result(result)

        stop_event.wait(REST_POLL_INTERVAL)

    logger.info("REST polling loop stopped")


# ---------------------------------------------------------------------------
# WebSocket event loop
# ---------------------------------------------------------------------------

def ws_event_loop(event_queue: queue.Queue, stop_event: threading.Event) -> None:
    """
    Consume Kalshi ticker events from the WS queue, update live_prices,
    and immediately scan affected markets for arb opportunities.
    """
    logger.info("WebSocket event loop started")
    while not stop_event.is_set():
        try:
            event = event_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        msg_type = event.get("type", "")

        if msg_type == "ws_connected":
            # WS (re)connected — ensure authorized opps are watched and trigger trader
            from db import get_authorized_opportunities
            auth_opps = get_authorized_opportunities()
            if auth_opps:
                tickers = [o["ticker"] for o in auth_opps if o.get("ticker")]
                if tickers:
                    _prioritize_opportunity_tickers(tickers)
                if TRADING_ENABLED:
                    for o in auth_opps:
                        _trade_priority_ids.append(o["id"])
                    _trade_trigger.set()
                    logger.info("WS (re)connected: triggering trader for %d authorized opp(s)", len(auth_opps))
            continue

        if msg_type == "ticker":
            msg = event.get("msg", {})
            ticker = msg.get("market_ticker", "")
            if not ticker:
                continue

            yes_ask = _parse_ws_price(msg.get("yes_ask_dollars") or msg.get("yes_ask"))
            no_ask  = _parse_ws_price(msg.get("no_ask_dollars")  or msg.get("no_ask"))

            if yes_ask is None and no_ask is None:
                continue  # no price info in this tick

            with prices_lock:
                entry = live_prices.get(ticker, {})
                # Only update fields that are present in this message
                if yes_ask is not None:
                    entry["yes_ask"]      = yes_ask
                    entry["yes_ask_size"] = float(msg.get("yes_ask_size_fp") or msg.get("yes_ask_size") or entry.get("yes_ask_size", 0))
                if no_ask is not None:
                    entry["no_ask"]      = no_ask
                    entry["no_ask_size"] = float(msg.get("no_ask_size_fp") or msg.get("no_ask_size") or entry.get("no_ask_size", 0))
                live_prices[ticker] = entry

            _check_ticker_market(ticker, source="WS")

    logger.info("WebSocket event loop stopped")


def _parse_ws_price(val) -> Optional[float]:
    try:
        f = float(val)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def _minutes_to_close(close_time: Optional[str]) -> Optional[float]:
    if not close_time:
        return None
    try:
        end_dt = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        return (end_dt - datetime.now(timezone.utc)).total_seconds() / 60
    except (ValueError, AttributeError):
        return None


def _ws_should_alert(key: str, net_profit: float) -> bool:
    now = datetime.now(timezone.utc)
    with alert_cooldown_lock:
        rec = alert_cooldown.get(key)
        if rec is not None:
            secs_elapsed       = (now - rec["last_alert_at"]).total_seconds()
            profit_improvement = net_profit - rec["last_net_profit"]
            if secs_elapsed < WS_ALERT_COOLDOWN_SECS and profit_improvement < WS_MIN_PROFIT_IMPROVEMENT:
                return False
        alert_cooldown[key] = {"last_alert_at": now, "last_net_profit": net_profit}
    return True


def _check_ticker_market(ticker: str, source: str) -> None:
    """
    Find the event group containing this ticker and scan it for arb.
    Records WS activity for REST-skip logic.
    """
    # Use cached grouping for O(1) ticker lookup instead of O(n) re-grouping
    with _event_groups_lock:
        event_key = _ticker_to_event_key.get(ticker)
        if event_key is None:
            return  # ticker not in current market universe
        event_markets = _event_groups.get(event_key, [])

    _now = datetime.now(timezone.utc)
    with ws_activity_lock:
        ws_activity[ticker] = _now
        ws_event_log.append((_now, ticker))

    if len(event_markets) == 1:
        result = compute_opportunity(event_markets[0], source=source)
    else:
        result = compute_multi_outcome_opportunity(event_markets, source=source)

    if not result:
        return

    k = result["ticker"]

    if result["category"] == "opportunity":
        if result["has_zero_size"]:
            save_opportunity(result)
            logger.debug("WS opportunity [zero-size suppressed]  ticker=%s  net=%.3f%%",
                         k, result["net_profit"] * 100)
            return

        if not _ws_should_alert(k, result["net_profit"]):
            logger.debug("WS cooldown hit  ticker=%s  net=%.3f%%", k, result["net_profit"] * 100)
            return

        row_id = save_opportunity(result)
        if row_id:
            _trade_priority_ids.append(row_id)
        _trade_trigger.set()  # wake trading loop immediately
        mins = _minutes_to_close(result.get("close_time"))
        if mins is not None and mins < EXPIRING_SOON_MINS:
            if (5 <= mins < EXPIRING_SOON_MINS
                    and result["net_profit"] >= EXPIRING_ACTIONABLE_MIN_PROFIT
                    and all(s >= MIN_LEG_SIZE for s in result["ask_sizes"])):
                alert_expiring_actionable(result, mins)
                logger.info("WS EXPIRING_ACTIONABLE #%d  %.0f min  net=%.3f%%",
                            row_id, mins, result["net_profit"] * 100)
            else:
                logger.info("WS expiring [suppressed] #%d  %.0f min  net=%.3f%%",
                            row_id, mins, result["net_profit"] * 100)
        else:
            alert(result)
            logger.info("WS opportunity #%d  net=%.3f%%", row_id, result["net_profit"] * 100)

    else:  # near_miss
        if not _ws_should_alert(k, result["net_profit"]):
            logger.debug("WS cooldown hit (near-miss)  ticker=%s", k)
            return
        row_id = save_opportunity(result)
        alert_near_miss(result)
        logger.info("WS near-miss  #%d  net=%.3f%%", row_id, result["net_profit"] * 100)


# ---------------------------------------------------------------------------
# Market refresh loop
# ---------------------------------------------------------------------------

def _merge_new_markets(discovered: list[dict]) -> int:
    """
    Add newly discovered markets to markets_by_ticker and live_prices.
    Skips tickers already tracked.  Returns count of genuinely new tickers.
    """
    added = 0
    for m in discovered:
        t = m["ticker"]
        if t in markets_by_ticker:
            continue
        if MIN_VOLUME_24H > 0 and float(m.get("volume_24h", 0) or 0) < MIN_VOLUME_24H:
            continue
        markets_by_ticker[t] = m
        with prices_lock:
            if t not in live_prices:
                live_prices[t] = {
                    "yes_ask":      m.get("seed_yes_ask"),
                    "yes_ask_size": m.get("seed_yes_ask_size", 0.0),
                    "no_ask":       m.get("seed_no_ask"),
                    "no_ask_size":  m.get("seed_no_ask_size", 0.0),
                }
        added += 1
    return added


def market_refresh_loop(ws_client: Optional[KalshiWSClient], stop_event: threading.Event) -> None:
    """
    Periodically refresh the market list using three tiers:

    Fast refresh (every MARKET_REFRESH_INTERVAL, default 5 min):
      Re-fetch only known tickers via GET /markets?tickers=...
      Updates prices and detects closed/settled markets.

    Near-term scan (every NEAR_TERM_SCAN_INTERVAL, default 15 min):
      Fetch markets closing within NEAR_TERM_HORIZON_HOURS (default 12h).
      Catches new sports events and near-term markets without a full scan.

    Full rescan (every 6 hours):
      Re-run fetch_active_markets() to discover all newly opened markets.
    """
    logger.info(
        "Market refresh loop started (fast=%.0fs  near-term=%.0fs  full=6h)",
        MARKET_REFRESH_INTERVAL, NEAR_TERM_SCAN_INTERVAL,
    )
    FULL_RESCAN_INTERVAL = 6 * 3600
    last_full_rescan   = time.time()
    last_near_term_scan = time.time() - NEAR_TERM_SCAN_INTERVAL  # run near-term on first tick

    while not stop_event.is_set():
        try:
            now_ts = time.time()
            do_full      = (now_ts - last_full_rescan)    >= FULL_RESCAN_INTERVAL
            do_near_term = (now_ts - last_near_term_scan) >= NEAR_TERM_SCAN_INTERVAL

            if do_full:
                logger.info("Running full market rescan…")
                markets = fetch_active_markets()
                _prewarm_event_size_cache(markets)  # prewarm before any cap
                # Apply MAX_MARKETS cap (highest volume first) for scanning only
                if MAX_MARKETS:
                    markets.sort(key=lambda m: m["volume_24h"], reverse=True)
                    markets = markets[:MAX_MARKETS]
                new_tickers: dict[str, dict] = {
                    m["ticker"]: m for m in markets
                    if MIN_VOLUME_24H <= 0 or float(m.get("volume_24h", 0) or 0) >= MIN_VOLUME_24H
                }
                last_full_rescan    = now_ts
                last_near_term_scan = now_ts  # full scan subsumes near-term
                save_markets_cache(new_tickers)

                # Atomic swap: update existing + remove evicted (closed/settled)
                markets_by_ticker.update(new_tickers)
                for t in list(markets_by_ticker):
                    if t not in new_tickers:
                        markets_by_ticker.pop(t, None)
                        with alert_cooldown_lock:
                            alert_cooldown.pop(t, None)
                        with rest_state_lock:
                            rest_state.pop(t, None)
                        with ws_activity_lock:
                            ws_activity.pop(t, None)
                        with prices_lock:
                            live_prices.pop(t, None)
                        with priority_lock:
                            priority_tickers.discard(t)

                # Seed live_prices for newly discovered markets
                with prices_lock:
                    for m in new_tickers.values():
                        t = m["ticker"]
                        if t not in live_prices:
                            live_prices[t] = {
                                "yes_ask":      m.get("seed_yes_ask"),
                                "yes_ask_size": m.get("seed_yes_ask_size", 0.0),
                                "no_ask":       m.get("seed_no_ask"),
                                "no_ask_size":  m.get("seed_no_ask_size", 0.0),
                            }

            elif do_near_term:
                discovered = fetch_near_term_markets(NEAR_TERM_HORIZON_HOURS)
                added = _merge_new_markets(discovered)
                last_near_term_scan = now_ts
                if added:
                    logger.info("Near-term scan added %d new markets (total=%d)",
                                added, len(markets_by_ticker))

                # Fast-path price refresh for all known tickers runs below
                known = list(markets_by_ticker.keys())
                if known:
                    refreshed = fetch_market_prices(known)
                    for ticker, prices in refreshed.items():
                        if ticker in markets_by_ticker:
                            markets_by_ticker[ticker].update(prices)
                    # Evict tickers that didn't come back
                    for ticker in known:
                        if ticker not in refreshed:
                            markets_by_ticker.pop(ticker, None)
                            with alert_cooldown_lock:
                                alert_cooldown.pop(ticker, None)
                            with rest_state_lock:
                                rest_state.pop(ticker, None)
                            with ws_activity_lock:
                                ws_activity.pop(ticker, None)
                            with prices_lock:
                                live_prices.pop(ticker, None)
                            with priority_lock:
                                priority_tickers.discard(ticker)

            else:
                # Fast path: re-fetch only known tickers to update prices + detect closures
                known = list(markets_by_ticker.keys())
                if not known:
                    stop_event.wait(MARKET_REFRESH_INTERVAL)
                    continue
                refreshed = fetch_market_prices(known)
                for ticker, prices in refreshed.items():
                    if ticker in markets_by_ticker:
                        markets_by_ticker[ticker].update(prices)
                for ticker in known:
                    if ticker not in refreshed:
                        markets_by_ticker.pop(ticker, None)
                        with alert_cooldown_lock:
                            alert_cooldown.pop(ticker, None)
                        with rest_state_lock:
                            rest_state.pop(ticker, None)
                        with ws_activity_lock:
                            ws_activity.pop(ticker, None)
                        with prices_lock:
                            live_prices.pop(ticker, None)
                        with priority_lock:
                            priority_tickers.discard(ticker)

            if ws_client:
                all_t = list(markets_by_ticker.keys())
                all_t_set = set(all_t)
                with priority_lock:
                    pri = [t for t in priority_tickers if t in all_t_set]
                remaining = [t for t in all_t if t not in priority_tickers]
                ws_client.update_subscriptions(pri + remaining)

            logger.info("Market list updated: %d markets tracked", len(markets_by_ticker))
            _rebuild_event_groups()
            _log_market_composition()

        except Exception as exc:
            logger.error("Market refresh error: %s", exc)

        stop_event.wait(MARKET_REFRESH_INTERVAL)

    logger.info("Market refresh loop stopped")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Kalshi Arbitrage Bot")
    parser.add_argument("--no-ws", action="store_true", help="Disable WebSocket, use REST-only")
    parser.add_argument(
        "--debug-mode",
        action="store_true",
        help="Log ALL sub-100%% books (sets min_net_profit=-5%% and min_leg_size=0).",
    )
    args = parser.parse_args()

    if args.debug_mode:
        global MIN_NET_PROFIT, MIN_LEG_SIZE
        MIN_NET_PROFIT = -0.05
        MIN_LEG_SIZE   = 0.0
        logger.warning(
            "DEBUG MODE active — min_net_profit=%.0f%%  min_leg_size=%.1f",
            MIN_NET_PROFIT * 100, MIN_LEG_SIZE,
        )

    logger.info("Initialising database …")
    init_db()
    logger.info("Total opportunities logged so far: %d", opportunity_count())

    stop_event = threading.Event()

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received, stopping …")
        stop_event.set()

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Kick off the initial market scan in a background thread so the bot
    # starts polling/listening immediately.  The REST poller handles an empty
    # markets_by_ticker gracefully (no-op until markets arrive).
    logger.info("Starting background initial market scan …")
    def _initial_load():
        # ── Phase 1: instant start from DB cache ────────────────────────────
        # Load full (unfiltered) cache first to prewarm event-size counts, then
        # apply the volume filter for active scanning. This ensures completeness
        # checks work correctly during the window before Phase 2 finishes.
        all_cached = load_markets_from_cache(min_volume_24h=0)
        if all_cached:
            _prewarm_event_size_cache(all_cached)
        cached = [m for m in all_cached if MIN_VOLUME_24H <= 0 or float(m.get("volume_24h", 0) or 0) >= MIN_VOLUME_24H] if all_cached else []
        if cached:
            for m in cached:
                markets_by_ticker[m["ticker"]] = m
                with prices_lock:
                    if m["ticker"] not in live_prices:
                        live_prices[m["ticker"]] = {
                            "yes_ask":      m.get("seed_yes_ask"),
                            "yes_ask_size": m.get("seed_yes_ask_size", 0.0),
                            "no_ask":       m.get("seed_no_ask"),
                            "no_ask_size":  m.get("seed_no_ask_size", 0.0),
                        }
            _rebuild_event_groups()
            logger.info(
                "Loaded %d markets from DB cache — bot scanning immediately. "
                "Live API scan running in background…",
                len(cached),
            )
            if _ws_client is not None:
                _ws_client.update_subscriptions(list(markets_by_ticker.keys()))
                logger.info("WS subscriptions primed from cache: %d tickers",
                            len(markets_by_ticker))
        else:
            logger.info("No DB cache found — waiting for full API scan…")

        # ── Phase 2: full API scan to refresh & update cache ────────────────
        try:
            markets = fetch_active_markets()
            _prewarm_event_size_cache(markets)  # prewarm before any cap
            if MAX_MARKETS:
                markets.sort(key=lambda m: m["volume_24h"], reverse=True)
                markets = markets[:MAX_MARKETS]
            for m in markets:
                markets_by_ticker[m["ticker"]] = m
                with prices_lock:
                    if m["ticker"] not in live_prices:
                        live_prices[m["ticker"]] = {
                            "yes_ask":      m.get("seed_yes_ask"),
                            "yes_ask_size": m.get("seed_yes_ask_size", 0.0),
                            "no_ask":       m.get("seed_no_ask"),
                            "no_ask_size":  m.get("seed_no_ask_size", 0.0),
                        }
            _rebuild_event_groups()
            save_markets_cache(markets_by_ticker)
            logger.info(
                "API scan complete: %d markets loaded, cache updated (%d rows)",
                len(markets_by_ticker), markets_cache_count(),
            )
            _log_market_composition()

            # Push fresh subscription list to WS now that we have markets.
            # (WS started with an empty list because markets load async.)
            if _ws_client is not None:
                all_t = list(markets_by_ticker.keys())
                with priority_lock:
                    pri = [t for t in priority_tickers if t in set(all_t)]
                remaining = [t for t in all_t if t not in priority_tickers]
                _ws_client.update_subscriptions(pri + remaining)
                logger.info("WS subscriptions updated: %d tickers", len(all_t))

            # Fire trader for any opportunities already authorized in the DB
            if TRADING_ENABLED:
                from db import get_authorized_opportunities
                auth_opps = get_authorized_opportunities()
                if auth_opps:
                    for o in auth_opps:
                        _trade_priority_ids.append(o["id"])
                    _trade_trigger.set()
                    logger.info("Initial load: %d authorized opp(s) found in DB — trader triggered", len(auth_opps))

        except Exception as exc:
            logger.error("Initial API market scan failed: %s", exc)

    threading.Thread(target=_initial_load, daemon=True, name="initial-load").start()

    ws_client: Optional[KalshiWSClient] = None
    event_queue: queue.Queue = queue.Queue()
    threads: list[threading.Thread] = []

    global _ws_client
    if not args.no_ws:
        all_tickers = list(markets_by_ticker.keys())
        ws_client  = KalshiWSClient(event_queue)
        _ws_client = ws_client
        ws_client.start(all_tickers)

        ws_loop = threading.Thread(
            target=ws_event_loop,
            args=(event_queue, stop_event),
            daemon=True,
            name="ws-event-loop",
        )
        ws_loop.start()
        threads.append(ws_loop)
    else:
        logger.info("WebSocket disabled — REST-only mode")

    rest_thread = threading.Thread(
        target=rest_poll_loop,
        args=(stop_event,),
        daemon=True,
        name="rest-poll",
    )
    rest_thread.start()
    threads.append(rest_thread)

    refresh_thread = threading.Thread(
        target=market_refresh_loop,
        args=(ws_client, stop_event),
        daemon=True,
        name="market-refresh",
    )
    refresh_thread.start()
    threads.append(refresh_thread)

    if TRADING_ENABLED:
        trade_thread = threading.Thread(
            target=trading_loop,
            args=(stop_event, _trade_trigger, _trade_priority_ids),
            daemon=True,
            name="trading-loop",
        )
        trade_thread.start()
        threads.append(trade_thread)
    else:
        logger.info("Trading disabled (TRADING_ENABLED=false) — set true in .env to enable")

    if SETTLE_ENABLED:
        settle_thread = threading.Thread(
            target=settle_loop,
            args=(stop_event,),
            daemon=True,
            name="settle-loop",
        )
        settle_thread.start()
        threads.append(settle_thread)

    logger.info(
        "Bot running. Press Ctrl+C to stop. "
        "WebSocket=%s  TakerFeeCoeff=%.4f  MinProfit=%.2f%%  Trading=%s",
        not args.no_ws,
        TAKER_FEE_COEFF,
        MIN_NET_PROFIT * 100,
        TRADING_ENABLED,
    )

    stop_event.wait()
    if ws_client:
        ws_client.stop()
    for t in threads:
        t.join(timeout=5)

    logger.info("Bot stopped. Total opportunities logged: %d", opportunity_count())


if __name__ == "__main__":
    main()

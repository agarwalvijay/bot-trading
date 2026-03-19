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
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

from alerter import alert, alert_near_miss, alert_expiring_actionable
from config import (
    EXPIRING_ACTIONABLE_MIN_PROFIT,
    EXPIRING_SOON_MINS,
    TAKER_FEE_COEFF,
    HIGH_PROFIT_THRESHOLD,
    MARKET_REFRESH_INTERVAL,
    MAX_REST_MARKETS,
    MIN_LEG_PRICE,
    MIN_LEG_SIZE,
    MIN_NET_PROFIT,
    NEAR_MISS_LOWER,
    REST_POLL_INTERVAL,
    WS_ALERT_COOLDOWN_SECS,
    WS_FRESHNESS_SECS,
    WS_MIN_PROFIT_IMPROVEMENT,
)
from db import init_db, opportunity_count, save_opportunity, update_opportunity
from fetcher import (
    KalshiWSClient,
    fetch_active_markets,
    fetch_market_prices,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("arb_bot")

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
    logger.info(
        "Market composition: %d true binary markets  |  "
        "%d multi-outcome events (%d total outcome markets)",
        n_binary, n_multi_events, n_multi_markets,
    )
    if multi:
        samples = sorted(multi.items(), key=lambda kv: len(kv[1]), reverse=True)[:5]
        for eid, ms in samples:
            logger.info(
                "  Multi-outcome event  eid=%s  n=%d  first_title=%s",
                eid, len(ms), ms[0].get("title", "?")[:70],
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

    if any(p < MIN_LEG_PRICE for p in ask_prices):
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
    }


def compute_multi_outcome_opportunity(markets: list, source: str = "REST") -> Optional[dict]:
    """
    Evaluate a multi-outcome categorical event for arb.

    Correct check: sum of YES asks across ALL sibling outcome markets < 1.0.
    Buying YES on every outcome guarantees exactly one $1 payout.
    """
    for market in markets:
        mins = _minutes_to_close(market.get("close_time"))
        if mins is not None and mins < -10:
            return None

    ask_prices: list[float] = []
    ask_sizes:  list[float] = []
    outcomes:   list[str]   = []
    tickers:    list[str]   = []

    with prices_lock:
        for market in markets:
            t = market["ticker"]
            entry = live_prices.get(t)
            if entry is None:
                return None
            yes_ask = entry.get("yes_ask")
            if yes_ask is None:
                return None
            ask_prices.append(yes_ask)
            ask_sizes.append(entry.get("yes_ask_size", 0.0))
            outcomes.append(market.get("title", t)[:60])
            tickers.append(t)

    if any(p < MIN_LEG_PRICE for p in ask_prices):
        return None

    sum_asks = sum(ask_prices)
    if sum_asks >= 1.0:
        return None

    # Sanity check: for truly mutually exclusive outcomes the sum of YES asks
    # must be reasonably close to 1.0.  Correlated threshold markets (e.g.
    # "Over 135pts", "Over 156pts", "Over 162pts") share an event_ticker but
    # are NOT exhaustive — their YES prices can sum to 0.35 or less.
    # We require sum > 0.7 to avoid these false positives.
    if sum_asks < 0.70:
        return None

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

    event_ticker = markets[0].get("event_ticker") or markets[0]["ticker"]
    title = f"[{len(markets)}-way] {markets[0]['title'][:60]}"
    close_time = markets[0].get("close_time", "") if category == "opportunity" else None

    return {
        "ticker":          event_ticker,
        "event_ticker":    event_ticker,
        "title":           title,
        "outcomes":        outcomes,
        "ask_prices":      ask_prices,
        "ask_sizes":       ask_sizes,
        "sum_asks":        sum_asks,
        "gross_profit":    gross_profit,
        "total_fees":      total_fees_val,
        "net_profit":      net_profit,
        "taker_fee_coeff": TAKER_FEE_COEFF,
        "source":          source,
        "category":        category,
        "has_zero_size":   has_zero_size,
        "close_time":      close_time,
    }


def scan_all_markets(source: str = "REST") -> list[dict]:
    """Check every tracked market/event for arb and return all results."""
    groups = _group_by_event(list(markets_by_ticker.values()))
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
# Phase 2 stub — order execution
# ---------------------------------------------------------------------------

def execute_arb(opp: dict[str, Any]) -> None:
    """
    PHASE 2 STUB
    ────────────
    Wire actual order placement here.

    Steps:
      1. Load KALSHI_API_KEY_ID + KALSHI_PRIVATE_KEY_PATH from config
      2. For each (ticker, ask_price, side) in opp:
             POST /portfolio/orders  {ticker, action="buy", side, count, price, type="limit"}
      3. Handle fills, rejections, position tracking
      4. Emit fill event back to alerter
    """
    logger.info(
        "[PHASE 2 STUB] Would execute arb on %s (net profit %.3f%%)",
        opp["ticker"],
        opp["net_profit"] * 100,
    )


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
    all_markets = list(markets_by_ticker.values())
    groups = _group_by_event(all_markets)

    affected_keys: set[str] = set()
    for key, event_markets in groups.items():
        for m in event_markets:
            if m["ticker"] == ticker:
                _now = datetime.now(timezone.utc)
                with ws_activity_lock:
                    ws_activity[ticker] = _now
                    ws_event_log.append((_now, ticker))
                affected_keys.add(key)
                break

    for key in affected_keys:
        event_markets = groups[key]
        if len(event_markets) == 1:
            result = compute_opportunity(event_markets[0], source=source)
        else:
            result = compute_multi_outcome_opportunity(event_markets, source=source)

        if not result:
            continue

        k = result["ticker"]

        if result["category"] == "opportunity":
            if result["has_zero_size"]:
                save_opportunity(result)
                logger.debug("WS opportunity [zero-size suppressed]  ticker=%s  net=%.3f%%",
                             k, result["net_profit"] * 100)
                continue

            if not _ws_should_alert(k, result["net_profit"]):
                logger.debug("WS cooldown hit  ticker=%s  net=%.3f%%", k, result["net_profit"] * 100)
                continue

            row_id = save_opportunity(result)
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
                continue
            row_id = save_opportunity(result)
            alert_near_miss(result)
            logger.info("WS near-miss  #%d  net=%.3f%%", row_id, result["net_profit"] * 100)


# ---------------------------------------------------------------------------
# Market refresh loop
# ---------------------------------------------------------------------------

def market_refresh_loop(ws_client: Optional[KalshiWSClient], stop_event: threading.Event) -> None:
    """
    Periodically refresh the market list.

    Because Kalshi has millions of markets and the full paginating scan takes
    several minutes, we use two strategies:

    Fast refresh (every MARKET_REFRESH_INTERVAL):
      Re-fetch only the tickers we already know via GET /markets?tickers=...
      This updates prices + detects closed markets quickly without re-paginating.

    Full rescan (every 6 hours):
      Re-run fetch_active_markets() to discover newly opened markets.
    """
    logger.info("Market refresh loop started (interval=%.0fs)", MARKET_REFRESH_INTERVAL)
    FULL_RESCAN_INTERVAL = 6 * 3600  # full re-paginate every 6 hours
    last_full_rescan = time.time()

    while not stop_event.is_set():
        try:
            now_ts = time.time()
            do_full = (now_ts - last_full_rescan) >= FULL_RESCAN_INTERVAL

            if do_full:
                logger.info("Running full market rescan…")
                markets = fetch_active_markets()
                new_tickers: dict[str, dict] = {m["ticker"]: m for m in markets}
                last_full_rescan = now_ts
            else:
                # Fast path: re-fetch only known tickers to update prices + detect closures
                known = list(markets_by_ticker.keys())
                if not known:
                    stop_event.wait(MARKET_REFRESH_INTERVAL)
                    continue
                refreshed = fetch_market_prices(known)
                new_tickers = {}
                for ticker, prices in refreshed.items():
                    m = markets_by_ticker.get(ticker, {}).copy()
                    m.update(prices)
                    new_tickers[ticker] = m
                # Drop any tickers that didn't come back (closed/settled)
                for ticker in known:
                    if ticker not in refreshed:
                        new_tickers.pop(ticker, None)

            # Atomic swap: add/update first, then remove evicted
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

            # Seed live_prices for newly discovered markets (full rescan only)
            if do_full:
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

            if ws_client:
                all_t = list(markets_by_ticker.keys())
                all_t_set = set(all_t)
                with priority_lock:
                    pri = [t for t in priority_tickers if t in all_t_set]
                remaining = [t for t in all_t if t not in priority_tickers]
                ws_client.update_subscriptions(pri + remaining)

            logger.info("Market list updated: %d markets tracked", len(markets_by_ticker))
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
        try:
            markets = fetch_active_markets()
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
            logger.info("Initial market scan complete: %d markets loaded", len(markets_by_ticker))
            _log_market_composition()
        except Exception as exc:
            logger.error("Initial market scan failed: %s", exc)

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

    logger.info(
        "Bot running. Press Ctrl+C to stop. "
        "WebSocket=%s  TakerFeeCoeff=%.4f  MinProfit=%.2f%%",
        not args.no_ws,
        TAKER_FEE_COEFF,
        MIN_NET_PROFIT * 100,
    )

    stop_event.wait()
    if ws_client:
        ws_client.stop()
    for t in threads:
        t.join(timeout=5)

    logger.info("Bot stopped. Total opportunities logged: %d", opportunity_count())


if __name__ == "__main__":
    main()

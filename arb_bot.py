"""
Polymarket Arbitrage Bot — Orchestrator & Scanner
==================================================

Phase 1  (implemented)
  • Fetches active markets from the Gamma API
  • Detects underround opportunities: sum of best-ask prices < 1.0
  • Filters by minimum net profit after a configurable fee rate
  • Logs every opportunity to SQLite
  • Alerts to console (and optionally Telegram)
  • WebSocket feed provides real-time price updates alongside REST polling

Phase 2  (stub — see execute_arb())
  • A clear hook is left for actual order execution once credentials are wired in

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
    FEE_RATE,
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
    PolymarketWSClient,
    best_ask,
    fetch_active_markets,
    fetch_order_books,
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

# markets_by_condition: condition_id -> market dict (question, tokens, …)
markets_by_condition: dict[str, dict] = {}

# live_prices: token_id -> {"price": float, "size": float}
# Updated by both REST polls and WebSocket events
live_prices: dict[str, dict] = {}
prices_lock = threading.Lock()

# Unified alert cooldown — shared by BOTH REST and WS paths.
# Keyed on condition_id.  Prevents any source from re-alerting the same
# market within WS_ALERT_COOLDOWN_SECS regardless of which thread fires first.
# Schema: {last_alert_at: datetime, last_net_profit: float}
alert_cooldown: dict[str, dict] = {}
alert_cooldown_lock = threading.Lock()

# REST-only DB state — tracks the active DB row for each live opportunity
# so REST can update it in-place instead of inserting a duplicate each cycle.
# Schema: {row_id: int, sum_asks: float}
rest_state: dict[str, dict] = {}
rest_state_lock = threading.Lock()

# WS activity tracking for Option-C REST scheduling.
# ws_activity: condition_id -> datetime of most-recent WS price event.
# ws_event_log: deque of (datetime, condition_id) for rolling 60s coverage metrics.
ws_activity: dict[str, datetime] = {}
ws_activity_lock = threading.Lock()
ws_event_log: collections.deque = collections.deque()  # type: ignore[type-arg]

# Rolling-chunk REST pointer: absolute counter; modded against total cold markets each cycle.
_rest_chunk_start: int = 0

# Module-level WS client reference — set in main() before threads start so that
# REST and other helpers can trigger subscription updates without needing the object
# passed through every call chain.
_ws_client: Optional[PolymarketWSClient] = None

# Priority WS tokens — REST-detected opportunity tokens are promoted to the front
# of the WS subscription so they survive the MAX_WS_TOKENS cap and receive
# real-time tick-by-tick updates immediately after REST spots them.
priority_token_ids: set[str] = set()
priority_lock = threading.Lock()

# ---------------------------------------------------------------------------
# WS priority subscription helper
# ---------------------------------------------------------------------------

def _prioritize_opportunity_tokens(token_ids: list[str]) -> None:
    """
    Promote token_ids to the front of the WS subscription list.

    Called by REST when it detects a live opportunity so that subsequent
    price ticks arrive in real-time via WS rather than waiting for the next
    REST chunk cycle.  Safe to call even when WS is disabled (_ws_client=None).
    """
    global _ws_client
    with priority_lock:
        new_tokens = set(token_ids) - priority_token_ids
        if not new_tokens:
            return  # already prioritized — no work to do
        priority_token_ids.update(new_tokens)

    if _ws_client is None:
        return

    # Rebuild subscription: priority tokens first so they survive the cap slice.
    all_tids = list({
        t.get("token_id") or t.get("tokenId", "")
        for m in markets_by_condition.values()
        for t in m["tokens"]
    })
    all_tids_set = set(all_tids)
    with priority_lock:
        pri = [t for t in priority_token_ids if t in all_tids_set]
    remaining = [t for t in all_tids if t not in priority_token_ids]
    _ws_client.update_subscriptions(pri + remaining)
    logger.info(
        "WS priority bump: +%d tokens promoted  |  total priority=%d",
        len(new_tokens), len(pri),
    )


# ---------------------------------------------------------------------------
# Event grouping helpers
# ---------------------------------------------------------------------------

def _group_by_event(markets: list) -> dict:
    """
    Group markets by their Gamma event_id.

    Markets that share an event_id are sibling outcomes of the same categorical
    event (e.g. ECB: 50bps / 25bps / hold / increase).  Markets without an
    event_id, or whose event_id is unique in the dataset, are treated as
    standalone binary markets.

    Returns: {event_key: [market, ...]}
      where event_key is the event_id for multi-outcome events and the
      condition_id for standalone binary markets.
    """
    # First pass: count how many markets share each event_id.
    event_counts: dict[str, int] = {}
    for m in markets:
        eid = m.get("event_id", "")
        if eid:
            event_counts[eid] = event_counts.get(eid, 0) + 1

    groups: dict[str, list] = {}
    for m in markets:
        eid = m.get("event_id", "")
        # Only group under event_id when multiple markets share it.
        if eid and event_counts.get(eid, 0) > 1:
            groups.setdefault(eid, []).append(m)
        else:
            # Standalone binary — key on condition_id to keep it independent.
            groups[m["condition_id"]] = [m]

    return groups


def _log_market_composition() -> None:
    """Log a breakdown of binary vs multi-outcome markets."""
    groups = _group_by_event(list(markets_by_condition.values()))
    n_binary = sum(1 for ms in groups.values() if len(ms) == 1)
    multi = {k: ms for k, ms in groups.items() if len(ms) > 1}
    n_multi_events  = len(multi)
    n_multi_markets = sum(len(ms) for ms in multi.values())
    logger.info(
        "Market composition: %d true binary markets  |  "
        "%d multi-outcome events (%d total outcome markets)",
        n_binary, n_multi_events, n_multi_markets,
    )
    # Log up to 5 sample multi-outcome events so we can verify grouping is correct
    if multi:
        samples = sorted(multi.items(), key=lambda kv: len(kv[1]), reverse=True)[:5]
        for eid, ms in samples:
            logger.info(
                "  Multi-outcome event  eid=%s  n=%d  first_q=%s",
                eid, len(ms), ms[0].get("question", "?")[:70],
            )


# ---------------------------------------------------------------------------
# Core arbitrage logic
# ---------------------------------------------------------------------------

def compute_opportunity(market: dict, source: str = "REST") -> Optional[dict]:
    """
    Evaluate one market for arb and near-miss.  Returns a dict with a
    'category' field ("opportunity" | "near_miss"), or None.

    Filters applied in order
    ────────────────────────
    1. Incomplete data       — any leg missing from live_prices → skip
    2. Near-certain outcome  — any leg ask_price < MIN_LEG_PRICE (1¢) → skip
    3. No underround         — sum_asks >= 1.0 → skip

    Category rules (evaluated in priority order)
    ──────────────────────────────────────────────
    opportunity : net_profit >= MIN_NET_PROFIT  AND  all sizes >= MIN_LEG_SIZE
    near_miss   : gross_profit > 0  AND  NEAR_MISS_LOWER <= net_profit < 0
                  (underround exists but fees consume it; maker rebate might flip)

    Extra fields
    ────────────
    has_zero_size : True if any leg has size <= 0 (seed-price ghost, not fillable)
    end_date_iso  : market end date, only populated when net_profit > HIGH_PROFIT_THRESHOLD
    """
    # Filter 1: resolved markets (end_date > 10 min in the past) — prices are meaningless
    mins = _minutes_to_close(market.get("end_date_iso"))
    if mins is not None and mins < -10:
        return None  # RESOLVED

    tokens = market["tokens"]
    ask_prices: list[float] = []
    ask_sizes: list[float] = []
    outcomes: list[str] = []
    token_ids: list[str] = []

    with prices_lock:
        for t in tokens:
            tid = t.get("token_id") or t.get("tokenId", "")
            outcome = t.get("outcome", tid[:8])
            entry = live_prices.get(tid)
            if entry is None:
                return None  # never polled
            if entry["price"] is None:
                return None  # polled but /books returned empty asks — NO_BOOK
            ask_prices.append(entry["price"])
            ask_sizes.append(entry["size"])
            outcomes.append(outcome)
            token_ids.append(tid)

    # Filter 2: near-certain outcomes distort the sum and aren't real inefficiencies
    if any(p < MIN_LEG_PRICE for p in ask_prices):
        return None

    sum_asks = sum(ask_prices)
    if sum_asks >= 1.0:
        return None  # no underround

    gross_profit = 1.0 - sum_asks
    total_fees = FEE_RATE * sum_asks
    net_profit = gross_profit - total_fees
    has_zero_size = any(s <= 0 for s in ask_sizes)

    # Classify
    if net_profit >= MIN_NET_PROFIT and all(s >= MIN_LEG_SIZE for s in ask_sizes):
        category = "opportunity"
    elif gross_profit > 0 and NEAR_MISS_LOWER <= net_profit < 0:
        category = "near_miss"
    else:
        return None

    # Always attach end_date_iso for opportunities so alerter can compute time-to-close
    end_date_iso = market.get("end_date_iso", "") if category == "opportunity" else None

    return {
        "condition_id": market["condition_id"],
        "question": market["question"],
        "token_ids": token_ids,
        "outcomes": outcomes,
        "ask_prices": ask_prices,
        "ask_sizes": ask_sizes,
        "sum_asks": sum_asks,
        "gross_profit": gross_profit,
        "total_fees": total_fees,
        "net_profit": net_profit,
        "fee_rate": FEE_RATE,
        "source": source,
        "category": category,
        "has_zero_size": has_zero_size,
        "end_date_iso": end_date_iso,
    }


def compute_multi_outcome_opportunity(markets: list, source: str = "REST") -> Optional[dict]:
    """
    Evaluate a multi-outcome event for arb.

    The correct arb check is: sum of YES best-asks across ALL sibling outcome
    markets < 1.0.  Buying YES on every outcome guarantees exactly one $1
    payout (the outcome that occurs).

    We NEVER sum YES+NO within a single outcome market here — that binary
    check is reserved for truly standalone binary markets via compute_opportunity().
    """
    # Skip if any outcome market is resolved
    for market in markets:
        mins = _minutes_to_close(market.get("end_date_iso"))
        if mins is not None and mins < -10:
            return None

    ask_prices: list[float] = []
    ask_sizes:  list[float] = []
    outcomes:   list[str]   = []
    token_ids:  list[str]   = []

    with prices_lock:
        for market in markets:
            # Identify the YES token: outcome label "Yes", or first token by convention.
            yes_tok = next(
                (t for t in market["tokens"] if t.get("outcome", "").lower() == "yes"),
                market["tokens"][0] if market["tokens"] else None,
            )
            if yes_tok is None:
                return None

            tid = yes_tok.get("token_id") or yes_tok.get("tokenId", "")
            entry = live_prices.get(tid)
            if entry is None:
                return None           # never polled
            if entry["price"] is None:
                return None           # no live book for this leg

            ask_prices.append(entry["price"])
            ask_sizes.append(entry["size"])
            outcomes.append(market.get("question", "")[:60])
            token_ids.append(tid)

    if any(p < MIN_LEG_PRICE for p in ask_prices):
        return None

    sum_asks = sum(ask_prices)
    if sum_asks >= 1.0:
        return None

    gross_profit = 1.0 - sum_asks
    total_fees   = FEE_RATE * sum_asks
    net_profit   = gross_profit - total_fees
    has_zero_size = any(s <= 0 for s in ask_sizes)

    if net_profit >= MIN_NET_PROFIT and all(s >= MIN_LEG_SIZE for s in ask_sizes):
        category = "opportunity"
    elif gross_profit > 0 and NEAR_MISS_LOWER <= net_profit < 0:
        category = "near_miss"
    else:
        return None

    # Use the shared event_id as the condition_id key; label as multi-way.
    event_id = markets[0].get("event_id") or markets[0]["condition_id"]
    question  = f"[{len(markets)}-way event] {markets[0]['question'][:60]}"
    end_date_iso = markets[0].get("end_date_iso", "") if category == "opportunity" else None

    return {
        "condition_id": event_id,
        "question":     question,
        "token_ids":    token_ids,
        "outcomes":     outcomes,
        "ask_prices":   ask_prices,
        "ask_sizes":    ask_sizes,
        "sum_asks":     sum_asks,
        "gross_profit": gross_profit,
        "total_fees":   total_fees,
        "net_profit":   net_profit,
        "fee_rate":     FEE_RATE,
        "source":       source,
        "category":     category,
        "has_zero_size": has_zero_size,
        "end_date_iso": end_date_iso,
    }


def scan_all_markets(source: str = "REST") -> list[dict]:
    """
    Check every tracked market/event for arb and return all opportunities.

    Routes each group to the correct compute function:
      • 1 market  per event_id → standalone binary (YES + NO check)
      • N markets per event_id → multi-outcome event (sum of YES asks check)
    """
    groups = _group_by_event(list(markets_by_condition.values()))
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
    Wire actual order placement here.  The opportunity dict contains everything
    needed: token_ids, ask_prices, ask_sizes, condition_id, etc.

    Steps to implement:
      1. Derive / load L2 API credentials from config (POLY_ADDRESS, POLY_API_KEY, …)
      2. For each (token_id, ask_price) in opp["token_ids"]:
             POST /order  {token_id, price, size, side="BUY", order_type="FOK"}
      3. Handle partial fills, cancellations, and position tracking
      4. Emit a fill event back to the alerter

    The py-clob-client library (pip install py-clob-client) provides a
    ready-made client.ClobClient that handles EIP-712 signing and L2 headers.
    """
    logger.info(
        "[PHASE 2 STUB] Would execute arb on %s (net profit %.3f%%)",
        opp["condition_id"],
        opp["net_profit"] * 100,
    )
    # TODO: implement execution




def _fire_rest_alert(result: dict, row_id: int) -> None:
    """Emit the appropriate alert for a REST-detected opportunity or near-miss."""
    if result["category"] == "opportunity":
        if result["has_zero_size"]:
            logger.debug("Opportunity #%d  net=%.3f%%  [zero-size, suppressed]",
                         row_id, result["net_profit"] * 100)
            return
        mins = _minutes_to_close(result.get("end_date_iso"))
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
    else:  # near_miss
        alert_near_miss(result)
        logger.info("Near-miss  #%d  gross=%.3f%%  net=%.3f%%  (fees ate the spread)",
                    row_id, result["gross_profit"] * 100, result["net_profit"] * 100)


def _rest_handle_result(result: dict) -> None:
    """
    Deduplicate REST scan results.

    DB logic (rest_state — REST-only):
      First detection  → INSERT row, record row_id + sum_asks.
      Same prices      → skip entirely (no DB write, no alert check).
      Prices changed   → UPDATE existing row in place; proceed to alert check.

    Alert logic (alert_cooldown — shared with WS):
      Suppressed if another source (REST or WS) alerted within
      WS_ALERT_COOLDOWN_SECS AND profit hasn't improved by WS_MIN_PROFIT_IMPROVEMENT.
    """
    cid = result["condition_id"]
    now = datetime.now(timezone.utc)

    with rest_state_lock:
        state = rest_state.get(cid)

    # ── DB: insert or update ─────────────────────────────────────────────────
    if state is not None and abs(result["sum_asks"] - state["sum_asks"]) < 0.0001:
        return  # price unchanged — skip entirely

    if state is None:
        row_id = save_opportunity(result)
        with rest_state_lock:
            rest_state[cid] = {"row_id": row_id, "sum_asks": result["sum_asks"]}
    else:
        update_opportunity(state["row_id"], result)
        row_id = state["row_id"]
        with rest_state_lock:
            rest_state[cid]["sum_asks"] = result["sum_asks"]

    # ── WS priority: promote opportunity tokens for real-time tracking ───────
    if result["category"] == "opportunity":
        _prioritize_opportunity_tokens(result["token_ids"])

    # ── Alert: check shared cooldown ─────────────────────────────────────────
    with alert_cooldown_lock:
        rec = alert_cooldown.get(cid)
        if rec is not None:
            secs_elapsed       = (now - rec["last_alert_at"]).total_seconds()
            profit_improvement = result["net_profit"] - rec["last_net_profit"]
            if secs_elapsed < WS_ALERT_COOLDOWN_SECS and profit_improvement < WS_MIN_PROFIT_IMPROVEMENT:
                return  # cooldown still active (may have been set by WS)
        alert_cooldown[cid] = {"last_alert_at": now, "last_net_profit": result["net_profit"]}

    _fire_rest_alert(result, row_id)


# ---------------------------------------------------------------------------
# REST polling loop
# ---------------------------------------------------------------------------

def rest_poll_loop(stop_event: threading.Event) -> None:
    """
    Continuously poll order books via REST and update live_prices.

    Option C — rolling-chunk with WS-skip
    ──────────────────────────────────────
    Each cycle:
      1. Identify "cold" markets: no WS price event in the last WS_FRESHNESS_SECS.
      2. Take a rolling slice of MAX_REST_MARKETS cold markets (wrapping around).
      3. Poll only that slice — WS-fresh markets are already up-to-date.

    This lets the REST budget cover all cold markets over successive cycles while
    WS-active markets stay current without wasting REST calls on them.
    """
    global _rest_chunk_start
    logger.info("REST polling loop started (interval=%.0fs, freshness=%ds)",
                REST_POLL_INTERVAL, WS_FRESHNESS_SECS)
    while not stop_event.is_set():
        now_dt = datetime.now(timezone.utc)
        all_markets = list(markets_by_condition.values())
        n_total = len(all_markets)

        # ── Identify cold markets ────────────────────────────────────────────
        with ws_activity_lock:
            fresh_conds = {
                cid for cid, last in ws_activity.items()
                if (now_dt - last).total_seconds() <= WS_FRESHNESS_SECS
            }
        cold_markets = [m for m in all_markets if m["condition_id"] not in fresh_conds]
        total_cold = len(cold_markets)
        n_ws_fresh = n_total - total_cold

        # ── Rolling chunk ────────────────────────────────────────────────────
        if total_cold > 0:
            chunk_idx = _rest_chunk_start % total_cold
            end = chunk_idx + MAX_REST_MARKETS
            if end <= total_cold:
                poll_markets = cold_markets[chunk_idx:end]
            else:
                # Wrap around the cold list
                poll_markets = cold_markets[chunk_idx:] + cold_markets[:end - total_cold]
            _rest_chunk_start = end % total_cold
            chunk_num = chunk_idx // MAX_REST_MARKETS + 1
            total_chunks = max(1, math.ceil(total_cold / MAX_REST_MARKETS))
        else:
            poll_markets = []
            _rest_chunk_start = 0
            chunk_num = 0
            total_chunks = 0

        token_ids = list({
            t.get("token_id") or t.get("tokenId", "")
            for m in poll_markets
            for t in m["tokens"]
        })
        if not token_ids:
            stop_event.wait(REST_POLL_INTERVAL)
            continue

        try:
            books = fetch_order_books(token_ids)
        except Exception as exc:
            logger.error("REST book fetch error: %s", exc)
            stop_event.wait(REST_POLL_INTERVAL)
            continue

        with prices_lock:
            for tid, book in books.items():
                price, size = best_ask(book)
                # Always overwrite — including None — to clear stale Gamma seed prices.
                # None means the /books response had an empty asks array (no live book).
                live_prices[tid] = {"price": price, "size": size}

        # ── WS coverage metrics (last 60 s) ──────────────────────────────────
        cutoff = now_dt.timestamp() - 60.0
        with ws_activity_lock:
            # Prune entries older than 60 s from the left of the deque
            while ws_event_log and ws_event_log[0][0].timestamp() < cutoff:
                ws_event_log.popleft()
            ws_events_60s = len(ws_event_log)
            ws_unique_60s = len({cid for _, cid in ws_event_log})

        # ── Scan summary ────────────────────────────────────────────────────
        n_polled = len(poll_markets)
        n_resolved = 0    # end_date > 10 min in the past
        n_full_book = 0   # all legs have a non-None ask price
        n_no_book = 0     # at least one leg returned empty asks from /books
        n_underround = 0  # sum of asks < 1.0 (regardless of profit threshold)
        underround_markets: list[tuple[str, float]] = []  # (question, net_profit)
        resolved_to_evict: list[str] = []  # condition_ids to remove from cache

        with prices_lock:
            for market in markets_by_condition.values():
                mins_left = _minutes_to_close(market.get("end_date_iso"))
                if mins_left is not None and mins_left < -10:
                    n_resolved += 1
                    resolved_to_evict.append(market["condition_id"])
                    continue
                tids = [t.get("token_id") or t.get("tokenId", "") for t in market["tokens"]]
                entries = [live_prices.get(tid) for tid in tids]
                if not all(entries):
                    continue  # never polled
                if any(e["price"] is None for e in entries):
                    n_no_book += 1
                    continue
                n_full_book += 1
                s = sum(e["price"] for e in entries)
                if s < 1.0:
                    n_underround += 1
                    net = 1.0 - (1.0 + FEE_RATE) * s
                    underround_markets.append((market["question"], net))

        # Evict resolved markets and clean up all associated state.
        for cid in resolved_to_evict:
            evicted = markets_by_condition.pop(cid, None)
            with alert_cooldown_lock:
                alert_cooldown.pop(cid, None)
            with rest_state_lock:
                rest_state.pop(cid, None)
            with ws_activity_lock:
                ws_activity.pop(cid, None)
            if evicted:
                tids = [t.get("token_id") or t.get("tokenId", "") for t in evicted.get("tokens", [])]
                with prices_lock:
                    for tid in tids:
                        live_prices.pop(tid, None)
                with priority_lock:
                    for tid in tids:
                        priority_token_ids.discard(tid)

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
    Consume events from the WebSocket queue, update live_prices, and
    immediately scan for opportunities when a price changes.
    """
    logger.info("WebSocket event loop started")
    while not stop_event.is_set():
        try:
            event = event_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        event_type = event.get("event_type") or event.get("type", "")
        token_id = event.get("asset_id") or event.get("token_id") or event.get("market", "")

        if event_type == "price_change":
            # Single price update
            price = event.get("price")
            size = event.get("size", event.get("amount", 0))
            if token_id and price is not None:
                with prices_lock:
                    live_prices[token_id] = {"price": float(price), "size": float(size)}
                _check_token_markets(token_id, source="WS")

        elif event_type in ("book", "orderbook"):
            # Full order book snapshot
            if token_id:
                asks = event.get("asks") or event.get("sells", [])
                if asks:
                    best_entry = min(asks, key=lambda a: float(a["price"]) if isinstance(a, dict) else float(a[0]))
                    if isinstance(best_entry, dict):
                        ws_price = float(best_entry.get("price", best_entry.get("p", 0)))
                        ws_size = float(best_entry.get("size", best_entry.get("s", 0)))
                    else:
                        ws_price, ws_size = float(best_entry[0]), float(best_entry[1])
                    with prices_lock:
                        live_prices[token_id] = {"price": ws_price, "size": ws_size}
                    _check_token_markets(token_id, source="WS")
                else:
                    # Empty asks from WS — clear any stale price for this token
                    with prices_lock:
                        live_prices[token_id] = {"price": None, "size": 0.0}

        elif event_type == "last_trade_price":
            # Not used for arb detection but could seed initial prices
            pass

    logger.info("WebSocket event loop stopped")


def _minutes_to_close(end_date_iso: Optional[str]) -> Optional[float]:
    """Return minutes until market closes, or None if end_date is absent/unparseable."""
    if not end_date_iso:
        return None
    try:
        normalized = end_date_iso.replace("Z", "+00:00")
        end_dt = datetime.fromisoformat(normalized)
        if end_dt.tzinfo is None:
            end_dt = end_dt.replace(tzinfo=timezone.utc)
        return (end_dt - datetime.now(timezone.utc)).total_seconds() / 60
    except (ValueError, AttributeError):
        return None


def _ws_should_alert(condition_id: str, net_profit: float) -> bool:
    """
    Return True if this market is eligible for a WS alert.

    Uses the shared alert_cooldown so REST and WS never double-alert the same
    market within WS_ALERT_COOLDOWN_SECS regardless of which thread fired first.
    Updates the record when returning True.
    """
    now = datetime.now(timezone.utc)
    with alert_cooldown_lock:
        rec = alert_cooldown.get(condition_id)
        if rec is not None:
            secs_elapsed = (now - rec["last_alert_at"]).total_seconds()
            profit_improvement = net_profit - rec["last_net_profit"]
            if secs_elapsed < WS_ALERT_COOLDOWN_SECS and profit_improvement < WS_MIN_PROFIT_IMPROVEMENT:
                return False
        alert_cooldown[condition_id] = {"last_alert_at": now, "last_net_profit": net_profit}
    return True


def _check_token_markets(token_id: str, source: str) -> None:
    """
    Find all markets containing token_id and scan them for arb / near-miss.

    WS deduplication
    ─────────────────
    Each binary market has two tokens; a single price update fires this
    function twice (once per token).  The cooldown is keyed on condition_id
    so both token events count as one.

    Expiry classification (opportunities only, after cooldown check)
    ──────────────────────────────────────────────────────────────────
    EXPIRING (<30 min)            → save to DB, suppress console
    EXPIRING_ACTIONABLE (5-30 min, net >1%, size >$50) → dedicated alert
    Normal                        → standard alert
    """
    # Find the event group(s) affected by this token update, then run the
    # correct compute function for each affected event.
    all_markets = list(markets_by_condition.values())
    groups = _group_by_event(all_markets)

    # Identify which event keys contain this token.
    affected_keys: set[str] = set()
    for key, event_markets in groups.items():
        for m in event_markets:
            ids = [t.get("token_id") or t.get("tokenId", "") for t in m["tokens"]]
            if token_id in ids:
                # Record WS activity on the individual condition so REST skipping works.
                _now = datetime.now(timezone.utc)
                with ws_activity_lock:
                    ws_activity[m["condition_id"]] = _now
                    ws_event_log.append((_now, m["condition_id"]))
                affected_keys.add(key)
                break  # one match per group is enough

    for key in affected_keys:
        event_markets = groups[key]
        if len(event_markets) == 1:
            result = compute_opportunity(event_markets[0], source=source)
        else:
            result = compute_multi_outcome_opportunity(event_markets, source=source)

        if not result:
            continue

        cond_id = result["condition_id"]

        if result["category"] == "opportunity":
            if result["has_zero_size"]:
                # Save silently; not fillable
                save_opportunity(result)
                logger.debug("WS opportunity [zero-size suppressed]  cond=...%s  net=%.3f%%",
                             cond_id[-8:], result["net_profit"] * 100)
                continue

            if not _ws_should_alert(cond_id, result["net_profit"]):
                # Cooldown active — price updated in memory but no alert/save
                logger.debug("WS cooldown hit  cond=...%s  net=%.3f%%",
                             cond_id[-8:], result["net_profit"] * 100)
                continue

            row_id = save_opportunity(result)

            mins = _minutes_to_close(result.get("end_date_iso"))
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
                # execute_arb(result)

        else:  # near_miss
            if not _ws_should_alert(cond_id, result["net_profit"]):
                logger.debug("WS cooldown hit (near-miss)  cond=...%s", cond_id[-8:])
                continue
            row_id = save_opportunity(result)
            alert_near_miss(result)
            logger.info("WS near-miss  #%d  net=%.3f%%", row_id, result["net_profit"] * 100)


# ---------------------------------------------------------------------------
# Market refresh loop
# ---------------------------------------------------------------------------

def market_refresh_loop(ws_client: Optional[PolymarketWSClient], stop_event: threading.Event) -> None:
    """Periodically re-fetch the active market list and update subscriptions."""
    logger.info("Market refresh loop started (interval=%.0fs)", MARKET_REFRESH_INTERVAL)
    while not stop_event.is_set():
        try:
            markets = fetch_active_markets()
            new_conds: dict[str, dict] = {}
            for m in markets:
                new_conds[m["condition_id"]] = m

            # Atomic swap: add/update new markets first, then remove old ones.
            # Never leave markets_by_condition empty — a clear() + update() gap
            # would cause scan_all_markets() to return [] and wrongly invalidate
            # alert_cooldown/rest_state entries.
            markets_by_condition.update(new_conds)
            for k in list(markets_by_condition):
                if k not in new_conds:
                    evicted = markets_by_condition.pop(k, None)
                    with alert_cooldown_lock:
                        alert_cooldown.pop(k, None)
                    with rest_state_lock:
                        rest_state.pop(k, None)
                    with ws_activity_lock:
                        ws_activity.pop(k, None)
                    if evicted:
                        tids = [t.get("token_id") or t.get("tokenId", "") for t in evicted.get("tokens", [])]
                        with prices_lock:
                            for tid in tids:
                                live_prices.pop(tid, None)
                        with priority_lock:
                            for tid in tids:
                                priority_token_ids.discard(tid)

            # Seed live_prices from Gamma's outcomePrices so every market has
            # an initial price before the first REST poll / WS event arrives.
            # These are mid-prices so use a placeholder size of 0 (liquidity
            # filter will exclude them until real order-book data arrives).
            with prices_lock:
                for m in markets:
                    for tid, p in m.get("seed_prices", {}).items():
                        if tid not in live_prices:
                            live_prices[tid] = {"price": p, "size": 0.0}

            if ws_client:
                all_token_ids = list({
                    t.get("token_id") or t.get("tokenId", "")
                    for m in markets_by_condition.values()
                    for t in m["tokens"]
                })
                # Priority tokens first so they survive the MAX_WS_TOKENS cap.
                with priority_lock:
                    pri = [t for t in priority_token_ids if t in set(all_token_ids)]
                remaining = [t for t in all_token_ids if t not in priority_token_ids]
                ws_client.update_subscriptions(pri + remaining)

            logger.info("Market list updated: %d markets tracked", len(markets_by_condition))
            _log_market_composition()

        except Exception as exc:
            logger.error("Market refresh error: %s", exc)

        stop_event.wait(MARKET_REFRESH_INTERVAL)

    logger.info("Market refresh loop stopped")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket Arbitrage Bot")
    parser.add_argument("--no-ws", action="store_true", help="Disable WebSocket, use REST-only")
    parser.add_argument(
        "--debug-mode",
        action="store_true",
        help=(
            "Log ALL sub-100%% books (sets min_net_profit=-5%% and min_leg_size=0). "
            "Use to verify the pipeline end-to-end before tightening thresholds."
        ),
    )
    args = parser.parse_args()

    if args.debug_mode:
        global MIN_NET_PROFIT, MIN_LEG_SIZE
        MIN_NET_PROFIT = -0.05   # surface everything, even overrounds
        MIN_LEG_SIZE = 0.0
        logger.warning(
            "DEBUG MODE active — min_net_profit=%.0f%%  min_leg_size=%.1f  "
            "(all sub-100%% books will be logged regardless of profit)",
            MIN_NET_PROFIT * 100, MIN_LEG_SIZE,
        )

    logger.info("Initialising database …")
    init_db()
    logger.info("Total opportunities logged so far: %d", opportunity_count())

    stop_event = threading.Event()

    def _shutdown(signum, frame):
        logger.info("Shutdown signal received, stopping …")
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Initial market load (blocking — must succeed before any scan)
    logger.info("Loading initial market list …")
    try:
        markets = fetch_active_markets()
        for m in markets:
            markets_by_condition[m["condition_id"]] = m
            for tid, p in m.get("seed_prices", {}).items():
                live_prices[tid] = {"price": p, "size": 0.0}
        logger.info("Loaded %d markets", len(markets_by_condition))
        _log_market_composition()
    except Exception as exc:
        logger.critical("Failed to load initial markets: %s", exc)
        sys.exit(1)

    ws_client: Optional[PolymarketWSClient] = None
    event_queue: queue.Queue = queue.Queue()

    threads: list[threading.Thread] = []

    # WebSocket setup
    global _ws_client
    if not args.no_ws:
        all_token_ids = list({
            t.get("token_id") or t.get("tokenId", "")
            for m in markets_by_condition.values()
            for t in m["tokens"]
        })
        ws_client = PolymarketWSClient(event_queue)
        _ws_client = ws_client  # expose to REST thread for priority subscription updates
        ws_client.start(all_token_ids)

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

    # REST polling
    rest_thread = threading.Thread(
        target=rest_poll_loop,
        args=(stop_event,),
        daemon=True,
        name="rest-poll",
    )
    rest_thread.start()
    threads.append(rest_thread)

    # Market refresh
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
        "WebSocket=%s  FeeRate=%.1f%%  MinProfit=%.2f%%",
        not args.no_ws,
        FEE_RATE * 100,
        MIN_NET_PROFIT * 100,
    )

    # Block until shutdown
    stop_event.wait()
    if ws_client:
        ws_client.stop()
    for t in threads:
        t.join(timeout=5)

    logger.info("Bot stopped. Total opportunities logged: %d", opportunity_count())


if __name__ == "__main__":
    main()

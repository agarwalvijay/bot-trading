"""
Kalshi Arbitrage Execution Engine
==================================

Two-phase commit with unwind logic:

  Pre-flight : re-fetch ALL leg prices fresh from Kalshi REST API.
               Abort if sum >= 1.0, net profit below threshold, or any
               leg price has drifted beyond MAX_LEG_DRIFT.

  Phase 1    : Place the LEAST liquid leg first (smallest ask_size).
               Wait for full fill confirmation.
               If timeout: cancel, abort entirely.

  Phase 2    : Place ALL remaining legs simultaneously after Phase 1 confirms.
               Wait for fills in parallel (one thread per leg).
               If all fill: ARB COMPLETE — log profit.
               If any fail: trigger UNWIND sequence.

  Unwind (in order of preference):
    1. Retry failed leg(s) once at ask + 1¢, wait UNWIND_RETRY_DELAY_SECS
    2. Limit-sell filled leg(s) at fill price, wait UNWIND_LIMIT_TIMEOUT_SECS
    3. Market-sell filled leg(s) — accept spread loss
    4. Hold as directional (only if ALLOW_DIRECTIONAL_HOLD=true)
"""

import json
import logging
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

from config import (
    ALLOW_DIRECTIONAL_HOLD,
    DEMO_MODE,
    FILL_TIMEOUT_SECS,
    MAX_CONTRACTS_PER_TRADE,
    MAX_TRADE_COST,
    MIN_NET_PROFIT,
    SETTLE_ENABLED,
    SETTLE_MIN_PROFIT_RATIO,
    SETTLE_POLL_INTERVAL,
    SETTLE_SKIP_IF_EXPIRY_MINS,
    TAKER_FEE_COEFF,
    TRADE_POLL_INTERVAL,
    TRADING_ENABLED,
    UNWIND_LIMIT_TIMEOUT_SECS,
    UNWIND_RETRY_DELAY_SECS,
)
from db import (
    create_trade,
    get_authorized_opportunities,
    get_complete_trades,
    get_trade,
    has_fresh_detection,
    mark_likely_resolved,
    set_authorized,
    stamp_trader_invoked,
    update_trade,
)
from fetcher import cancel_order, fetch_market_prices, get_order, get_order_fills, place_order

logger = logging.getLogger(__name__)

# Guard: only one trade active at a time
_trade_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Fill polling
# ---------------------------------------------------------------------------

def _wait_for_fill(order_id: str, timeout_secs: float) -> Optional[dict]:
    """
    Poll GET /portfolio/orders/{order_id} until fully filled or timeout.

    On timeout: attempts to cancel the order, then returns any partial fill.
    404 on get_order means Kalshi already processed the order (filled or
    auto-cancelled) — fall through to fill lookup immediately.
    """
    deadline = time.time() + timeout_secs
    while time.time() < deadline:
        try:
            order = get_order(order_id)
            status = order.get("status", "")
            if status == "filled":
                return order
            if status in ("canceled", "expired"):
                fc = order.get("filled_count", 0)
                return order if fc > 0 else None
        except Exception as exc:
            if "404" in str(exc):
                # Order gone from active list — already filled or auto-cancelled
                logger.info("get_order(%s) 404 — checking fills", order_id)
                return get_order_fills(order_id)
            logger.warning("get_order(%s) error: %s", order_id, exc)
        time.sleep(0.5)

    # Timeout — try to cancel, take any partial fill
    logger.warning("Fill timeout on order %s — cancelling", order_id)
    try:
        order = cancel_order(order_id)
        return order if order.get("filled_count", 0) > 0 else None
    except Exception as exc:
        if "404" in str(exc):
            # Cancel 404 = order already processed; check fills
            logger.info("cancel_order(%s) 404 — checking fills", order_id)
            return get_order_fills(order_id)
        logger.error("cancel_order(%s) failed: %s", order_id, exc)
        return None


# ---------------------------------------------------------------------------
# Pre-flight price check
# ---------------------------------------------------------------------------

def _preflight(opp: dict) -> tuple[Optional[dict], Optional[str]]:
    """
    Re-fetch current prices for all legs.

    Returns (result_dict, None) on success, or (None, reason_str) on abort.

    Aborts if:
      - Current sum of asks >= 1.0  (arb no longer exists)
      - Current net profit < MIN_NET_PROFIT
      - Any leg has insufficient size
    """
    try:
        outcome_tickers = json.loads(opp.get("outcome_tickers") or "[]")
        logged_prices   = json.loads(opp.get("ask_prices")      or "[]")
        logged_sizes    = json.loads(opp.get("ask_sizes")        or "[]")
    except Exception as exc:
        reason = f"parse_error: {exc}"
        logger.error("preflight: %s", reason)
        return None, reason

    # For binary markets the single ticker IS the event ticker
    if not outcome_tickers:
        outcome_tickers = [opp["ticker"]]

    current = fetch_market_prices(outcome_tickers)
    if not current:
        reason = "price_fetch_returned_nothing"
        logger.warning("preflight: %s — abort", reason)
        return None, reason

    legs = []
    for i, ticker in enumerate(outcome_tickers):
        entry = current.get(ticker, {})
        curr_price = entry.get("yes_ask")
        curr_size  = entry.get("yes_ask_size", 0.0)
        logged_p   = logged_prices[i] if i < len(logged_prices) else None

        # Price at/below floor with zero size → market has likely resolved
        if (curr_price is None or curr_price <= 0.01) and curr_size == 0:
            reason = f"likely_resolved: {ticker} price={curr_price} size=0"
            logger.info("preflight: %s", reason)
            return None, reason

        if curr_price is None:
            reason = f"no_ask_price: {ticker}"
            logger.warning("preflight: %s — abort", reason)
            return None, reason

        legs.append({
            "ticker":       ticker,
            "price":        curr_price,
            "size":         curr_size,
            "logged_price": logged_p,
            "logged_size":  logged_sizes[i] if i < len(logged_sizes) else 0.0,
        })

    curr_sum   = sum(l["price"] for l in legs)
    gross      = 1.0 - curr_sum
    fees       = sum(TAKER_FEE_COEFF * l["price"] * (1 - l["price"]) for l in legs)
    net_profit = gross - fees

    if curr_sum >= 1.0:
        reason = f"sum_ge_1: sum={curr_sum:.4f}"
        logger.info("preflight: %s — no arb, abort", reason)
        return None, reason

    if net_profit < MIN_NET_PROFIT:
        reason = f"net_profit_too_low: {net_profit*100:.3f}% < min={MIN_NET_PROFIT*100:.3f}%"
        logger.info("preflight: %s — abort", reason)
        return None, reason

    # Sort least→most liquid (smallest size first = Phase 1 leg)
    legs.sort(key=lambda l: l["size"])

    # Cap contract count: min(available_size, MAX_CONTRACTS_PER_TRADE, budget-based cap)
    # Total cost = curr_sum * count  (each contract costs its ask price)
    budget_count = int(MAX_TRADE_COST / curr_sum) if curr_sum > 0 else MAX_CONTRACTS_PER_TRADE
    max_count = min(int(min(l["size"] for l in legs)), MAX_CONTRACTS_PER_TRADE, budget_count)
    if max_count <= 0:
        reason = "zero_contracts_available"
        logger.warning("preflight: %s — abort", reason)
        return None, reason

    logger.info(
        "preflight OK: sum=%.4f  net=+%.2f%%  contracts=%d  legs=%s",
        curr_sum, net_profit * 100, max_count,
        [(l["ticker"], l["price"]) for l in legs],
    )
    return {"legs": legs, "count": max_count, "net_profit": net_profit, "sum": curr_sum}, None


# ---------------------------------------------------------------------------
# Unwind logic
# ---------------------------------------------------------------------------

def _unwind(trade_id: int, filled_legs: list[dict]) -> None:
    """
    Attempt to unwind filled legs in order of preference.

    filled_legs: list of {"ticker", "order_id", "fill_price", "fill_count", "next_ask"}
    """
    logger.warning("UNWIND triggered for trade %d — %d leg(s) to unwind",
                   trade_id, len(filled_legs))

    # ── Step 1: retry failed leg at ask + 1¢ ────────────────────────────────
    # (Caller passes next_ask for the failed leg; this is handled in execute_trade)
    # This function only unwinds already-filled legs.

    for leg in filled_legs:
        ticker     = leg["ticker"]
        count      = leg["fill_count"]
        fill_price = leg["fill_price"]

        logger.info("Unwinding %d contracts of %s (filled at %.4f)", count, ticker, fill_price)
        unwound = False

        # ── Step 2: limit sell at fill price ────────────────────────────────
        try:
            coid = str(uuid.uuid4())
            sell = place_order(ticker, "sell", "yes", count,
                               fill_price, order_type="limit",
                               client_order_id=coid)
            sell_id = sell.get("order_id", "")
            filled_sell = _wait_for_fill(sell_id, UNWIND_LIMIT_TIMEOUT_SECS)
            if filled_sell and filled_sell.get("filled_count", 0) > 0:
                sell_price = filled_sell.get("yes_price", fill_price * 100) / 100
                pnl = (sell_price - fill_price) * count
                logger.info("Limit-sell unwind filled: %.4f  pnl=%.2f", sell_price, pnl)
                update_trade(trade_id,
                             status="unwind_limit",
                             unwind_reason=f"limit_sell ticker={ticker} pnl={pnl:.4f}",
                             net_pnl=pnl)
                unwound = True
                continue
            else:
                logger.warning("Limit-sell timeout for %s — attempting cancel before market sell", ticker)
                cancel_ok = False
                try:
                    cancel_order(sell_id)
                    cancel_ok = True
                except Exception as exc:
                    if "404" in str(exc):
                        # Order already processed (may have filled at the last moment)
                        logger.info("cancel_order(%s) 404 during unwind — treating as filled", sell_id)
                        cancel_ok = True  # safe to proceed; order is gone
                    else:
                        logger.error(
                            "cancel_order(%s) FAILED during unwind: %s — "
                            "skipping market sell to avoid double-sell; manual intervention needed",
                            sell_id, exc,
                        )
                        update_trade(trade_id,
                                     status="unwind_failed",
                                     unwind_reason=f"cancel_failed_before_market_sell ticker={ticker}")
                if not cancel_ok:
                    continue  # skip market sell for this leg — order state unknown
        except Exception as exc:
            logger.error("Limit-sell order failed for %s: %s", ticker, exc)

        if unwound:
            continue

        # ── Step 3: market sell ──────────────────────────────────────────────
        try:
            coid = str(uuid.uuid4())
            sell = place_order(ticker, "sell", "yes", count,
                               None, order_type="market",
                               client_order_id=coid)
            sell_id = sell.get("order_id", "")
            filled_sell = _wait_for_fill(sell_id, 30)
            if filled_sell and filled_sell.get("filled_count", 0) > 0:
                sell_price = filled_sell.get("yes_price", 0) / 100
                pnl = (sell_price - fill_price) * count
                logger.warning("Market-sell unwind: %.4f  pnl=%.2f", sell_price, pnl)
                update_trade(trade_id,
                             status="unwind_market",
                             unwind_reason=f"market_sell ticker={ticker} pnl={pnl:.4f}",
                             net_pnl=pnl)
                unwound = True
            else:
                logger.error("Market-sell FAILED for %s — manual intervention needed", ticker)
        except Exception as exc:
            logger.error("Market-sell order failed for %s: %s", ticker, exc)

        if unwound:
            continue

        # ── Step 4: directional hold ─────────────────────────────────────────
        if ALLOW_DIRECTIONAL_HOLD:
            logger.warning(
                "PARTIAL_ARB_HELD: holding %d × %s at %.4f as directional position",
                count, ticker, fill_price,
            )
            update_trade(trade_id,
                         status="unwind_hold",
                         unwind_reason=f"directional_hold ticker={ticker}")
        else:
            logger.error(
                "UNWIND_FAILED: could not sell %d × %s — set ALLOW_DIRECTIONAL_HOLD=true "
                "or close position manually", count, ticker,
            )
            update_trade(trade_id,
                         status="unwind_failed",
                         unwind_reason=f"all_unwind_options_exhausted ticker={ticker}")


# ---------------------------------------------------------------------------
# Core execution
# ---------------------------------------------------------------------------

def execute_trade(opp: dict) -> None:
    """
    Execute a two-phase arb trade for an authorized opportunity.
    All state is written to the trades table.
    """
    opp_id = opp["id"]
    mode   = "DEMO" if DEMO_MODE else "LIVE"
    logger.info("=== TRADE START [%s] opp_id=%d  %s ===", mode, opp_id, opp.get("title", ""))
    stamp_trader_invoked(opp_id)

    # ── Pre-flight ───────────────────────────────────────────────────────────
    verified, abort_reason = _preflight(opp)
    if verified is None:
        if abort_reason and abort_reason.startswith("likely_resolved:"):
            mark_likely_resolved(opp_id)
            logger.info("Auto-deauthorized likely-resolved opp_id=%d: %s", opp_id, abort_reason)
        elif abort_reason and any(abort_reason.startswith(p) for p in ("sum_ge_1:", "net_profit_too_low:")):
            # Arb temporarily closed — stay authorized to catch next reappearance, don't spam logs
            logger.debug("Preflight (opp_id=%d): %s — waiting for arb to reopen", opp_id, abort_reason)
        else:
            # Transient failure (prices momentarily unavailable) — leave authorized, retry next tick
            logger.info("Preflight failed (opp_id=%d): %s — will retry", opp_id, abort_reason)
        return False  # preflight failed — caller may try next authorized opp

    legs      = verified["legs"]
    count     = verified["count"]
    leg_tickers  = [l["ticker"] for l in legs]
    target_prices = [l["price"] for l in legs]
    coids     = [str(uuid.uuid4()) for _ in legs]

    trade_id = create_trade(opp_id, leg_tickers, [count] * len(legs),
                            target_prices, coids, DEMO_MODE)
    logger.info("Trade record created: trade_id=%d", trade_id)

    filled_legs  = []   # legs successfully filled (for unwind if needed)
    order_ids    = []
    fill_prices  = []
    fill_counts  = []

    # ── Phase 1: least-liquid leg ────────────────────────────────────────────
    leg1 = legs[0]
    logger.info("Phase 1: %s  count=%d  price=%.4f  coid=%s",
                leg1["ticker"], count, leg1["price"], coids[0])
    update_trade(trade_id, status="phase1_placed")

    try:
        order1 = place_order(leg1["ticker"], "buy", "yes", count,
                             leg1["price"], client_order_id=coids[0])
        oid1 = order1.get("order_id", "")
        order_ids.append(oid1)
        update_trade(trade_id, order_ids=json.dumps(order_ids))
    except Exception as exc:
        logger.error("Phase 1 place_order failed: %s", exc)
        update_trade(trade_id, status="aborted",
                     notes=f"phase1_place_error: {exc}",
                     completed_at=datetime.now(timezone.utc).isoformat())
        return

    filled1 = _wait_for_fill(oid1, FILL_TIMEOUT_SECS)
    if not filled1 or filled1.get("filled_count", 0) == 0:
        logger.warning("Phase 1 not filled (timeout) opp_id=%d — will retry next tick", opp_id)
        update_trade(trade_id, status="aborted",
                     notes="phase1_timeout",
                     completed_at=datetime.now(timezone.utc).isoformat())
        return True  # trade was attempted

    actual_count = filled1.get("filled_count", count)
    fp1 = filled1.get("yes_price", round(leg1["price"] * 100)) / 100
    fill_prices.append(fp1)
    fill_counts.append(actual_count)
    filled_legs.append({"ticker": leg1["ticker"], "order_id": oid1,
                        "fill_price": fp1, "fill_count": actual_count})

    logger.info("Phase 1 FILLED: %d × %s @ %.4f", actual_count, leg1["ticker"], fp1)
    update_trade(trade_id,
                 status="phase1_filled",
                 fill_prices=json.dumps(fill_prices),
                 fill_counts=json.dumps(fill_counts))

    # ── Phase 2+: remaining legs (simultaneous placement, parallel fill wait) ─
    phase2_failed = []
    phase2_legs   = legs[1:]

    # Fire all phase 2 orders at once
    p2_orders = []  # list of (leg, order_id) or (leg, None) on place failure
    update_trade(trade_id, status="phase2_placed")
    for i, leg in enumerate(phase2_legs, start=1):
        logger.info("Phase 2 place leg %d: %s  count=%d  price=%.4f",
                    i, leg["ticker"], actual_count, leg["price"])
        try:
            order = place_order(leg["ticker"], "buy", "yes", actual_count,
                                leg["price"], client_order_id=coids[i])
            oid = order.get("order_id", "")
            order_ids.append(oid)
            p2_orders.append((leg, oid))
        except Exception as exc:
            logger.error("Phase 2 place_order failed for %s: %s", leg["ticker"], exc)
            p2_orders.append((leg, None))
            phase2_failed.append(leg)

    update_trade(trade_id, order_ids=json.dumps(order_ids))

    # Now wait for all fills in parallel (one thread per leg)
    p2_results: dict[str, Optional[dict]] = {}

    def _fill_worker(ticker: str, oid: str) -> None:
        p2_results[ticker] = _wait_for_fill(oid, FILL_TIMEOUT_SECS)

    threads = []
    for leg, oid in p2_orders:
        if oid is None:
            continue
        t = threading.Thread(target=_fill_worker, args=(leg["ticker"], oid), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # Collect results
    for i, (leg, oid) in enumerate(p2_orders, start=1):
        if oid is None:
            continue
        filled = p2_results.get(leg["ticker"])
        if filled and filled.get("filled_count", 0) > 0:
            fc = filled.get("filled_count", actual_count)
            fp = filled.get("yes_price", round(leg["price"] * 100)) / 100
            fill_prices.append(fp)
            fill_counts.append(fc)
            filled_legs.append({"ticker": leg["ticker"], "order_id": oid,
                                 "fill_price": fp, "fill_count": fc})
            logger.info("Phase 2 leg %d FILLED: %d × %s @ %.4f", i, fc, leg["ticker"], fp)
        else:
            logger.warning("Phase 2 leg %d NOT filled: %s", i, leg["ticker"])
            phase2_failed.append(leg)

    update_trade(trade_id,
                 fill_prices=json.dumps(fill_prices),
                 fill_counts=json.dumps(fill_counts))

    # ── Unwind check ─────────────────────────────────────────────────────────
    if phase2_failed:
        # Step 1: retry failed legs once at ask + 1¢
        still_failed = []
        time.sleep(UNWIND_RETRY_DELAY_SECS)
        update_trade(trade_id, status="unwind_retry")

        retry_tickers = [l["ticker"] for l in phase2_failed]
        refreshed = fetch_market_prices(retry_tickers)

        for leg in phase2_failed:
            curr = refreshed.get(leg["ticker"], {})
            retry_price = curr.get("yes_ask")
            if retry_price is None or retry_price > leg["price"] + 0.02:
                logger.warning("Retry price %.4f too high for %s — skip retry",
                               retry_price or 0, leg["ticker"])
                still_failed.append(leg)
                continue

            coid_r = str(uuid.uuid4())
            try:
                order_r = place_order(leg["ticker"], "buy", "yes", actual_count,
                                      retry_price, client_order_id=coid_r)
                oid_r = order_r.get("order_id", "")
                filled_r = _wait_for_fill(oid_r, FILL_TIMEOUT_SECS)
                if filled_r and filled_r.get("filled_count", 0) > 0:
                    fc  = filled_r.get("filled_count", actual_count)
                    fp  = filled_r.get("yes_price", round(retry_price * 100)) / 100
                    fill_prices.append(fp)
                    fill_counts.append(fc)
                    filled_legs.append({"ticker": leg["ticker"], "order_id": oid_r,
                                        "fill_price": fp, "fill_count": fc})
                    logger.info("Retry FILLED: %d × %s @ %.4f", fc, leg["ticker"], fp)
                else:
                    logger.warning("Retry also failed for %s", leg["ticker"])
                    still_failed.append(leg)
            except Exception as exc:
                logger.error("Retry order failed for %s: %s", leg["ticker"], exc)
                still_failed.append(leg)

        if still_failed:
            # Unwind all legs that DID fill (we can't complete the arb)
            legs_to_unwind = [fl for fl in filled_legs]
            _unwind(trade_id, legs_to_unwind)
            update_trade(trade_id,
                         completed_at=datetime.now(timezone.utc).isoformat(),
                         fill_prices=json.dumps(fill_prices),
                         fill_counts=json.dumps(fill_counts))
            return True  # trade was attempted

    # ── ARB COMPLETE ─────────────────────────────────────────────────────────
    total_cost    = sum(fp * fc for fp, fc in zip(fill_prices, fill_counts))
    payout        = actual_count  # $1 per contract if all legs resolve correctly
    gross_pnl     = payout - total_cost
    fee_pnl       = sum(TAKER_FEE_COEFF * fp * (1 - fp) * fc
                        for fp, fc in zip(fill_prices, fill_counts))
    net_pnl       = gross_pnl - fee_pnl

    now = datetime.now(timezone.utc).isoformat()
    update_trade(trade_id,
                 status="complete",
                 completed_at=now,
                 fill_prices=json.dumps(fill_prices),
                 fill_counts=json.dumps(fill_counts),
                 gross_pnl=gross_pnl,
                 fee_pnl=fee_pnl,
                 net_pnl=net_pnl)

    logger.info(
        "=== ARB COMPLETE [%s] trade_id=%d  contracts=%d  "
        "gross=+$%.4f  fees=-$%.4f  net=+$%.4f ===",
        mode, trade_id, actual_count, gross_pnl, fee_pnl, net_pnl,
    )
    return True  # trade was attempted


# ---------------------------------------------------------------------------
# Trading loop (runs as a daemon thread)
# ---------------------------------------------------------------------------

def trading_loop(stop_event: threading.Event,
                 trade_trigger: Optional[threading.Event] = None,
                 priority_ids=None) -> None:
    """
    Poll for authorized opportunities and execute trades one at a time.
    Runs as a background thread started from arb_bot.main().

    trade_trigger: optional Event set by WS/REST handlers when a fresh arb is
    detected. Wakes the loop immediately rather than waiting TRADE_POLL_INTERVAL.
    priority_ids: optional deque of opp_ids to try first this tick.
    """
    if not TRADING_ENABLED:
        logger.info("Trading disabled (TRADING_ENABLED=false) — trading loop not running")
        return

    mode = "DEMO" if DEMO_MODE else "LIVE"
    logger.info("Trading loop started [%s]", mode)

    # First run is treated as event-driven (handles authorized opps present at startup)
    triggered = True

    while not stop_event.is_set():
        try:
            opps = get_authorized_opportunities()
            if opps:
                # Drain priority deque — sort triggered opp(s) to front
                pri: set = set()
                if priority_ids:
                    while True:
                        try:
                            pri.add(priority_ids.popleft())
                        except IndexError:
                            break
                    opps.sort(key=lambda o: 0 if o["id"] in pri else 1)

                logger.info("Trading loop: %d authorized opportunity/ies [%s]",
                            len(opps), "event" if triggered else "poll")
                # One trade at a time — acquire lock and try each opp in priority order
                if _trade_lock.acquire(blocking=False):
                    try:
                        for opp in opps:
                            # Poll-mode: skip opps with no fresh detection since last invocation
                            if not triggered:
                                invoked_at = opp.get("trader_invoked_at")
                                if invoked_at and not has_fresh_detection(opp["ticker"], invoked_at):
                                    logger.debug("Poll: skipping opp_id=%d (no fresh detection since %s)",
                                                 opp["id"], invoked_at)
                                    continue
                            if execute_trade(opp):  # True = trade attempted; stop iterating
                                break
                    finally:
                        _trade_lock.release()
                else:
                    logger.debug("Trade already in progress — skipping tick")
        except Exception as exc:
            logger.error("Trading loop error: %s", exc)

        # Wait for trigger (immediate wake on new arb) or fallback timeout
        # Capture result for next iteration: True = event-driven, False = poll timeout
        if trade_trigger is not None:
            triggered = trade_trigger.wait(timeout=TRADE_POLL_INTERVAL)
            trade_trigger.clear()
            if triggered:
                logger.debug("Trading loop woken by arb trigger")
        else:
            stop_event.wait(TRADE_POLL_INTERVAL)
            triggered = False

    logger.info("Trading loop stopped")


# ---------------------------------------------------------------------------
# Settle loop (runs as a daemon thread)
# ---------------------------------------------------------------------------

def _try_settle(trade: dict) -> None:
    """
    Check whether a completed trade can be exited early at a profit.

    Fetches current YES bids for all leg tickers. If selling all positions now
    yields exit_profit >= SETTLE_MIN_PROFIT_RATIO × recorded net_pnl, place
    simultaneous sell orders and update the trade to 'settled'.
    """
    trade_id   = trade["id"]
    mode       = "DEMO" if DEMO_MODE else "LIVE"

    try:
        leg_tickers  = json.loads(trade.get("leg_tickers")  or "[]")
        fill_prices_l = json.loads(trade.get("fill_prices") or "[]")
        fill_counts_l = json.loads(trade.get("fill_counts") or "[]")
    except Exception as exc:
        logger.warning("settle trade %d: parse error %s", trade_id, exc)
        return

    if not leg_tickers or not fill_prices_l:
        return

    if len(fill_counts_l) != len(leg_tickers) or len(fill_prices_l) != len(leg_tickers):
        logger.warning("settle trade %d: leg/fill count mismatch — skipping", trade_id)
        return

    if any(fc <= 0 for fc in fill_counts_l):
        return

    entry_cost = sum(fp * fc for fp, fc in zip(fill_prices_l, fill_counts_l))

    # Skip if expiry is imminent — just let it resolve
    close_time = trade.get("close_time", "") or ""
    if close_time:
        try:
            ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            if ct.tzinfo is None:
                ct = ct.replace(tzinfo=timezone.utc)
            mins_left = (ct - datetime.now(timezone.utc)).total_seconds() / 60
            if mins_left < SETTLE_SKIP_IF_EXPIRY_MINS:
                logger.debug("settle trade %d: expiry in %.0f min — skipping", trade_id, mins_left)
                return
        except (ValueError, AttributeError):
            pass

    # Fetch current bids
    current = fetch_market_prices(leg_tickers)
    if not current:
        return

    bids       = []
    bid_sizes  = []
    for ticker in leg_tickers:
        entry  = current.get(ticker, {})
        bid    = entry.get("yes_bid")
        bsize  = entry.get("yes_bid_size", 0.0)
        if bid is None:
            logger.debug("settle trade %d: no bid for %s", trade_id, ticker)
            return
        bids.append(bid)
        bid_sizes.append(bsize)

    # Ensure bid liquidity covers each leg's actual position
    for i, (bsize, fc) in enumerate(zip(bid_sizes, fill_counts_l)):
        if bsize < fc:
            logger.debug(
                "settle trade %d: insufficient bid size for leg %d %s (need %d, available %.0f)",
                trade_id, i, leg_tickers[i], fc, bsize,
            )
            return

    exit_revenue = sum(b * fc for b, fc in zip(bids, fill_counts_l))
    exit_fees    = sum(TAKER_FEE_COEFF * b * (1 - b) * fc for b, fc in zip(bids, fill_counts_l))
    exit_profit  = exit_revenue - exit_fees - entry_cost

    recorded_net = trade.get("net_pnl") or 0.0
    threshold    = SETTLE_MIN_PROFIT_RATIO * recorded_net

    logger.debug(
        "settle trade %d: exit_profit=%.4f  threshold=%.4f (ratio=%.2f × net=%.4f)  bids=%s",
        trade_id, exit_profit, threshold, SETTLE_MIN_PROFIT_RATIO, recorded_net,
        [round(b, 4) for b in bids],
    )

    if exit_profit < threshold:
        return

    logger.info(
        "=== SETTLE [%s] trade_id=%d  exit_profit=+$%.4f  (threshold=+$%.4f) ===",
        mode, trade_id, exit_profit, threshold,
    )

    # Place simultaneous sell orders (use per-leg fill count, not a shared count)
    sell_orders = []
    for i, (ticker, bid) in enumerate(zip(leg_tickers, bids)):
        fc = fill_counts_l[i]
        coid = str(uuid.uuid4())
        try:
            order = place_order(ticker, "sell", "yes", fc,
                                bid, order_type="limit", client_order_id=coid)
            sell_orders.append((ticker, bid, fc, order.get("order_id", "")))
            logger.info("Settle sell placed: %s  count=%d  bid=%.4f", ticker, fc, bid)
        except Exception as exc:
            logger.error("Settle sell failed for %s: %s — aborting settle", ticker, exc)
            # Cancel already-placed sells to avoid partial exit
            for _, _, placed_oid in sell_orders:
                try:
                    cancel_order(placed_oid)
                except Exception:
                    pass
            return

    # Wait for all sell fills in parallel
    sell_results: dict[str, Optional[dict]] = {}

    def _sell_fill_worker(ticker: str, oid: str) -> None:
        sell_results[ticker] = _wait_for_fill(oid, FILL_TIMEOUT_SECS)

    threads = []
    for ticker, _, fc, oid in sell_orders:
        t = threading.Thread(target=_sell_fill_worker, args=(ticker, oid), daemon=True)
        t.start()
        threads.append(t)
    for t in threads:
        t.join()

    exit_prices_actual = []
    all_filled = True
    for ticker, bid, fc, oid in sell_orders:
        filled = sell_results.get(ticker)
        if filled and filled.get("filled_count", 0) > 0:
            ep = filled.get("yes_price", round(bid * 100)) / 100
            exit_prices_actual.append(ep)
            logger.info("Settle fill confirmed: %s @ %.4f", ticker, ep)
        else:
            logger.warning("Settle sell NOT filled for %s — trade left open", ticker)
            all_filled = False
            exit_prices_actual.append(None)

    if not all_filled:
        # Cancel any unfilled sell orders and leave trade as 'complete' for retry
        for ticker, _, fc, oid in sell_orders:
            try:
                cancel_order(oid)
            except Exception:
                pass
        return

    # Compute actual exit PnL using per-leg fill counts
    actual_exit_revenue = sum(
        ep * fc for ep, (_, _, fc, _) in zip(exit_prices_actual, sell_orders)
        if ep is not None
    )
    actual_exit_fees = sum(
        TAKER_FEE_COEFF * ep * (1 - ep) * fc
        for ep, (_, _, fc, _) in zip(exit_prices_actual, sell_orders)
        if ep is not None
    )
    actual_exit_pnl = actual_exit_revenue - actual_exit_fees - entry_cost

    update_trade(trade_id,
                 status="settled",
                 completed_at=datetime.now(timezone.utc).isoformat(),
                 exit_prices=json.dumps(exit_prices_actual),
                 exit_pnl=actual_exit_pnl)

    logger.info(
        "=== SETTLED [%s] trade_id=%d  exit_pnl=+$%.4f  "
        "(vs theoretical net=+$%.4f  saved %.0f%% of wait) ===",
        mode, trade_id, actual_exit_pnl, recorded_net,
        100 * actual_exit_pnl / recorded_net if recorded_net else 0,
    )


def settle_loop(stop_event: threading.Event) -> None:
    """
    Monitor completed trades and exit positions early when profitable.
    Runs as a background thread started from arb_bot.main().
    """
    if not SETTLE_ENABLED:
        logger.info("Settle loop disabled (SETTLE_ENABLED=false)")
        return

    mode = "DEMO" if DEMO_MODE else "LIVE"
    logger.info("Settle loop started [%s]  poll=%ds  ratio=%.2f",
                mode, SETTLE_POLL_INTERVAL, SETTLE_MIN_PROFIT_RATIO)

    while not stop_event.is_set():
        try:
            trades = get_complete_trades()
            if trades:
                logger.debug("Settle loop: checking %d complete trade(s)", len(trades))
            for trade in trades:
                _try_settle(trade)
        except Exception as exc:
            logger.error("Settle loop error: %s", exc)

        stop_event.wait(SETTLE_POLL_INTERVAL)

    logger.info("Settle loop stopped")

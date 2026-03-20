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

  Phase 2    : Place remaining legs immediately after Phase 1 confirms.
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
    MAX_LEG_DRIFT,
    MIN_NET_PROFIT,
    TAKER_FEE_COEFF,
    TRADE_POLL_INTERVAL,
    TRADING_ENABLED,
    UNWIND_LIMIT_TIMEOUT_SECS,
    UNWIND_RETRY_DELAY_SECS,
)
from db import (
    create_trade,
    get_authorized_opportunities,
    get_trade,
    mark_likely_resolved,
    update_trade,
)
from fetcher import cancel_order, fetch_market_prices, get_order, place_order

logger = logging.getLogger(__name__)

# Guard: only one trade active at a time
_trade_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Fill polling
# ---------------------------------------------------------------------------

def _wait_for_fill(order_id: str, timeout_secs: float) -> Optional[dict]:
    """
    Poll GET /portfolio/orders/{order_id} until fully filled or timeout.

    On timeout: attempts to cancel the order, then returns any partial fill
    (or None if nothing filled).
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
            logger.warning("get_order(%s) error: %s", order_id, exc)
        time.sleep(0.5)

    # Timeout — try to cancel, take any partial fill
    logger.warning("Fill timeout on order %s — cancelling", order_id)
    try:
        order = cancel_order(order_id)
        return order if order.get("filled_count", 0) > 0 else None
    except Exception as exc:
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
      - Any leg price has drifted > MAX_LEG_DRIFT from detected price
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

        if logged_p is not None and abs(curr_price - logged_p) > MAX_LEG_DRIFT:
            drift = abs(curr_price - logged_p)
            reason = f"price_drift: {ticker} {logged_p:.4f}→{curr_price:.4f} drift={drift:.4f}"
            logger.warning("preflight: %s (max={MAX_LEG_DRIFT}) — abort", reason)
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

    # Cap contract count at min(available_size, MAX_CONTRACTS_PER_TRADE)
    max_count = min(int(min(l["size"] for l in legs)), MAX_CONTRACTS_PER_TRADE)
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
                logger.warning("Limit-sell timeout for %s — escalating to market", ticker)
                try:
                    cancel_order(sell_id)
                except Exception:
                    pass
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

    # ── Pre-flight ───────────────────────────────────────────────────────────
    verified, abort_reason = _preflight(opp)
    if verified is None:
        if abort_reason and abort_reason.startswith("likely_resolved:"):
            mark_likely_resolved(opp_id)
            logger.info("Auto-deauthorized likely-resolved opp_id=%d: %s", opp_id, abort_reason)
        else:
            # Transient failure (drift, spread closed) — leave authorized, retry next tick
            logger.info("Preflight failed (opp_id=%d): %s — will retry", opp_id, abort_reason)
        return

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
        logger.warning("Phase 1 not filled — aborting (opp_id=%d)", opp_id)
        update_trade(trade_id, status="aborted",
                     notes="phase1_timeout",
                     completed_at=datetime.now(timezone.utc).isoformat())
        return

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

    # ── Phase 2+: remaining legs ─────────────────────────────────────────────
    phase2_failed = []

    for i, leg in enumerate(legs[1:], start=1):
        logger.info("Phase 2 leg %d: %s  count=%d  price=%.4f",
                    i, leg["ticker"], actual_count, leg["price"])
        update_trade(trade_id, status="phase2_placed")

        try:
            order = place_order(leg["ticker"], "buy", "yes", actual_count,
                                leg["price"], client_order_id=coids[i])
            oid = order.get("order_id", "")
            order_ids.append(oid)
            update_trade(trade_id, order_ids=json.dumps(order_ids))
        except Exception as exc:
            logger.error("Phase 2 place_order failed for %s: %s", leg["ticker"], exc)
            phase2_failed.append(leg)
            continue

        filled = _wait_for_fill(oid, FILL_TIMEOUT_SECS)

        if filled and filled.get("filled_count", 0) > 0:
            fc  = filled.get("filled_count", actual_count)
            fp  = filled.get("yes_price", round(leg["price"] * 100)) / 100
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
            return

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


# ---------------------------------------------------------------------------
# Trading loop (runs as a daemon thread)
# ---------------------------------------------------------------------------

def trading_loop(stop_event: threading.Event) -> None:
    """
    Poll for authorized opportunities and execute trades one at a time.
    Runs as a background thread started from arb_bot.main().
    """
    if not TRADING_ENABLED:
        logger.info("Trading disabled (TRADING_ENABLED=false) — trading loop not running")
        return

    mode = "DEMO" if DEMO_MODE else "LIVE"
    logger.info("Trading loop started [%s]", mode)

    while not stop_event.is_set():
        try:
            opps = get_authorized_opportunities()
            if opps:
                logger.info("Trading loop: %d authorized opportunity/ies", len(opps))
                # One trade at a time — acquire lock and execute
                if _trade_lock.acquire(blocking=False):
                    try:
                        execute_trade(opps[0])
                    finally:
                        _trade_lock.release()
                else:
                    logger.debug("Trade already in progress — skipping tick")
        except Exception as exc:
            logger.error("Trading loop error: %s", exc)

        stop_event.wait(TRADE_POLL_INTERVAL)

    logger.info("Trading loop stopped")

"""
Data fetching layer.

Provides:
  - REST helpers (Gamma market list, CLOB order books)
  - WebSocket client that streams real-time price updates and feeds them
    into a thread-safe queue consumed by arb_bot.py
"""

import json
import logging
import queue
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import requests
import websocket  # websocket-client

from config import (
    BOOK_BATCH_SIZE,
    CLOB_BASE_URL,
    GAMMA_BASE_URL,
    GAMMA_PAGE_SIZE,
    GAMMA_SORT_FIELD,
    MAX_MARKETS,
    MAX_WS_TOKENS,
    WS_URL,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# REST helpers
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


def _get(url: str, params: Optional[dict] = None, retries: int = 3) -> Any:
    """GET with simple exponential-backoff retry."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            logger.warning("HTTP %s on GET %s (attempt %d)", exc.response.status_code, url, attempt + 1)
            if exc.response.status_code in (429, 503):
                time.sleep(2 ** attempt)
            else:
                raise
        except requests.RequestException as exc:
            logger.warning("Request error on GET %s: %s (attempt %d)", url, exc, attempt + 1)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to GET {url} after {retries} attempts")


def fetch_active_markets() -> list[dict]:
    """
    Return active CLOB markets from the Gamma API.

    Each returned dict contains:
        condition_id  – unique market identifier
        question      – human-readable question
        tokens        – list of {token_id, outcome}
        seed_prices   – {token_id: best_ask} from Gamma (used to prime live_prices)
    """
    global _event_field_sample_logged
    markets: list[dict] = []
    offset = 0
    page = 0

    while True:
        data = _get(
            f"{GAMMA_BASE_URL}/markets",
            params={
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": GAMMA_PAGE_SIZE,
                "offset": offset,
                "order": GAMMA_SORT_FIELD,
                "ascending": "false",
            },
        )

        # Gamma returns a list directly or {"markets": [...]}
        batch = data if isinstance(data, list) else data.get("markets", [])
        page += 1
        logger.debug("Gamma page %d: %d raw markets (offset=%d)", page, len(batch), offset)

        if not batch:
            break

        for m in batch:
            # Skip markets not on the CLOB
            if not m.get("enableOrderBook") or not m.get("acceptingOrders"):
                continue

            # One-time diagnostic: log all event-related fields from the first market
            # that has any of them set.  This lets us verify we're reading the right keys.
            if not _event_field_sample_logged:
                event_keys = {k: m[k] for k in m if "event" in k.lower() or k in ("groupId", "parentEventId", "parentConditionId")}
                if event_keys or m.get("eventId"):
                    logger.info(
                        "Gamma event-field sample (cond=...%s  q=%s): %s",
                        str(m.get("conditionId", "?"))[-12:],
                        m.get("question", "?")[:60],
                        event_keys,
                    )
                    _event_field_sample_logged = True

            # clobTokenIds and outcomes are JSON-encoded strings in the Gamma response
            raw_token_ids = m.get("clobTokenIds", "[]")
            raw_outcomes = m.get("outcomes", "[]")
            try:
                token_ids = json.loads(raw_token_ids) if isinstance(raw_token_ids, str) else raw_token_ids
                outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
            except (json.JSONDecodeError, TypeError):
                continue

            if len(token_ids) < 2:
                continue

            tokens = [
                {"token_id": tid, "outcome": out}
                for tid, out in zip(token_ids, outcomes)
            ]

            # Gamma includes bestAsk per market (single price for binary YES token).
            # Use outcomePrices to seed both legs when available.
            seed_prices: dict[str, float] = {}
            raw_prices = m.get("outcomePrices", "[]")
            try:
                prices = json.loads(raw_prices) if isinstance(raw_prices, str) else raw_prices
                for tid, p in zip(token_ids, prices):
                    seed_prices[tid] = float(p)
            except (json.JSONDecodeError, TypeError, ValueError):
                pass

            # event_id groups sibling outcome markets (e.g. all legs of an
            # ECB rate decision event).  Fall back to "" for standalone markets.
            # Try every field name Gamma has been observed to use.
            raw_event = m.get("event") or {}
            event_id = str(
                m.get("eventId")
                or m.get("event_id")
                or m.get("parentEventId")
                or m.get("groupId")
                or (raw_event.get("id") if isinstance(raw_event, dict) else None)
                or (raw_event.get("slug") if isinstance(raw_event, dict) else None)
                or ""
            )

            markets.append({
                "condition_id": m.get("conditionId") or m.get("condition_id", ""),
                "question": m.get("question", ""),
                "tokens": tokens,
                "seed_prices": seed_prices,
                "end_date_iso": m.get("endDateIso") or m.get("endDate", ""),
                "event_id": event_id,
            })

        # Stop if last page was smaller than a full page (exhausted)
        if len(batch) < GAMMA_PAGE_SIZE:
            break

        # Hard cap — only applied when MAX_MARKETS > 0
        if MAX_MARKETS and len(markets) >= MAX_MARKETS:
            break

        offset += GAMMA_PAGE_SIZE

    result = markets[:MAX_MARKETS] if MAX_MARKETS else markets

    # Drop markets whose end_date closed more than 1 hour ago.
    # Gamma's active=true filter is imperfect and often includes recently-resolved
    # markets, which bloat the tracking dict and waste REST poll budget.
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    live: list[dict] = []
    for m in result:
        end_iso = m.get("end_date_iso", "")
        if end_iso:
            try:
                end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                if end_dt < one_hour_ago:
                    continue  # expired — skip
            except (ValueError, AttributeError):
                pass  # unparseable date — keep the market
        live.append(m)

    n_expired = len(result) - len(live)

    # Diagnostic: count markets by event_id population, and log sample event objects.
    n_with_eid = sum(1 for m in live if m.get("event_id"))
    n_without_eid = len(live) - n_with_eid

    # Group by event_id and find multi-market events (these should NOT be binary-checked)
    _eid_counts: dict[str, int] = {}
    for m in live:
        eid = m.get("event_id", "")
        if eid:
            _eid_counts[eid] = _eid_counts.get(eid, 0) + 1
    n_multi_events  = sum(1 for c in _eid_counts.values() if c > 1)
    n_multi_markets = sum(c for c in _eid_counts.values() if c > 1)

    logger.info(
        "Fetched %d active CLOB markets from Gamma API (%d pages, page_size=%d)"
        " — dropped %d already-expired | with event_id: %d | without: %d"
        " | multi-outcome events: %d (%d outcome markets)",
        len(live), page, GAMMA_PAGE_SIZE, n_expired,
        n_with_eid, n_without_eid, n_multi_events, n_multi_markets,
    )
    return live


_sample_logged = False  # log one raw book response to verify parsing
_event_field_sample_logged = False  # log one raw Gamma market with event fields


def fetch_order_books(token_ids: list[str]) -> dict[str, dict]:
    """
    POST to /books in batches of BOOK_BATCH_SIZE.

    Request body : [{"token_id": "id1"}, {"token_id": "id2"}, ...]
    Response     : [{"asset_id": "id", "bids": [...], "asks": [...]}, ...]

    Actual sort order (confirmed from live API):
      bids: ascending by price  → bids[-1] = best bid (highest)
      asks: descending by price → asks[-1] = best ask (lowest / cheapest)

    Returns mapping  token_id -> book dict
    """
    global _sample_logged
    results: dict[str, dict] = {}

    for i in range(0, len(token_ids), BOOK_BATCH_SIZE):
        batch = token_ids[i : i + BOOK_BATCH_SIZE]
        try:
            resp = SESSION.post(
                f"{CLOB_BASE_URL}/books",
                json=[{"token_id": t} for t in batch],
                timeout=20,
            )
            resp.raise_for_status()
            data = resp.json()

            if not isinstance(data, list):
                logger.warning("Unexpected /books response type: %s", type(data))
                continue

            # Log one sample on the very first successful call to verify parsing
            if not _sample_logged and data:
                sample = data[0]
                bids = sample.get("bids", [])
                asks = sample.get("asks", [])
                best_bid_price = max((float(b["price"]) for b in bids), default=None) if bids else None
                best_ask_price = min((float(a["price"]) for a in asks), default=None) if asks else None
                logger.info(
                    "Sample /books response — asset_id=...%s  "
                    "bids[0]=%s  bids[-1]=%s  asks[0]=%s  asks[-1]=%s  "
                    "→ best_bid=%.4f  best_ask=%.4f",
                    sample.get("asset_id", "?")[-12:],
                    bids[0]["price"] if bids else "—",
                    bids[-1]["price"] if bids else "—",
                    asks[0]["price"] if asks else "—",
                    asks[-1]["price"] if asks else "—",
                    best_bid_price or 0,
                    best_ask_price or 0,
                )
                _sample_logged = True

            for book in data:
                tid = book.get("asset_id", "")
                if tid:
                    results[tid] = book

        except requests.HTTPError as exc:
            logger.warning("POST /books HTTP %s for batch starting ...%s", exc.response.status_code, batch[0][-12:])
        except Exception as exc:
            logger.warning("POST /books error: %s", exc)

    return results


def best_ask(book: dict) -> tuple:
    """
    Return (best_ask_price, size_at_best_ask) from a book dict.

    Returns (None, 0.0) when the ask side is empty — callers must treat
    None as "no live order book" and NOT fall back to any default price.
    """
    asks = book.get("asks", [])
    if not asks:
        return None, 0.0

    # Find the entry with the minimum price (cheapest offer to buy from)
    best = min(asks, key=lambda a: float(a["price"]) if isinstance(a, dict) else float(a[0]))
    if isinstance(best, dict):
        price = float(best.get("price", best.get("p", "inf")))
        size = float(best.get("size", best.get("s", 0)))
    elif isinstance(best, (list, tuple)):
        price, size = float(best[0]), float(best[1])
    else:
        return float("inf"), 0.0

    return price, size


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------

class PolymarketWSClient:
    """
    Connects to the Polymarket market WebSocket channel and pushes
    price-change / book events into `event_queue`.

    Events placed in the queue are raw dicts from the server.
    """

    def __init__(self, event_queue: queue.Queue):
        self._queue = event_queue
        self._subscribed_token_ids: set[str] = set()
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()

    # ── public API ──────────────────────────────────────────────────────────

    def start(self, token_ids: list[str]) -> None:
        """Connect and subscribe to token_ids."""
        with self._lock:
            self._subscribed_token_ids = set(token_ids)
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="ws-client")
        self._thread.start()
        logger.info("WebSocket client started (%d tokens)", len(token_ids))

    def update_subscriptions(self, token_ids: list[str]) -> None:
        """Replace the tracked token set and re-subscribe."""
        with self._lock:
            self._subscribed_token_ids = set(token_ids)
        if self._ws:
            self._send_subscribe()

    def stop(self) -> None:
        self._running = False
        if self._ws:
            self._ws.close()

    # ── internals ───────────────────────────────────────────────────────────

    def _run(self) -> None:
        while self._running:
            try:
                self._ws = websocket.WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as exc:
                logger.error("WebSocket run error: %s", exc)
            if self._running:
                logger.info("WebSocket reconnecting in 5 s …")
                time.sleep(5)

    def _send_subscribe(self) -> None:
        if self._ws is None:
            return
        with self._lock:
            ids = list(self._subscribed_token_ids)
        # Cap to MAX_WS_TOKENS — oversized frames cause immediate disconnection.
        # The stored set is already ordered by insertion (volume desc), so
        # slicing keeps the highest-volume tokens.
        if len(ids) > MAX_WS_TOKENS:
            ids = ids[:MAX_WS_TOKENS]
            logger.debug("WS subscription capped to %d tokens", MAX_WS_TOKENS)
        if not ids:
            return
        msg = json.dumps({"assets_ids": ids, "type": "market"})
        try:
            self._ws.send(msg)
            logger.debug("WS subscribed to %d tokens", len(ids))
        except Exception as exc:
            logger.warning("WS send error: %s", exc)

    def _on_open(self, ws) -> None:
        logger.info("WebSocket connected to %s", WS_URL)
        self._send_subscribe()

    def _on_message(self, ws, raw: str) -> None:
        try:
            data = json.loads(raw)
            # Server may send a list or a single object
            events = data if isinstance(data, list) else [data]
            for event in events:
                self._queue.put(event)
        except json.JSONDecodeError:
            logger.debug("Non-JSON WS message: %s", raw[:200])

    def _on_error(self, ws, error) -> None:
        logger.warning("WebSocket error: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        logger.info("WebSocket closed (code=%s): %s", code, msg)

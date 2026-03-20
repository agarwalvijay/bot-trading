"""
Data fetching layer for Kalshi.

Provides:
  - RSA-PSS auth header generation (required for order placement; market data is public)
  - REST helpers: market listing, batch price refresh via GET /markets?tickers=
  - WebSocket client streaming real-time ticker updates into a thread-safe queue
"""

import base64
import json
import logging
import queue
import threading
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import requests

try:
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
    _CRYPTO_AVAILABLE = True
except ImportError:
    _CRYPTO_AVAILABLE = False
    logging.getLogger(__name__).warning(
        "cryptography package not installed — authenticated endpoints (order placement) unavailable. "
        "Run: pip install cryptography"
    )

import websocket  # websocket-client

from config import (
    BOOK_BATCH_SIZE,
    KALSHI_BASE_URL,
    KALSHI_WS_URL,
    KALSHI_API_KEY_ID,
    KALSHI_PRIVATE_KEY_PATH,
    MARKET_PAGE_SIZE,
    MAX_MARKETS,
    MAX_WS_MARKETS,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# RSA-PSS auth (required for order placement — not for market data reads)
# ---------------------------------------------------------------------------

_private_key = None


def _load_private_key():
    global _private_key
    if _private_key is not None:
        return _private_key
    if not _CRYPTO_AVAILABLE:
        return None
    try:
        with open(KALSHI_PRIVATE_KEY_PATH, "rb") as f:
            _private_key = serialization.load_pem_private_key(f.read(), password=None)
        logger.info("Kalshi private key loaded from %s", KALSHI_PRIVATE_KEY_PATH)
        return _private_key
    except FileNotFoundError:
        logger.debug("No private key at %s — authenticated endpoints disabled", KALSHI_PRIVATE_KEY_PATH)
        return None
    except Exception as exc:
        logger.warning("Failed to load Kalshi private key: %s", exc)
        return None


def _auth_headers(method: str, path: str) -> dict:
    """
    Return KALSHI-ACCESS-* headers for an authenticated request.
    Signs: {timestamp_ms}{METHOD_UPPER}{path_without_query}
    """
    key = _load_private_key()
    if not key or not KALSHI_API_KEY_ID:
        return {}
    ts = str(int(time.time() * 1000))
    msg = (ts + method.upper() + path).encode("utf-8")
    sig = key.sign(
        msg,
        asym_padding.PSS(
            mgf=asym_padding.MGF1(hashes.SHA256()),
            salt_length=asym_padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return {
        "KALSHI-ACCESS-KEY":       KALSHI_API_KEY_ID,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


# ---------------------------------------------------------------------------
# REST helpers
# ---------------------------------------------------------------------------

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


def _get(path: str, params: Optional[dict] = None, auth: bool = False, retries: int = 3) -> Any:
    """GET with simple exponential-backoff retry."""
    url = f"{KALSHI_BASE_URL}{path}"
    headers = _auth_headers("GET", path) if auth else {}
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, headers=headers, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            logger.warning("HTTP %s on GET %s (attempt %d)", exc.response.status_code, path, attempt + 1)
            if exc.response.status_code in (429, 503):
                time.sleep(2 ** attempt)
            else:
                raise
        except requests.RequestException as exc:
            logger.warning("Request error on GET %s: %s (attempt %d)", path, exc, attempt + 1)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to GET {path} after {retries} attempts")


def _parse_price(val) -> Optional[float]:
    """Parse a Kalshi price field (string or numeric) to float, or None."""
    try:
        f = float(val)
        return f if f > 0 else None
    except (TypeError, ValueError):
        return None


def fetch_active_markets() -> list[dict]:
    """
    Return all open Kalshi markets via cursor pagination.

    Each returned dict contains:
        ticker            – market identifier  (e.g. "FED-25MAR-T4.50")
        event_ticker      – parent event  (groups sibling outcome markets)
        title             – human-readable question
        close_time        – ISO-8601 close datetime
        seed_yes_ask      – best YES ask price from listing (primes live_prices)
        seed_no_ask       – best NO ask price
        seed_yes_ask_size – contracts available at best YES ask
        seed_no_ask_size  – contracts available at best NO ask
    """
    markets: list[dict] = []
    cursor: Optional[str] = None
    page = 0
    last_found_page = 0  # track when we last found an active market

    now_ts = int(time.time())
    while True:
        params: dict = {
            "status":      "open",
            "limit":       MARKET_PAGE_SIZE,
            "mve_filter":  "exclude",       # skip sports parlay combinatorial markets
            "min_close_ts": now_ts,         # skip already-closed markets server-side
        }
        if cursor:
            params["cursor"] = cursor

        data = _get("/markets", params=params)
        batch = data.get("markets", [])
        page += 1
        logger.info("Kalshi page %d: %d markets fetched so far=%d", page, len(batch), len(markets))

        if not batch:
            break

        for m in batch:
            ticker = m.get("ticker", "")
            if not ticker:
                continue

            # Skip provisional markets and markets with no 24h volume.
            if m.get("is_provisional"):
                continue
            if float(m.get("volume_24h_fp") or 0) <= 0:
                continue

            yes_ask      = _parse_price(m.get("yes_ask_dollars") or m.get("yes_ask"))
            no_ask       = _parse_price(m.get("no_ask_dollars")  or m.get("no_ask"))
            yes_ask_size = float(m.get("yes_ask_size_fp") or m.get("yes_ask_size") or 0)
            no_ask_size  = float(m.get("no_ask_size_fp")  or m.get("no_ask_size")  or 0)

            last_found_page = page
            markets.append({
                "ticker":            ticker,
                "event_ticker":      m.get("event_ticker", ""),
                "title":             (m.get("title") or m.get("yes_sub_title") or ticker),
                "close_time":        m.get("close_time") or m.get("expiration_time") or "",
                "seed_yes_ask":      yes_ask,
                "seed_no_ask":       no_ask,
                "seed_yes_ask_size": yes_ask_size,
                "seed_no_ask_size":  no_ask_size,
                "volume_24h":        float(m.get("volume_24h_fp") or 0),
            })

        cursor = data.get("cursor")
        if not cursor:
            break

        if MAX_MARKETS and len(markets) >= MAX_MARKETS:
            break

        if MAX_MARKETS and len(markets) >= MAX_MARKETS:
            break

        time.sleep(0.1)  # 100ms between listing pages to stay within rate limits

    # Sort by 24h volume descending so MAX_MARKETS cap keeps the most liquid
    markets.sort(key=lambda m: m["volume_24h"], reverse=True)
    result = markets[:MAX_MARKETS] if MAX_MARKETS else markets

    # Drop markets that closed more than 1 hour ago
    one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
    live: list[dict] = []
    for m in result:
        ct = m.get("close_time", "")
        if ct:
            try:
                end_dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                if end_dt.tzinfo is None:
                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                if end_dt < one_hour_ago:
                    continue
            except (ValueError, AttributeError):
                pass
        live.append(m)

    n_expired = len(result) - len(live)

    # Diagnostic: event grouping stats
    _eid_counts: dict[str, int] = {}
    for m in live:
        eid = m.get("event_ticker", "")
        if eid:
            _eid_counts[eid] = _eid_counts.get(eid, 0) + 1
    n_multi_events  = sum(1 for c in _eid_counts.values() if c > 1)
    n_multi_markets = sum(c for c in _eid_counts.values() if c > 1)

    logger.info(
        "Fetched %d open Kalshi markets (%d pages) — dropped %d expired"
        " | multi-outcome events: %d (%d markets)",
        len(live), page, n_expired, n_multi_events, n_multi_markets,
    )
    return live


def fetch_event_market_count(event_ticker: str) -> int:
    """
    Return the total number of open markets for an event, ignoring the
    volume filter used by the main scanner.  Used to verify that we have
    all outcomes before treating a multi-outcome group as arb-eligible.
    """
    try:
        data = _get("/markets", params={
            "event_ticker": event_ticker,
            "status":       "open",
            "limit":        200,
        })
        markets = data.get("markets", [])
        # If cursor present the event has >200 outcomes — return a large sentinel
        if data.get("cursor"):
            return 999
        return len(markets)
    except Exception as exc:
        logger.warning("fetch_event_market_count(%s) failed: %s", event_ticker, exc)
        return 0


def fetch_market_prices(tickers: list[str]) -> dict[str, dict]:
    """
    Batch-refresh best-ask prices for a list of tickers.

    Uses GET /markets?tickers=t1,t2,... which returns current top-of-book
    prices and sizes without needing individual order book calls.

    Returns: {ticker: {yes_ask, yes_ask_size, no_ask, no_ask_size}}
    Both ask prices may be None when there is no live offer on that side.
    """
    results: dict[str, dict] = {}

    for i in range(0, len(tickers), BOOK_BATCH_SIZE):
        batch = tickers[i: i + BOOK_BATCH_SIZE]
        try:
            data = _get("/markets", params={"tickers": ",".join(batch), "limit": len(batch)})
            for m in data.get("markets", []):
                ticker = m.get("ticker", "")
                if not ticker:
                    continue
                results[ticker] = {
                    "yes_ask":      _parse_price(m.get("yes_ask_dollars") or m.get("yes_ask")),
                    "yes_ask_size": float(m.get("yes_ask_size_fp") or m.get("yes_ask_size") or 0),
                    "no_ask":       _parse_price(m.get("no_ask_dollars")  or m.get("no_ask")),
                    "no_ask_size":  float(m.get("no_ask_size_fp")  or m.get("no_ask_size")  or 0),
                }
        except Exception as exc:
            logger.warning("fetch_market_prices batch error: %s", exc)
        time.sleep(0.2)  # 200 ms between batches — ~5 req/s, within Kalshi rate limits

    return results


# ---------------------------------------------------------------------------
# WebSocket client
# ---------------------------------------------------------------------------

class KalshiWSClient:
    """
    Connects to the Kalshi market WebSocket and pushes ticker update events
    into `event_queue` for consumption by arb_bot.py.

    Subscribes to the `ticker` channel which delivers yes_ask / no_ask
    updates in real-time as the order book changes.
    """

    def __init__(self, event_queue: queue.Queue):
        self._queue = event_queue
        self._subscribed_tickers: set[str] = set()
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()
        self._msg_id = 1

    # ── public API ──────────────────────────────────────────────────────────

    def start(self, tickers: list[str]) -> None:
        with self._lock:
            self._subscribed_tickers = set(tickers)
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="ws-client")
        self._thread.start()
        logger.info("Kalshi WS client started (%d tickers)", len(tickers))

    def update_subscriptions(self, tickers: list[str]) -> None:
        with self._lock:
            self._subscribed_tickers = set(tickers)
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
                # Auth headers must be on the HTTP upgrade request itself.
                # Sign with path "/trade-api/ws/v2" (no host, no query string).
                headers = _auth_headers("GET", "/trade-api/ws/v2")
                if not headers:
                    logger.warning(
                        "Kalshi WS: no API credentials configured — "
                        "set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH in .env"
                    )
                self._ws = websocket.WebSocketApp(
                    KALSHI_WS_URL,
                    header=headers,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as exc:
                logger.error("Kalshi WS run error: %s", exc)
            if self._running:
                logger.info("Kalshi WS reconnecting in 5s…")
                time.sleep(5)

    def _send_subscribe(self) -> None:
        if self._ws is None:
            return
        with self._lock:
            tickers = list(self._subscribed_tickers)
        if len(tickers) > MAX_WS_MARKETS:
            tickers = tickers[:MAX_WS_MARKETS]
            logger.debug("WS subscription capped to %d tickers", MAX_WS_MARKETS)
        if not tickers:
            return
        msg = json.dumps({
            "id":  self._msg_id,
            "cmd": "subscribe",
            "params": {
                "channels":       ["ticker"],
                "market_tickers": tickers,
            },
        })
        self._msg_id += 1
        try:
            self._ws.send(msg)
            logger.debug("Kalshi WS subscribed to %d tickers", len(tickers))
        except Exception as exc:
            logger.warning("Kalshi WS send error: %s", exc)

    def _on_open(self, ws) -> None:
        logger.info("Kalshi WS connected to %s", KALSHI_WS_URL)
        self._send_subscribe()

    def _on_message(self, ws, raw: str) -> None:
        try:
            data = json.loads(raw)
            self._queue.put(data)
        except json.JSONDecodeError:
            logger.debug("Non-JSON WS message: %s", raw[:200])

    def _on_error(self, ws, error) -> None:
        logger.warning("Kalshi WS error: %s", error)

    def _on_close(self, ws, code, msg) -> None:
        logger.info("Kalshi WS closed (code=%s): %s", code, msg)

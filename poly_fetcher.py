"""
Polymarket public data helpers for cross-exchange comparison.

This module is intentionally read-only:
- Gamma API for active market metadata.
- CLOB /books for top-of-book asks.
"""

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests

from config import (
    BOOK_BATCH_SIZE,
    MAX_MARKETS,
    POLY_CLOB_BASE_URL,
    POLY_GAMMA_BASE_URL,
    POLY_GAMMA_MARKETS_PATH,
    POLY_GAMMA_PAGE_SIZE,
    POLY_GAMMA_SORT_FIELD,
)

logger = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


def _get(url: str, params: Optional[dict] = None, retries: int = 3) -> Any:
    """GET with simple retry/backoff."""
    for attempt in range(retries):
        try:
            resp = SESSION.get(url, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else 0
            logger.warning("HTTP %s on GET %s (attempt %d)", code, url, attempt + 1)
            if code in (429, 503):
                time.sleep(2 ** attempt)
            else:
                raise
        except requests.RequestException as exc:
            logger.warning("Request error on GET %s: %s (attempt %d)", url, exc, attempt + 1)
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed GET {url} after {retries} attempts")


def fetch_active_markets() -> list[dict]:
    """
    Return active CLOB markets from Gamma.

    Each row includes:
      condition_id, question, tokens[{token_id,outcome}], end_date_iso, event_id
    """
    markets: list[dict] = []
    offset = 0
    page = 0

    while True:
        data = _get(
            f"{POLY_GAMMA_BASE_URL}{POLY_GAMMA_MARKETS_PATH}",
            params={
                "active": "true",
                "closed": "false",
                "archived": "false",
                "limit": POLY_GAMMA_PAGE_SIZE,
                "offset": offset,
                "order": POLY_GAMMA_SORT_FIELD,
                "ascending": "false",
            },
        )
        batch = data if isinstance(data, list) else data.get("markets", [])
        page += 1
        if not batch:
            break

        for m in batch:
            if not m.get("enableOrderBook") or not m.get("acceptingOrders"):
                continue

            raw_token_ids = m.get("clobTokenIds", "[]")
            raw_outcomes = m.get("outcomes", "[]")
            try:
                token_ids = json.loads(raw_token_ids) if isinstance(raw_token_ids, str) else raw_token_ids
                outcomes = json.loads(raw_outcomes) if isinstance(raw_outcomes, str) else raw_outcomes
            except (json.JSONDecodeError, TypeError):
                continue

            if len(token_ids) < 2:
                continue

            tokens = [{"token_id": tid, "outcome": out} for tid, out in zip(token_ids, outcomes)]

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
                "end_date_iso": m.get("endDateIso") or m.get("endDate", ""),
                "event_id": event_id,
            })

        if len(batch) < POLY_GAMMA_PAGE_SIZE:
            break
        if MAX_MARKETS and len(markets) >= MAX_MARKETS:
            break
        offset += POLY_GAMMA_PAGE_SIZE

    result = markets[:MAX_MARKETS] if MAX_MARKETS else markets

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
                    continue
            except (ValueError, AttributeError):
                pass
        live.append(m)

    logger.info("Polymarket Gamma: %d active markets (pages=%d)", len(live), page)
    return live


def fetch_order_books(token_ids: list[str]) -> dict[str, dict]:
    """
    Return mapping token_id -> raw CLOB book dict from POST /books.
    """
    results: dict[str, dict] = {}
    for i in range(0, len(token_ids), BOOK_BATCH_SIZE):
        batch = token_ids[i:i + BOOK_BATCH_SIZE]
        try:
            resp = SESSION.post(
                f"{POLY_CLOB_BASE_URL}/books",
                json=[{"token_id": t} for t in batch],
                timeout=25,
            )
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list):
                continue
            for book in data:
                tid = book.get("asset_id", "")
                if tid:
                    results[tid] = book
        except Exception as exc:
            logger.warning("Polymarket /books batch failed (%d tokens): %s", len(batch), exc)
    return results


def best_ask(book: dict) -> tuple[Optional[float], float]:
    """Return (price, size) for cheapest ask; (None,0) if ask side empty."""
    asks = book.get("asks", [])
    if not asks:
        return None, 0.0
    best = min(asks, key=lambda a: float(a["price"]) if isinstance(a, dict) else float(a[0]))
    if isinstance(best, dict):
        return float(best.get("price")), float(best.get("size", 0))
    if isinstance(best, (list, tuple)) and len(best) >= 2:
        return float(best[0]), float(best[1])
    return None, 0.0

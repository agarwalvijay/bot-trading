"""
Helpers for deriving a canonical market-level identifier.
"""

import json
from typing import Any
from typing import Optional


def canonical_market_key(
    ticker: Optional[str],
    event_ticker: Optional[str] = "",
    outcome_tickers: Any = None,
) -> str:
    """
    Return a stable key used for market-level authorization.

    - Multi-outcome opportunities map to event_ticker.
    - Binary opportunities map to ticker.
    """
    tkr = (ticker or "").strip().upper()
    evt = (event_ticker or "").strip().upper()

    outcomes = outcome_tickers
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except Exception:
            outcomes = []
    if not isinstance(outcomes, list):
        outcomes = []

    if len(outcomes) > 1:
        return evt or tkr
    return tkr or evt

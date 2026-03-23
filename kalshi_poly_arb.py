"""
Cross-venue arb scanner: Kalshi vs Polymarket (binary YES/NO markets only).

Scope:
- Detect similar market questions between venues.
- Compare top-of-book asks and surface potential arb edges.
- No execution/trading path.

Run:
  python3 kalshi_poly_arb.py
  python3 kalshi_poly_arb.py --min-edge 0.01 --min-sim 0.55 --max-results 100
"""

import argparse
import logging
import re
from collections import Counter
from typing import Optional

from config import POLY_TAKER_FEE_RATE, TAKER_FEE_COEFF
from fetcher import fetch_active_markets as fetch_kalshi_markets
from fetcher import fetch_market_prices
from poly_fetcher import best_ask as poly_best_ask
from poly_fetcher import fetch_active_markets as fetch_poly_markets
from poly_fetcher import fetch_order_books as fetch_poly_books

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("kalshi_poly_arb")

_STOPWORDS = {
    "the", "a", "an", "and", "or", "of", "to", "in", "on", "for", "with",
    "will", "be", "at", "vs", "game", "winner", "match", "price", "up", "down",
}


def _norm_tokens(text: str) -> set[str]:
    t = re.sub(r"[^a-z0-9 ]+", " ", (text or "").lower())
    toks = [x for x in t.split() if len(x) > 1 and x not in _STOPWORDS]
    return set(toks)


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    inter = len(a & b)
    uni = len(a | b)
    return inter / uni if uni else 0.0


def _kalshi_fee(price: float) -> float:
    return TAKER_FEE_COEFF * price * (1.0 - price)


def _poly_fee(price: float) -> float:
    return POLY_TAKER_FEE_RATE * price


def _extract_poly_yes_no_prices(market: dict, books_by_tid: dict[str, dict]) -> Optional[dict]:
    """
    Return {"yes_ask": x, "no_ask": y} from a Polymarket binary market.
    Requires explicit YES and NO outcomes.
    """
    tokens = market.get("tokens", [])
    if len(tokens) != 2:
        return None

    yes_tid = ""
    no_tid = ""
    for t in tokens:
        outcome = str(t.get("outcome", "")).strip().lower()
        tid = str(t.get("token_id", ""))
        if outcome == "yes":
            yes_tid = tid
        elif outcome == "no":
            no_tid = tid
    if not yes_tid or not no_tid:
        return None

    yes_book = books_by_tid.get(yes_tid, {})
    no_book = books_by_tid.get(no_tid, {})
    yes_ask, yes_size = poly_best_ask(yes_book)
    no_ask, no_size = poly_best_ask(no_book)
    if yes_ask is None or no_ask is None:
        return None
    return {
        "yes_ask": float(yes_ask),
        "yes_size": float(yes_size),
        "no_ask": float(no_ask),
        "no_size": float(no_size),
    }


def load_kalshi_binary() -> list[dict]:
    markets = fetch_kalshi_markets()
    by_event = Counter(m.get("event_ticker") or m.get("ticker") for m in markets)
    binaries = []
    for m in markets:
        eid = m.get("event_ticker") or m.get("ticker")
        if by_event[eid] == 1:
            binaries.append(m)

    tickers = [m["ticker"] for m in binaries if m.get("ticker")]
    prices = fetch_market_prices(tickers)

    rows = []
    for m in binaries:
        t = m["ticker"]
        p = prices.get(t, {})
        yes_ask = p.get("yes_ask")
        no_ask = p.get("no_ask")
        if yes_ask is None or no_ask is None:
            continue
        rows.append({
            "id": t,
            "question": m.get("title", t),
            "yes_ask": float(yes_ask),
            "no_ask": float(no_ask),
            "yes_size": float(p.get("yes_ask_size") or 0.0),
            "no_size": float(p.get("no_ask_size") or 0.0),
            "url": "",
        })
    logger.info("Kalshi binaries loaded: %d", len(rows))
    return rows


def load_polymarket_binary() -> list[dict]:
    markets = fetch_poly_markets()
    binaries = [m for m in markets if len(m.get("tokens", [])) == 2]

    token_ids = []
    for m in binaries:
        for t in m.get("tokens", []):
            tid = t.get("token_id")
            if tid:
                token_ids.append(tid)
    books = fetch_poly_books(token_ids)

    rows = []
    for m in binaries:
        px = _extract_poly_yes_no_prices(m, books)
        if not px:
            continue
        rows.append({
            "id": m.get("condition_id"),
            "question": m.get("question", ""),
            "yes_ask": px["yes_ask"],
            "no_ask": px["no_ask"],
            "yes_size": px["yes_size"],
            "no_size": px["no_size"],
            "url": "",
        })
    logger.info("Polymarket binaries loaded: %d", len(rows))
    return rows


def best_poly_match(k: dict, poly_rows: list[dict], min_sim: float) -> Optional[tuple[dict, float]]:
    kt = _norm_tokens(k["question"])
    best = None
    best_sim = 0.0
    for p in poly_rows:
        sim = _jaccard(kt, _norm_tokens(p["question"]))
        if sim > best_sim:
            best = p
            best_sim = sim
    if best and best_sim >= min_sim:
        return best, best_sim
    return None


def compute_cross_arb(k: dict, p: dict) -> list[dict]:
    """
    Two cross-venue directions:
    1) buy YES on Kalshi + buy NO on Poly
    2) buy NO on Kalshi + buy YES on Poly
    """
    out = []

    sum1 = k["yes_ask"] + p["no_ask"]
    fees1 = _kalshi_fee(k["yes_ask"]) + _poly_fee(p["no_ask"])
    net1 = 1.0 - sum1 - fees1
    out.append({
        "direction": "KALSHI_YES + POLY_NO",
        "sum_asks": sum1,
        "fees": fees1,
        "net_edge": net1,
    })

    sum2 = k["no_ask"] + p["yes_ask"]
    fees2 = _kalshi_fee(k["no_ask"]) + _poly_fee(p["yes_ask"])
    net2 = 1.0 - sum2 - fees2
    out.append({
        "direction": "KALSHI_NO + POLY_YES",
        "sum_asks": sum2,
        "fees": fees2,
        "net_edge": net2,
    })
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Kalshi vs Polymarket binary-market arb scanner")
    ap.add_argument("--min-sim", type=float, default=0.50, help="minimum text similarity for market matching")
    ap.add_argument("--min-edge", type=float, default=0.005, help="minimum net edge after fees")
    ap.add_argument("--max-results", type=int, default=50, help="max rows to print")
    args = ap.parse_args()

    kalshi_rows = load_kalshi_binary()
    poly_rows = load_polymarket_binary()
    if not kalshi_rows or not poly_rows:
        logger.info("No comparable markets found.")
        return

    findings = []
    for k in kalshi_rows:
        m = best_poly_match(k, poly_rows, args.min_sim)
        if not m:
            continue
        p, sim = m
        for cand in compute_cross_arb(k, p):
            if cand["net_edge"] >= args.min_edge:
                findings.append({
                    "net_edge": cand["net_edge"],
                    "direction": cand["direction"],
                    "similarity": sim,
                    "kalshi_q": k["question"],
                    "poly_q": p["question"],
                    "sum_asks": cand["sum_asks"],
                    "fees": cand["fees"],
                    "kalshi_yes": k["yes_ask"],
                    "kalshi_no": k["no_ask"],
                    "poly_yes": p["yes_ask"],
                    "poly_no": p["no_ask"],
                })

    findings.sort(key=lambda x: x["net_edge"], reverse=True)
    if not findings:
        print("No cross-venue arb candidates above threshold.")
        return

    print(f"Found {len(findings)} candidates (showing top {min(len(findings), args.max_results)}):")
    for i, f in enumerate(findings[:args.max_results], start=1):
        print(
            f"{i:03d}  edge={f['net_edge']*100:6.2f}%  sim={f['similarity']:.2f}  "
            f"{f['direction']}  sum={f['sum_asks']:.4f} fees={f['fees']:.4f}"
        )
        print(f"      K: {f['kalshi_q']}")
        print(f"      P: {f['poly_q']}")
        print(
            f"      px K(y/n)=({f['kalshi_yes']:.4f}/{f['kalshi_no']:.4f}) "
            f"P(y/n)=({f['poly_yes']:.4f}/{f['poly_no']:.4f})"
        )


if __name__ == "__main__":
    main()

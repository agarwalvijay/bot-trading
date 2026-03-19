"""
All tunable parameters for the Polymarket arbitrage bot.
Values are loaded from environment variables (via .env) with sane defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── API endpoints ────────────────────────────────────────────────────────────
CLOB_BASE_URL = os.getenv("CLOB_BASE_URL", "https://clob.polymarket.com")
GAMMA_BASE_URL = os.getenv("GAMMA_BASE_URL", "https://gamma-api.polymarket.com")
WS_URL = os.getenv("WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")

# ── Scanning behaviour ───────────────────────────────────────────────────────
# How often (seconds) to re-fetch market list from Gamma
MARKET_REFRESH_INTERVAL = float(os.getenv("MARKET_REFRESH_INTERVAL", "300"))

# How often (seconds) to poll REST order-books for each active market
REST_POLL_INTERVAL = float(os.getenv("REST_POLL_INTERVAL", "10"))

# Maximum markets to track simultaneously (0 = no cap, fetch all)
MAX_MARKETS = int(os.getenv("MAX_MARKETS", "0"))

# Per-page limit when paginating the Gamma market list
GAMMA_PAGE_SIZE = int(os.getenv("GAMMA_PAGE_SIZE", "500"))

# Number of markets per batch when fetching order books via REST
BOOK_BATCH_SIZE = int(os.getenv("BOOK_BATCH_SIZE", "100"))

# Sort field for Gamma market list — fetches most active markets first so that
# if MAX_MARKETS caps the list, the highest-volume markets are prioritised
GAMMA_SORT_FIELD = os.getenv("GAMMA_SORT_FIELD", "volume24hr")

# Maximum token IDs to subscribe to via WebSocket.
# Each subscription message is a single JSON frame; ~55k tokens crashes the
# connection. Keep this well under ~2000 tokens (~1000 binary markets).
MAX_WS_TOKENS = int(os.getenv("MAX_WS_TOKENS", "1000"))

# Markets that received a WS price event within this many seconds are considered
# "WS-fresh" and are skipped by the REST poller.  This lets REST cycles focus
# budget on cold markets not covered by the WebSocket feed.
WS_FRESHNESS_SECS = int(os.getenv("WS_FRESHNESS_SECS", "60"))

# Maximum markets to REST-poll per scan cycle.
# At 100 tokens/batch that's MAX_REST_MARKETS*2/100 HTTP calls per cycle.
# 2500 markets → ~50 calls/cycle, comfortably within a 10 s interval.
MAX_REST_MARKETS = int(os.getenv("MAX_REST_MARKETS", "2500"))

# ── Arbitrage filters ────────────────────────────────────────────────────────
# Fee rate applied per leg (Polymarket charges ~2 % taker fee)
FEE_RATE = float(os.getenv("FEE_RATE", "0.02"))

# Minimum net profit (after fees) as a fraction to surface an opportunity
# e.g. 0.005 = 0.5 cents per dollar risked
MIN_NET_PROFIT = float(os.getenv("MIN_NET_PROFIT", "0.005"))

# Minimum liquidity (ask size) required on each leg so the opportunity is
# actually fillable (in USDC units)
MIN_LEG_SIZE = float(os.getenv("MIN_LEG_SIZE", "50.0"))

# Minimum ask price per leg. Prices below this indicate a near-certain outcome
# (event already resolved or one side is ~worthless) — not a real inefficiency.
MIN_LEG_PRICE = float(os.getenv("MIN_LEG_PRICE", "0.01"))

# Near-miss band: underrounds where fees consume the gross profit, but a maker
# order (with fee rebate) could flip to profitable.
# Logged separately; never trigger execution.
NEAR_MISS_LOWER = float(os.getenv("NEAR_MISS_LOWER", "-0.02"))  # -2 %

# Net profit threshold above which end_date_iso is logged (high-profit sanity check)
HIGH_PROFIT_THRESHOLD = float(os.getenv("HIGH_PROFIT_THRESHOLD", "0.05"))  # 5 %

# ── WebSocket alert deduplication ────────────────────────────────────────────
# Minimum seconds between alerts for the same condition_id
WS_ALERT_COOLDOWN_SECS = int(os.getenv("WS_ALERT_COOLDOWN_SECS", "60"))

# Minimum net_profit improvement (absolute) required to re-alert after cooldown
WS_MIN_PROFIT_IMPROVEMENT = float(os.getenv("WS_MIN_PROFIT_IMPROVEMENT", "0.005"))  # 0.5 %

# ── Expiry alerting ───────────────────────────────────────────────────────────
# Markets closing in less than this many minutes are "expiring" — suppress from
# normal console output (too late to act without execution wired up)
EXPIRING_SOON_MINS = int(os.getenv("EXPIRING_SOON_MINS", "30"))

# For EXPIRING_ACTIONABLE: only surface if net_profit exceeds this threshold
EXPIRING_ACTIONABLE_MIN_PROFIT = float(os.getenv("EXPIRING_ACTIONABLE_MIN_PROFIT", "0.01"))  # 1 %

# ── Database ─────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "arb_opportunities.db")

# ── Alerting ─────────────────────────────────────────────────────────────────
# Telegram bot token and chat id (leave blank to disable Telegram alerts)
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Minimum net profit to fire a Telegram alert (higher threshold reduces noise)
TELEGRAM_MIN_PROFIT = float(os.getenv("TELEGRAM_MIN_PROFIT", "0.01"))

# ── Phase 2 stub: order execution credentials ────────────────────────────────
# L2 auth headers required for placing / cancelling orders
POLY_ADDRESS = os.getenv("POLY_ADDRESS", "")
POLY_API_KEY = os.getenv("POLY_API_KEY", "")
POLY_API_SECRET = os.getenv("POLY_API_SECRET", "")
POLY_API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "")
PRIVATE_KEY = os.getenv("PRIVATE_KEY", "")   # Only needed for L1 key derivation

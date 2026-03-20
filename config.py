"""
All tunable parameters for the Kalshi arbitrage bot.
Values are loaded from environment variables (via .env) with sane defaults.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── API endpoints ─────────────────────────────────────────────────────────────
KALSHI_BASE_URL = os.getenv("KALSHI_BASE_URL", "https://api.elections.kalshi.com/trade-api/v2")
KALSHI_WS_URL   = os.getenv("KALSHI_WS_URL",   "wss://api.elections.kalshi.com/trade-api/ws/v2")

# ── Kalshi API credentials ────────────────────────────────────────────────────
# Generate via Account Settings → API Keys on kalshi.com
# The private key PEM file is generated locally — Kalshi never stores it.
KALSHI_API_KEY_ID       = os.getenv("KALSHI_API_KEY_ID", "")
KALSHI_PRIVATE_KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "kalshi_private_key.pem")

# Demo API credentials (separate account at demo.kalshi.co).
# Fall back to prod credentials if not set — but demo will 401 with prod keys.
DEMO_API_KEY_ID       = os.getenv("DEMO_API_KEY_ID",       KALSHI_API_KEY_ID)
DEMO_PRIVATE_KEY_PATH = os.getenv("DEMO_PRIVATE_KEY_PATH", KALSHI_PRIVATE_KEY_PATH)

# ── Scanning behaviour ────────────────────────────────────────────────────────
# How often (seconds) to re-fetch the full market list
MARKET_REFRESH_INTERVAL = float(os.getenv("MARKET_REFRESH_INTERVAL", "300"))

# How often (seconds) to run a REST price-refresh cycle
REST_POLL_INTERVAL = float(os.getenv("REST_POLL_INTERVAL", "10"))

# Maximum markets to track simultaneously (0 = no cap)
MAX_MARKETS = int(os.getenv("MAX_MARKETS", "0"))

# Per-page limit when paginating the Kalshi market list (max 1000)
MARKET_PAGE_SIZE = int(os.getenv("MARKET_PAGE_SIZE", "1000"))

# Number of tickers per batch when refreshing prices via GET /markets?tickers=
BOOK_BATCH_SIZE = int(os.getenv("BOOK_BATCH_SIZE", "50"))

# Maximum market tickers to subscribe to via WebSocket
MAX_WS_MARKETS = int(os.getenv("MAX_WS_MARKETS", "500"))

# Markets that received a WS update within this many seconds are "WS-fresh"
# and are skipped by the REST poller
WS_FRESHNESS_SECS = int(os.getenv("WS_FRESHNESS_SECS", "60"))

# Maximum markets to REST-poll per scan cycle
MAX_REST_MARKETS = int(os.getenv("MAX_REST_MARKETS", "2500"))

# Near-term discovery scan: fetch markets closing within this many hours,
# run every NEAR_TERM_SCAN_INTERVAL seconds.  Catches new sports events
# without waiting for the full 6-hour rescan.
NEAR_TERM_SCAN_INTERVAL  = int(os.getenv("NEAR_TERM_SCAN_INTERVAL",  "900"))   # 15 min
NEAR_TERM_HORIZON_HOURS  = float(os.getenv("NEAR_TERM_HORIZON_HOURS", "12"))

# ── Fee model (Kalshi parabolic taker fee) ────────────────────────────────────
# Taker fee per contract = TAKER_FEE_COEFF × price × (1 − price)
# At $0.50 this equals 1.75¢/contract; approaches 0 near $0.01 or $0.99.
TAKER_FEE_COEFF = float(os.getenv("TAKER_FEE_COEFF", "0.07"))

# ── Arbitrage filters ─────────────────────────────────────────────────────────
# Minimum net profit (after fees) as a fraction to surface an opportunity
MIN_NET_PROFIT = float(os.getenv("MIN_NET_PROFIT", "0.005"))

# Minimum liquidity (contracts × price) required on each leg
MIN_LEG_SIZE = float(os.getenv("MIN_LEG_SIZE", "50.0"))

# Minimum ask price per leg — below this the outcome is near-certain
MIN_LEG_PRICE = float(os.getenv("MIN_LEG_PRICE", "0.01"))

# Near-miss band: underrounds where fees consume the profit
NEAR_MISS_LOWER = float(os.getenv("NEAR_MISS_LOWER", "-0.02"))

# For multi-outcome events with 5+ outcomes, require the sum of YES asks to
# be at least this high before treating the group as exhaustive.
# Below this threshold the "missing" probability lives in unlisted outcomes
# (e.g. song charts, open-ended rankings) — not a real arb.
# Rule of thumb: 0.85 means at most 15% probability in unlisted outcomes.
MIN_MULTI_OUTCOME_SUM = float(os.getenv("MIN_MULTI_OUTCOME_SUM", "0.85"))

# Net profit threshold above which close_time is logged for sanity checking
HIGH_PROFIT_THRESHOLD = float(os.getenv("HIGH_PROFIT_THRESHOLD", "0.05"))

# ── Alert deduplication ───────────────────────────────────────────────────────
WS_ALERT_COOLDOWN_SECS    = int(os.getenv("WS_ALERT_COOLDOWN_SECS", "60"))
WS_MIN_PROFIT_IMPROVEMENT = float(os.getenv("WS_MIN_PROFIT_IMPROVEMENT", "0.005"))

# ── Expiry alerting ───────────────────────────────────────────────────────────
EXPIRING_SOON_MINS             = int(os.getenv("EXPIRING_SOON_MINS", "30"))
EXPIRING_ACTIONABLE_MIN_PROFIT = float(os.getenv("EXPIRING_ACTIONABLE_MIN_PROFIT", "0.01"))

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", "arb_opportunities.db")

# ── Trading execution ─────────────────────────────────────────────────────────
# Master switch — set true only when ready to place real/demo orders.
TRADING_ENABLED = os.getenv("TRADING_ENABLED", "false").lower() == "true"

# When true, orders go to demo-api.kalshi.co (paper money, no real risk).
# Always start here. Switch to false only after demo validation.
DEMO_MODE = os.getenv("DEMO_MODE", "true").lower() == "true"

# Demo API base URL (only used for order placement when DEMO_MODE=true)
DEMO_BASE_URL = os.getenv("DEMO_BASE_URL", "https://demo-api.kalshi.co/trade-api/v2")

# Hard cap on contracts per leg per trade
MAX_CONTRACTS_PER_TRADE = int(os.getenv("MAX_CONTRACTS_PER_TRADE", "50"))

# Maximum total dollar cost per trade (all legs combined).
# Caps contract count so total spend <= this value.
# Set to your available balance minus a small buffer.
MAX_TRADE_COST = float(os.getenv("MAX_TRADE_COST", "100.0"))

# Abort pre-flight if any leg price has drifted more than this from detected price
MAX_LEG_DRIFT = float(os.getenv("MAX_LEG_DRIFT", "0.02"))

# Seconds to wait for a fill confirmation before declaring timeout
FILL_TIMEOUT_SECS = int(os.getenv("FILL_TIMEOUT_SECS", "10"))

# Seconds to pause before retrying leg B after Phase 2 failure
UNWIND_RETRY_DELAY_SECS = int(os.getenv("UNWIND_RETRY_DELAY_SECS", "5"))

# Seconds to wait for limit-sell unwind before escalating to market order
UNWIND_LIMIT_TIMEOUT_SECS = int(os.getenv("UNWIND_LIMIT_TIMEOUT_SECS", "60"))

# If true, hold an unwindable leg A position as a deliberate directional bet.
# Requires explicit opt-in — default false.
ALLOW_DIRECTIONAL_HOLD = os.getenv("ALLOW_DIRECTIONAL_HOLD", "false").lower() == "true"

# How often (seconds) the trading loop checks for newly authorized opportunities
TRADE_POLL_INTERVAL = int(os.getenv("TRADE_POLL_INTERVAL", "5"))

# ── Alerting ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "")
TELEGRAM_MIN_PROFIT = float(os.getenv("TELEGRAM_MIN_PROFIT", "0.01"))

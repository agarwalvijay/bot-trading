"""
Alerting layer.

Outputs arbitrage opportunities to:
  1. Console (always)
  2. Telegram (if TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID are configured)
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

import requests

from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_MIN_PROFIT,
)

logger = logging.getLogger(__name__)


def _fmt_pct(value: float) -> str:
    return f"{value * 100:.3f}%"


def _parse_end_date(end_date_iso: Optional[str]) -> Optional[datetime]:
    """Parse an ISO-8601 end-date string to a UTC-aware datetime, or None."""
    if not end_date_iso:
        return None
    try:
        # Python 3.9 fromisoformat doesn't handle trailing 'Z'
        normalized = end_date_iso.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
        # If Gamma returned a naive datetime (no tz info), assume UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return None


def _expiry_line(end_date_iso: Optional[str]) -> Optional[str]:
    """Return a formatted expiry status line, or None if end_date is absent/unparseable."""
    end_dt = _parse_end_date(end_date_iso)
    if end_dt is None:
        return None
    now = datetime.now(timezone.utc)
    mins = (end_dt - now).total_seconds() / 60
    if mins < 0:
        return f"  ⚠  MARKET MAY BE CLOSED (end_date was {end_date_iso})"
    elif mins < 60:
        return f"  ⚠  EXPIRING in {mins:.0f} min  ({end_date_iso})"
    else:
        hours = mins / 60
        return f"  ✓  OPEN  {hours:.1f} h remaining  ({end_date_iso})"


def _leg_lines(opp: dict[str, Any]) -> list[str]:
    lines = []
    for outcome, price, size in zip(opp["outcomes"], opp["ask_prices"], opp["ask_sizes"]):
        notional = price * size
        zero_flag = " ⚠ size=0" if size <= 0 else f"  (~${notional:.2f} notional)"
        lines.append(f"  {outcome:<20} ask={price:.4f}  size={size:.2f}{zero_flag}")
    return lines


def format_opportunity(opp: dict[str, Any]) -> str:
    """Return a human-readable multi-line string for a real opportunity."""
    extra = []
    expiry = _expiry_line(opp.get("end_date_iso"))
    if expiry:
        extra.append(expiry)

    lines = [
        "=" * 60,
        f"ARB OPPORTUNITY  [{opp.get('source', 'REST')}]  "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "-" * 60,
        f"Market : {opp['question'][:80]}",
        f"Cond ID: {opp['condition_id']}",
        "-" * 60,
        *_leg_lines(opp),
        "-" * 60,
        f"Sum of asks  : {opp['sum_asks']:.4f}",
        f"Gross profit : {_fmt_pct(opp['gross_profit'])}",
        f"Total fees   : {_fmt_pct(opp['total_fees'])}  (rate={_fmt_pct(opp['fee_rate'])})",
        f"NET PROFIT   : {_fmt_pct(opp['net_profit'])}",
        *extra,
        "=" * 60,
    ]
    return "\n".join(lines)


def format_near_miss(opp: dict[str, Any]) -> str:
    """Return a human-readable string for a near-miss (fees ate the spread)."""
    lines = [
        "-" * 60,
        f"NEAR-MISS  [{opp.get('source', 'REST')}]  "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"Market : {opp['question'][:80]}",
        *_leg_lines(opp),
        f"Gross={_fmt_pct(opp['gross_profit'])}  "
        f"Fees={_fmt_pct(opp['total_fees'])}  "
        f"Net={_fmt_pct(opp['net_profit'])}  "
        "(maker rebate could flip this)",
        "-" * 60,
    ]
    return "\n".join(lines)


def alert_console(opp: dict[str, Any]) -> None:
    print(format_opportunity(opp))


def alert_near_miss(opp: dict[str, Any]) -> None:
    print(format_near_miss(opp))


def alert_telegram(opp: dict[str, Any]) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if opp["net_profit"] < TELEGRAM_MIN_PROFIT:
        return

    legs = "\n".join(
        f"  {o}: ask={p:.4f} (size {s:.2f})"
        for o, p, s in zip(opp["outcomes"], opp["ask_prices"], opp["ask_sizes"])
    )
    text = (
        f"🤖 *ARB ALERT* [{opp.get('source', 'REST')}]\n"
        f"*{opp['question'][:100]}*\n\n"
        f"{legs}\n\n"
        f"Sum asks: `{opp['sum_asks']:.4f}`\n"
        f"Net profit: `{_fmt_pct(opp['net_profit'])}`"
    )

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(
            url,
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
        if not resp.ok:
            logger.warning("Telegram alert failed: %s", resp.text)
    except requests.RequestException as exc:
        logger.warning("Telegram request error: %s", exc)


def alert_expiring_actionable(opp: dict[str, Any], mins_to_close: float) -> None:
    """
    Print a high-visibility alert for markets expiring in 5-30 min with
    meaningful net profit.  Only useful once order execution is wired up.
    """
    legs = "\n".join(_leg_lines(opp))
    lines = [
        "!" * 60,
        f"⚡ EXPIRING_ACTIONABLE  [{opp.get('source','REST')}]  "
        f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"  CLOSES IN {mins_to_close:.0f} MIN  —  NET PROFIT {_fmt_pct(opp['net_profit'])}",
        "-" * 60,
        f"Market : {opp['question'][:80]}",
        f"Cond ID: {opp['condition_id']}",
        "-" * 60,
        legs,
        "-" * 60,
        f"Sum={opp['sum_asks']:.4f}  Gross={_fmt_pct(opp['gross_profit'])}  "
        f"Fees={_fmt_pct(opp['total_fees'])}  Net={_fmt_pct(opp['net_profit'])}",
        "  ⚠  Requires execution to be wired up — do NOT act manually",
        "!" * 60,
    ]
    print("\n".join(lines))


def alert(opp: dict[str, Any]) -> None:
    """Fire all configured alert channels for a real opportunity."""
    alert_console(opp)
    alert_telegram(opp)

# alert_near_miss / alert_expiring_actionable skip Telegram — too noisy without execution.

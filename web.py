"""
Simple web dashboard for the Polymarket arb bot.

Reads from the SQLite database written by arb_bot.py and renders a
live-refreshing HTML page of recent opportunities.

Run:
    python web.py                          # localhost:8080
    gunicorn -b 127.0.0.1:8080 web:app    # production
"""

import json
import re
import sqlite3
from datetime import datetime, timezone

from flask import Flask, render_template_string, request, redirect, url_for, jsonify

from config import DB_PATH, TAKER_FEE_COEFF, TRADING_ENABLED
from fetcher import fetch_market_prices

app = Flask(__name__)

# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kalshi Arb</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { font-size: 0.875rem; background: #f8f9fa; }
    .profit-pos  { color: #198754; font-weight: 700; }
    .profit-near { color: #fd7e14; font-weight: 600; }
    .profit-neg  { color: #6c757d; }
    td.legs      { font-size: 0.78rem; line-height: 1.6; white-space: nowrap; }
    .q           { max-width: 380px; word-break: break-word; }
    .stat-card   { min-width: 130px; }
    tr.authorized-row { background: #f0fff4 !important; }
  </style>
</head>
<body>
<div class="container-fluid py-3 px-4">

  <!-- Header -->
  <div class="d-flex justify-content-between align-items-center mb-3">
    <h5 class="mb-0 fw-bold">Kalshi Arb Monitor</h5>
    <div class="d-flex align-items-center gap-3">
      <span class="text-muted small">{{ now }} &mdash; refresh in <span id="countdown">30</span>s</span>
      <button class="btn btn-sm btn-link text-muted p-0" onclick="location.reload()" title="Refresh now">&#8635;</button>
      <form method="post" action="/clear" onsubmit="return confirm('Clear {{ category.replace(\"_\", \" \").title() + \" opportunities\" if category else \"ALL logged opportunities\" }}? This cannot be undone.');">
        <input type="hidden" name="cat" value="{{ category }}">
        <button type="submit" class="btn btn-sm btn-outline-danger">Clear {{ category.replace("_", " ").title() if category else "All" }}</button>
      </form>
    </div>
  </div>

  <!-- Stats row -->
  <div class="row g-2 mb-3">
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Total logged</div>
        <div class="fs-5 fw-bold">{{ stats.total }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Opportunities</div>
        <div class="fs-5 fw-bold text-success">{{ stats.opps }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Near misses</div>
        <div class="fs-5 fw-bold text-warning">{{ stats.near_miss }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Cumulative</div>
        <div class="fs-5 fw-bold text-danger">{{ stats.cumulative }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Non-exhaustive</div>
        <div class="fs-5 fw-bold" style="color:#6f42c1">{{ stats.non_exhaustive }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Spread</div>
        <div class="fs-5 fw-bold" style="color:#0d6efd">{{ stats.spread_market }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Ignored</div>
        <div class="fs-5 fw-bold text-muted">{{ stats.ignored }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Last logged</div>
        <div class="fs-5 fw-bold">{{ stats.last_seen_ago }}</div>
      </div>
    </div>
  </div>

  <!-- Filter tabs -->
  <ul class="nav nav-tabs mb-0">
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if not category }}" href="/">All</a>
    </li>
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if category == 'opportunity' }}"
         href="/?cat=opportunity">Opportunities</a>
    </li>
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if category == 'near_miss' }}"
         href="/?cat=near_miss">Near misses</a>
    </li>
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if category == 'cumulative' }}"
         href="/?cat=cumulative">Cumulative</a>
    </li>
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if category == 'non_exhaustive' }}"
         href="/?cat=non_exhaustive">Non-exhaustive</a>
    </li>
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if category == 'spread_market' }}"
         href="/?cat=spread_market">Spread</a>
    </li>
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if category == 'ignored' }}"
         href="/?cat=ignored">Ignored</a>
    </li>
    {% if trading_enabled %}
    <li class="nav-item ms-auto">
      <a class="nav-link text-success fw-semibold" href="/trades">&#9889; Trades</a>
    </li>
    {% endif %}
  </ul>

  <!-- Table -->
  <div class="card rounded-top-0 border-top-0">
    <div class="table-responsive">
      <table class="table table-sm table-hover align-middle mb-0">
        <thead class="table-dark">
          <tr>
            <th style="width:80px">Time</th>
            <th>Market</th>
            <th>Legs</th>
            <th style="width:80px">Sum asks</th>
            <th style="width:90px">Net profit</th>
            <th style="width:60px">Src</th>
            <th style="width:52px"></th>
          </tr>
        </thead>
        <tbody>
          {% for r in rows %}
          <tr class="{{ 'authorized-row' if r.authorized }}">
            <td class="text-muted text-nowrap">{{ r.time_ago }}</td>
            <td class="q">
              <a href="{{ r.kalshi_url }}" target="_blank" rel="noopener"
                 class="text-decoration-none text-dark">
                {{ r.question }}
              </a>
              {% if r.closes_in.label %}
                <br><span class="small {{ r.closes_in.css }}">{{ r.closes_in.label }}</span>
              {% endif %}
              {% if r.has_zero_size %}
                <br><span class="badge bg-secondary">zero-size</span>
              {% endif %}
              {% if r.category == 'near_miss' %}
                <br><span class="badge bg-warning text-dark">near-miss</span>
              {% elif r.category == 'cumulative' %}
                <br><span class="badge bg-danger">CUMULATIVE</span>
              {% elif r.category == 'non_exhaustive' %}
                <br><span class="badge" style="background:#6f42c1">NON-EXHAUSTIVE</span>
              {% elif r.category == 'spread_market' %}
                <br><span class="badge bg-primary">SPREAD</span>
              {% elif r.category == 'ignored' %}
                <br><span class="badge bg-secondary">IGNORED</span>
              {% endif %}
              {% if r.authorized %}
                {% if r.trade_id %}
                  <br><span class="badge bg-success">&#9889; AUTHORIZED</span>
                  <span class="badge bg-dark ms-1">trade #{{ r.trade_id }}</span>
                {% else %}
                  <br><span class="badge bg-success">&#9889; AUTHORIZED — attempting…</span>
                {% endif %}
              {% endif %}
            </td>
            <td class="legs">
              {% for leg in r.legs[:10] %}
              <div>
                <span class="text-muted">{{ leg.outcome[:18] }}</span>
                ask=<strong>{{ "%.4f" | format(leg.price) }}</strong>
                <span class="text-muted">({{ "%.0f" | format(leg.size) }})</span>
              </div>
              {% endfor %}
              {% if r.legs | length > 10 %}
              <div class="text-muted">…and {{ r.legs | length - 10 }} more</div>
              {% endif %}
            </td>
            <td>{{ "%.4f" | format(r.sum_asks) }}</td>
            <td class="{{ 'profit-pos' if r.net_profit >= 0.005 else ('profit-near' if r.net_profit >= 0 else 'profit-neg') }}">
              {{ "%+.3f%%" | format(r.net_profit * 100) }}
            </td>
            <td>
              <span class="badge {{ 'bg-info text-dark' if r.source == 'WS' else 'bg-secondary' }}">
                {{ r.source }}
              </span>
            </td>
            <td class="text-nowrap">
              <button class="btn btn-sm btn-link text-primary p-0 me-1" title="Live prices"
                      onclick="showLive({{ r.row_id }})">&#8635;</button>
              {% if r.category == 'opportunity' and trading_enabled %}
                {% if r.authorized %}
                <form method="post" action="/authorize/{{ r.row_id }}" style="margin:0;display:inline">
                  <input type="hidden" name="cat" value="{{ category }}">
                  <input type="hidden" name="authorize" value="0">
                  <button type="submit" class="btn btn-sm btn-link text-success p-0 me-1"
                          title="Deauthorize trade" style="font-size:1rem">&#9889;</button>
                </form>
                {% else %}
                <form method="post" action="/authorize/{{ r.row_id }}" style="margin:0;display:inline">
                  <input type="hidden" name="cat" value="{{ category }}">
                  <input type="hidden" name="authorize" value="1">
                  <button type="submit" class="btn btn-sm btn-link text-muted p-0 me-1"
                          title="Authorize for trading" style="font-size:1rem">&#9889;</button>
                </form>
                {% endif %}
              {% endif %}
              {% if r.category != 'opportunity' %}
              <form method="post" action="/set-category/{{ r.row_id }}" style="margin:0;display:inline">
                <input type="hidden" name="cat" value="{{ category }}">
                <input type="hidden" name="new_cat" value="opportunity">
                <button type="submit" class="btn btn-sm btn-link text-success p-0 me-1" title="Promote to opportunity">&#8679;</button>
              </form>
              {% endif %}
              {% if r.category != 'ignored' %}
              <form method="post" action="/set-category/{{ r.row_id }}" style="margin:0;display:inline">
                <input type="hidden" name="cat" value="{{ category }}">
                <input type="hidden" name="new_cat" value="ignored">
                <button type="submit" class="btn btn-sm btn-link text-secondary p-0 me-1" title="Ignore">&#8856;</button>
              </form>
              {% endif %}
              <form method="post" action="/delete/{{ r.row_id }}" style="margin:0;display:inline"
                    onsubmit="return confirm('Delete this row?');">
                <input type="hidden" name="cat" value="{{ category }}">
                <button type="submit" class="btn btn-sm btn-link text-danger p-0" title="Delete">&times;</button>
              </form>
            </td>
          </tr>
          {% endfor %}
          {% if not rows %}
          <tr>
            <td colspan="7" class="text-center text-muted py-5">No records yet.</td>
          </tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
  <div class="text-muted small mt-2">Showing {{ rows | length }} most recent records.</div>

</div>

<!-- Live prices modal -->
<div class="modal fade" id="liveModal" tabindex="-1">
  <div class="modal-dialog modal-lg">
    <div class="modal-content">
      <div class="modal-header py-2">
        <h6 class="modal-title fw-bold mb-0" id="liveModalTitle">Live Pricing</h6>
        <div class="d-flex align-items-center gap-2 ms-auto me-2">
          <button class="btn btn-sm btn-link text-muted p-0" id="liveRefreshBtn"
                  onclick="showLive(_currentLiveRowId)" title="Refresh prices">&#8635;</button>
        </div>
        <button type="button" class="btn-close" data-bs-dismiss="modal"></button>
      </div>
      <div class="modal-body p-3" id="liveModalBody">
        <div class="text-center py-4"><div class="spinner-border spinner-border-sm"></div> Fetching…</div>
      </div>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
<script>
(function() {
  var secs = 30;
  var el = document.getElementById('countdown');
  var iv = setInterval(function() {
    secs--;
    if (secs <= 0) { clearInterval(iv); location.reload(); }
    else { el.textContent = secs; }
  }, 1000);
})();
</script>
<script>
var _currentLiveRowId = null;
var _liveModal = null;
function showLive(rowId) {
  _currentLiveRowId = rowId;
  if (!_liveModal) _liveModal = new bootstrap.Modal(document.getElementById('liveModal'));
  document.getElementById('liveModalTitle').textContent = 'Live Pricing';
  document.getElementById('liveModalBody').innerHTML =
    '<div class="text-center py-4"><div class="spinner-border spinner-border-sm"></div> Fetching…</div>';
  if (!document.getElementById('liveModal').classList.contains('show')) _liveModal.show();

  fetch('/api/prices/' + rowId)
    .then(r => r.json())
    .then(data => {
      if (data.error) {
        document.getElementById('liveModalBody').innerHTML =
          '<div class="text-danger">' + data.error + '</div>';
        return;
      }
      document.getElementById('liveModalTitle').textContent = data.title;

      const fmtPct = v => v == null ? '—' : (v >= 0 ? '+' : '') + (v * 100).toFixed(2) + '%';
      const fmtP   = v => v == null ? '—' : v.toFixed(4);
      const arrow  = (curr, logged) => {
        if (curr == null || logged == null) return '';
        const d = curr - logged;
        if (Math.abs(d) < 0.0005) return '<span class="text-muted">→</span>';
        return d > 0
          ? '<span class="text-danger">▲ +' + (d * 100).toFixed(2) + '%</span>'
          : '<span class="text-success">▼ ' + (d * 100).toFixed(2) + '%</span>';
      };

      // Close time
      let closeHtml = '';
      if (data.time_to_close != null) {
        const h = Math.floor(data.time_to_close / 3600);
        const m = Math.floor((data.time_to_close % 3600) / 60);
        const label = data.time_to_close < 0 ? 'Expired' :
                      h > 0 ? h + 'h ' + m + 'm remaining' : m + 'm remaining';
        const cls = data.time_to_close < 3600 ? 'text-danger fw-bold' :
                    data.time_to_close < 14400 ? 'text-warning fw-semibold' : 'text-success';
        closeHtml = '<span class="' + cls + '">' + label + '</span>';
        if (data.close_time) closeHtml += ' <span class="text-muted small">(' + data.close_time.replace('T',' ').slice(0,16) + ' UTC)</span>';
      }

      // Legs table
      let rows = data.legs.map(l =>
        '<tr>' +
        '<td class="text-muted" style="font-size:0.8rem">' + l.outcome + '</td>' +
        '<td class="text-muted">' + fmtP(l.logged_price) + '</td>' +
        '<td class="fw-semibold">' + fmtP(l.current_price) + '</td>' +
        '<td>' + arrow(l.current_price, l.logged_price) + '</td>' +
        '<td class="text-muted">' + (l.current_size != null ? Math.round(l.current_size) : '—') + '</td>' +
        '</tr>'
      ).join('');

      // Summary row
      const sumArrow = arrow(data.current_sum, data.logged_sum);
      const profitCls = data.current_net_profit == null ? '' :
                        data.current_net_profit >= 0.005 ? 'text-success fw-bold' :
                        data.current_net_profit >= 0 ? 'text-warning fw-semibold' : 'text-muted';

      document.getElementById('liveModalBody').innerHTML = `
        <div class="mb-2 d-flex justify-content-between align-items-center">
          <div>${closeHtml || '<span class="text-muted">No close time</span>'}</div>
          <div class="text-muted small">Fetched just now</div>
        </div>
        <table class="table table-sm table-bordered mb-2" style="font-size:0.85rem">
          <thead class="table-secondary">
            <tr><th>Outcome</th><th>Logged ask</th><th>Current ask</th><th>Change</th><th>Size</th></tr>
          </thead>
          <tbody>${rows}</tbody>
          <tfoot class="table-light fw-semibold">
            <tr>
              <td>Sum</td>
              <td>${fmtP(data.logged_sum)}</td>
              <td>${fmtP(data.current_sum)}</td>
              <td>${sumArrow}</td>
              <td></td>
            </tr>
          </tfoot>
        </table>
        <div class="d-flex gap-4">
          <div>Logged net profit: <strong>${fmtPct(data.logged_net_profit)}</strong></div>
          <div>Current net profit: <strong class="${profitCls}">${fmtPct(data.current_net_profit)}</strong></div>
        </div>`;
    })
    .catch(err => {
      document.getElementById('liveModalBody').innerHTML =
        '<div class="text-danger">Request failed: ' + err + '</div>';
    });
}
</script>
</body>
</html>"""

# ---------------------------------------------------------------------------
# Trades template
# ---------------------------------------------------------------------------

TRADES_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kalshi Arb — Trades</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { font-size: 0.875rem; background: #f8f9fa; }
    .stat-card { min-width: 120px; }
    td.small-mono { font-size: 0.78rem; font-family: monospace; }
  </style>
</head>
<body>
<div class="container-fluid py-3 px-4">

  <!-- Header -->
  <div class="d-flex justify-content-between align-items-center mb-3">
    <h5 class="mb-0 fw-bold">&#9889; Trades</h5>
    <div class="d-flex align-items-center gap-3">
      <span class="text-muted small">{{ now }}</span>
      <button class="btn btn-sm btn-link text-muted p-0" onclick="location.reload()" title="Refresh">&#8635;</button>
      <a href="/" class="btn btn-sm btn-outline-secondary">&#8592; Opportunities</a>
    </div>
  </div>

  <!-- Stats -->
  <div class="row g-2 mb-3">
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Total trades</div>
        <div class="fs-5 fw-bold">{{ stats.total }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Complete</div>
        <div class="fs-5 fw-bold text-success">{{ stats.complete }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">In progress</div>
        <div class="fs-5 fw-bold text-primary">{{ stats.in_progress }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Unwound</div>
        <div class="fs-5 fw-bold text-warning">{{ stats.unwound }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Failed/Aborted</div>
        <div class="fs-5 fw-bold text-danger">{{ stats.failed }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Total net PnL</div>
        <div class="fs-5 fw-bold {{ 'text-success' if stats.total_net_pnl >= 0 else 'text-danger' }}">
          ${{ "%.4f" | format(stats.total_net_pnl) }}
        </div>
      </div>
    </div>
  </div>

  <!-- Table -->
  <div class="card">
    <div class="table-responsive">
      <table class="table table-sm table-hover align-middle mb-0">
        <thead class="table-dark">
          <tr>
            <th style="width:50px">#</th>
            <th style="width:70px">Mode</th>
            <th style="width:110px">Status</th>
            <th>Opportunity</th>
            <th style="width:90px">Started</th>
            <th style="width:90px">Completed</th>
            <th>Legs / Fills</th>
            <th style="width:90px">Net PnL</th>
            <th>Notes</th>
          </tr>
        </thead>
        <tbody>
          {% for t in trades %}
          <tr>
            <td class="text-muted">{{ t.id }}</td>
            <td>
              {% if t.demo_mode %}
                <span class="badge bg-info text-dark">DEMO</span>
              {% else %}
                <span class="badge bg-danger">LIVE</span>
              {% endif %}
            </td>
            <td>
              {% if t.status == 'complete' %}
                <span class="badge bg-success">complete</span>
              {% elif t.status in ('phase1_placed', 'phase2_placed') %}
                <span class="badge bg-primary">{{ t.status }}</span>
              {% elif t.status == 'phase1_filled' %}
                <span class="badge bg-info text-dark">phase1 filled</span>
              {% elif t.status == 'preflight_failed' %}
                <span class="badge bg-secondary">preflight failed</span>
              {% elif t.status == 'aborted' %}
                <span class="badge bg-danger">aborted</span>
              {% elif t.status.startswith('unwind') %}
                <span class="badge bg-warning text-dark">{{ t.status }}</span>
              {% else %}
                <span class="badge bg-secondary">{{ t.status }}</span>
              {% endif %}
            </td>
            <td class="small-mono">{{ t.opp_title }}</td>
            <td class="text-muted text-nowrap">{{ t.started_ago }}</td>
            <td class="text-muted text-nowrap">{{ t.completed_ago }}</td>
            <td class="small-mono">
              {% for leg in t.legs %}
              <div>
                <span class="text-muted">{{ leg.ticker.split('-')[0] }}…</span>
                {% if leg.fill_price %}
                  filled <strong>{{ leg.fill_count }}×</strong> @ {{ "%.4f" | format(leg.fill_price) }}
                {% else %}
                  target {{ "%.4f" | format(leg.target_price) }}
                {% endif %}
              </div>
              {% endfor %}
            </td>
            <td>
              {% if t.net_pnl is not none %}
                <span class="{{ 'text-success fw-bold' if t.net_pnl >= 0 else 'text-danger fw-bold' }}">
                  {{ "%+.4f" | format(t.net_pnl) }}
                </span>
              {% else %}
                <span class="text-muted">—</span>
              {% endif %}
            </td>
            <td class="text-muted small">
              {{ t.notes or '' }}
              {{ t.unwind_reason or '' }}
            </td>
          </tr>
          {% endfor %}
          {% if not trades %}
          <tr>
            <td colspan="9" class="text-center text-muted py-5">No trades yet.</td>
          </tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
  <div class="text-muted small mt-2">Showing {{ trades | length }} most recent trades. Auto-refreshes every 10s.</div>

</div>
<script>
setTimeout(function() { location.reload(); }, 10000);
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _time_ago(iso_str: str) -> str:
    if not iso_str:
        return "—"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        s = int((datetime.now(timezone.utc) - dt).total_seconds())
        if s < 60:
            return f"{s}s ago"
        if s < 3600:
            return f"{s // 60}m ago"
        return f"{s // 3600}h ago"
    except Exception:
        return iso_str[:16]


def _closes_in(close_time: str) -> dict:
    """Return a dict with label and css class for time-to-close display."""
    if not close_time:
        return {"label": "", "css": ""}
    try:
        ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
        if ct.tzinfo is None:
            ct = ct.replace(tzinfo=timezone.utc)
        secs = int((ct - datetime.now(timezone.utc)).total_seconds())
        if secs < 0:
            return {"label": "expired", "css": "text-muted"}
        if secs < 3600:
            m = secs // 60
            return {"label": f"closes {m}m", "css": "text-danger fw-semibold"}
        if secs < 86400:
            h = secs // 3600
            m = (secs % 3600) // 60
            label = f"closes {h}h {m}m" if m else f"closes {h}h"
            css = "text-warning fw-semibold" if secs < 14400 else "text-muted"
            return {"label": label, "css": css}
        d = secs // 86400
        return {"label": f"closes {d}d", "css": "text-muted"}
    except Exception:
        return {"label": "", "css": ""}


def _get_rows(category=None, limit: int = 200) -> list:
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        q = "SELECT * FROM opportunities"
        params: list = []
        if category:
            q += " WHERE category = ?"
            params.append(category)
        q += " ORDER BY detected_at DESC LIMIT ?"
        params.append(limit)
        raw = con.execute(q, params).fetchall()
        con.close()
    except Exception:
        return []

    rows = []
    for r in raw:
        try:
            outcomes = json.loads(r["outcomes"])
            prices   = json.loads(r["ask_prices"])
            sizes    = json.loads(r["ask_sizes"])
            legs = [
                {"outcome": o, "price": float(p), "size": float(s)}
                for o, p, s in zip(outcomes, prices, sizes)
            ]
        except Exception:
            legs = []
        event_ticker = r["event_ticker"] or r["ticker"]
        ticker       = r["ticker"]
        try:
            is_binary = json.loads(r["outcomes"] or "[]") == ["Yes", "No"]
        except Exception:
            is_binary = False

        if is_binary:
            # Link to the specific market ticker (full, no stripping)
            # e.g. KXNCAAWBGAME-26MAR19NAVYHARV → kxncaawbgame-26mar19navyharv
            url_slug = ticker.lower()
        elif "SPREAD" in event_ticker.upper():
            # Spread markets: swap SPREAD→GAME to link to the underlying game page
            # e.g. KXNHLSPREAD-26MAR19CHIMIN → kxnhlgame-26mar19chimin
            url_slug = re.sub(r"SPREAD", "GAME", event_ticker, flags=re.IGNORECASE).lower()
        else:
            # Use full event ticker — Kalshi routes /markets/{event_ticker} to the event page
            # e.g. KXFIFAGAME-26MAR26CZEIRL → kxfifagame-26mar26czeirl
            url_slug = event_ticker.lower()
        kalshi_url = f"https://kalshi.com/markets/{url_slug}"
        rows.append({
            "row_id":        r["id"],
            "time_ago":      _time_ago(r["detected_at"]),
            "question":      r["title"],
            "kalshi_url":    kalshi_url,
            "legs":          legs,
            "sum_asks":      r["sum_asks"],
            "net_profit":    r["net_profit"],
            "source":        r["source"],
            "category":      r["category"],
            "has_zero_size": r["has_zero_size"],
            "closes_in":     _closes_in(r["close_time"]),
            "authorized":    bool(r["authorized"]) if "authorized" in r.keys() else False,
            "trade_id":      r["trade_id"] if "trade_id" in r.keys() else None,
        })
    return rows


def _get_stats() -> dict:
    try:
        con = sqlite3.connect(DB_PATH)
        total         = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        opps          = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='opportunity'").fetchone()[0]
        near_miss     = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='near_miss'").fetchone()[0]
        cumulative    = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='cumulative'").fetchone()[0]
        non_exhaustive = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='non_exhaustive'").fetchone()[0]
        spread_market  = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='spread_market'").fetchone()[0]
        ignored        = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='ignored'").fetchone()[0]
        last          = con.execute("SELECT MAX(detected_at) FROM opportunities").fetchone()[0]
        con.close()
        return {"total": total, "opps": opps, "near_miss": near_miss,
                "cumulative": cumulative, "non_exhaustive": non_exhaustive,
                "spread_market": spread_market, "ignored": ignored,
                "last_seen_ago": _time_ago(last)}
    except Exception:
        return {"total": 0, "opps": 0, "near_miss": 0,
                "cumulative": 0, "non_exhaustive": 0, "spread_market": 0,
                "ignored": 0, "last_seen_ago": "—"}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    cat   = request.args.get("cat", "")
    rows  = _get_rows(category=cat or None)
    stats = _get_stats()
    now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return render_template_string(TEMPLATE, rows=rows, stats=stats,
                                  category=cat, now=now,
                                  trading_enabled=TRADING_ENABLED)


@app.route("/clear", methods=["POST"])
def clear_all():
    cat = request.form.get("cat", "")
    try:
        con = sqlite3.connect(DB_PATH)
        if cat:
            con.execute("DELETE FROM opportunities WHERE category = ?", (cat,))
        else:
            con.execute("DELETE FROM opportunities")
        con.commit()
        con.close()
    except Exception:
        pass
    return redirect(url_for("index", cat=cat) if cat else url_for("index"))


@app.route("/set-category/<int:row_id>", methods=["POST"])
def set_category(row_id: int):
    cat     = request.form.get("cat", "")
    new_cat = request.form.get("new_cat", "")
    allowed = {"opportunity", "near_miss", "cumulative", "non_exhaustive",
               "spread_market", "ignored"}
    if new_cat in allowed:
        try:
            con = sqlite3.connect(DB_PATH)
            con.execute("UPDATE opportunities SET category = ? WHERE id = ?", (new_cat, row_id))
            con.commit()
            con.close()
        except Exception:
            pass
    return redirect(url_for("index", cat=cat) if cat else url_for("index"))


@app.route("/delete/<int:row_id>", methods=["POST"])
def delete_row(row_id: int):
    cat = request.form.get("cat", "")
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("DELETE FROM opportunities WHERE id = ?", (row_id,))
        con.commit()
        con.close()
    except Exception:
        pass
    return redirect(url_for("index", cat=cat) if cat else url_for("index"))


@app.route("/authorize/<int:row_id>", methods=["POST"])
def authorize_row(row_id: int):
    cat       = request.form.get("cat", "")
    authorize = request.form.get("authorize", "1") == "1"
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("UPDATE opportunities SET authorized = ? WHERE id = ? AND category = 'opportunity'",
                    (1 if authorize else 0, row_id))
        con.commit()
        con.close()
    except Exception:
        pass
    return redirect(url_for("index", cat=cat) if cat else url_for("index"))


@app.route("/api/prices/<int:row_id>")
def api_prices(row_id: int):
    """Return current live prices for a logged opportunity row."""
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        row = con.execute("SELECT * FROM opportunities WHERE id = ?", (row_id,)).fetchone()
        if not row:
            con.close()
            return jsonify({"error": "Row not found"}), 404

        event_ticker     = row["event_ticker"] or row["ticker"]
        logged_prices    = json.loads(row["ask_prices"]      or "[]")
        outcomes         = json.loads(row["outcomes"]        or "[]")
        outcome_tickers  = json.loads(row["outcome_tickers"] or "[]")
        close_time       = row["close_time"]

        # Fall back to close_time from cache if not stored on the opportunity
        if not close_time:
            ct_row = con.execute(
                "SELECT close_time FROM markets_cache WHERE event_ticker = ? LIMIT 1",
                (event_ticker,),
            ).fetchone()
            if ct_row:
                close_time = ct_row["close_time"]

        con.close()
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500

    # Use stored outcome_tickers when available; fall back to cache lookup by event
    if outcome_tickers:
        tickers = outcome_tickers
    else:
        try:
            con2 = sqlite3.connect(DB_PATH)
            con2.row_factory = sqlite3.Row
            cache_rows = con2.execute(
                "SELECT ticker FROM markets_cache WHERE event_ticker = ? OR ticker = ?",
                (event_ticker, event_ticker),
            ).fetchall()
            con2.close()
            tickers = [r["ticker"] for r in cache_rows]
        except Exception:
            tickers = []

    if not tickers:
        return jsonify({"error": "Tickers not found in cache — run a market refresh first"}), 404

    try:
        current = fetch_market_prices(tickers)
    except Exception as exc:
        return jsonify({"error": f"Kalshi API error: {exc}"}), 502

    legs = []
    for i, outcome in enumerate(outcomes):
        # Use stored ticker for this position; fall back to event ticker itself
        ticker     = outcome_tickers[i] if i < len(outcome_tickers) else event_ticker
        logged_p   = logged_prices[i] if i < len(logged_prices) else None
        curr_entry = current.get(ticker, {})
        curr_price = curr_entry.get("yes_ask")
        curr_size  = curr_entry.get("yes_ask_size")
        legs.append({
            "outcome":       outcome,
            "logged_price":  logged_p,
            "current_price": curr_price,
            "current_size":  curr_size,
        })

    # Compute current sum + net profit if we have all prices
    curr_prices = [l["current_price"] for l in legs]
    if all(p is not None for p in curr_prices):
        curr_sum     = sum(curr_prices)
        gross        = 1.0 - curr_sum
        fees         = sum(TAKER_FEE_COEFF * p * (1 - p) for p in curr_prices)
        curr_net     = gross - fees
    else:
        curr_sum = curr_net = None

    # Time to close in seconds
    time_to_close = None
    if close_time:
        try:
            ct = datetime.fromisoformat(close_time.replace("Z", "+00:00"))
            if ct.tzinfo is None:
                ct = ct.replace(tzinfo=timezone.utc)
            time_to_close = int((ct - datetime.now(timezone.utc)).total_seconds())
        except Exception:
            pass

    return jsonify({
        "title":              row["title"],
        "legs":               legs,
        "logged_sum":         row["sum_asks"],
        "logged_net_profit":  row["net_profit"],
        "current_sum":        curr_sum,
        "current_net_profit": curr_net,
        "close_time":         close_time,
        "time_to_close":      time_to_close,
    })


def _get_trades(limit: int = 100) -> tuple[list, dict]:
    """Return trade rows and summary stats."""
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        raw = con.execute("""
            SELECT t.*, o.title as opp_title
            FROM trades t
            LEFT JOIN opportunities o ON t.opportunity_id = o.id
            ORDER BY t.id DESC
            LIMIT ?
        """, (limit,)).fetchall()
        con.close()
    except Exception:
        return [], {"total": 0, "complete": 0, "in_progress": 0,
                    "unwound": 0, "failed": 0, "total_net_pnl": 0.0}

    trades = []
    stats = {"total": 0, "complete": 0, "in_progress": 0,
             "unwound": 0, "failed": 0, "total_net_pnl": 0.0}

    in_progress_statuses = {"phase1_placed", "phase1_filled", "phase2_placed", "unwind_retry"}
    unwind_statuses      = {"unwind_limit", "unwind_market", "unwind_hold", "unwind_failed"}
    failed_statuses      = {"aborted"}

    for r in raw:
        status = r["status"] or ""
        stats["total"] += 1
        if status == "complete":
            stats["complete"] += 1
        elif status in in_progress_statuses:
            stats["in_progress"] += 1
        elif status in unwind_statuses:
            stats["unwound"] += 1
        elif status in failed_statuses:
            stats["failed"] += 1

        if r["net_pnl"] is not None:
            stats["total_net_pnl"] += r["net_pnl"]

        # Parse leg arrays for display
        try:
            leg_tickers    = json.loads(r["leg_tickers"]    or "[]")
            target_prices  = json.loads(r["target_prices"]  or "[]")
            fill_prices_l  = json.loads(r["fill_prices"]    or "[]")
            fill_counts_l  = json.loads(r["fill_counts"]    or "[]")
        except Exception:
            leg_tickers = target_prices = fill_prices_l = fill_counts_l = []

        legs = []
        for i, ticker in enumerate(leg_tickers):
            legs.append({
                "ticker":       ticker,
                "target_price": target_prices[i] if i < len(target_prices) else None,
                "fill_price":   fill_prices_l[i]  if i < len(fill_prices_l)  else None,
                "fill_count":   fill_counts_l[i]  if i < len(fill_counts_l)  else None,
            })

        trades.append({
            "id":            r["id"],
            "status":        status,
            "demo_mode":     bool(r["demo_mode"]),
            "opp_title":     r["opp_title"] or f"opp #{r['opportunity_id']}",
            "started_ago":   _time_ago(r["started_at"]),
            "completed_ago": _time_ago(r["completed_at"]) if r["completed_at"] else "—",
            "legs":          legs,
            "net_pnl":       r["net_pnl"],
            "notes":         r["notes"],
            "unwind_reason": r["unwind_reason"],
        })

    return trades, stats


@app.route("/trades")
def trades_page():
    trades, stats = _get_trades()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return render_template_string(TRADES_TEMPLATE, trades=trades, stats=stats, now=now)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=False)

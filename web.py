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

from config import DB_PATH, DEMO_MODE, MIN_VOLUME_24H, TAKER_FEE_COEFF, TRADING_ENABLED
from db import set_authorized
from fetcher import fetch_market_prices
from market_key import canonical_market_key

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
    .stat-card   { min-width: 112px; }
    tr.authorized-row { background: #f0fff4 !important; }
    a.card-link { text-decoration: none; color: inherit; display: inline-block; }
    .stat-card.active-filter { border: 2px solid #0d6efd; }
    .stats-row { flex-wrap: nowrap; overflow-x: auto; }
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
        <input type="hidden" name="view" value="{{ view_mode }}">
        <button type="submit" class="btn btn-sm btn-outline-danger">Clear {{ category.replace("_", " ").title() if category else "All" }}</button>
      </form>
    </div>
  </div>

  <!-- Stats row -->
  <div class="row g-1 mb-3 stats-row">
    <div class="col-auto">
      <a class="card-link" href="/?view={{ view_mode }}">
      <div class="card stat-card text-center px-2 py-1 {{ 'active-filter' if not category }}">
        <div class="text-muted small">Total logged</div>
        <div class="fs-6 fw-bold">{{ stats.total }}</div>
      </div>
      </a>
    </div>
    <div class="col-auto">
      <a class="card-link" href="/?cat=opportunity&view={{ view_mode }}">
      <div class="card stat-card text-center px-2 py-1 {{ 'active-filter' if category == 'opportunity' }}">
        <div class="text-muted small">Opportunities</div>
        <div class="fs-6 fw-bold text-success">{{ stats.opps }}</div>
      </div>
      </a>
    </div>
    <div class="col-auto">
      <a class="card-link" href="/?cat=near_miss&view={{ view_mode }}">
      <div class="card stat-card text-center px-2 py-1 {{ 'active-filter' if category == 'near_miss' }}">
        <div class="text-muted small">Near misses</div>
        <div class="fs-6 fw-bold text-warning">{{ stats.near_miss }}</div>
      </div>
      </a>
    </div>
    <div class="col-auto">
      <a class="card-link" href="/?cat=cumulative&view={{ view_mode }}">
      <div class="card stat-card text-center px-2 py-1 {{ 'active-filter' if category == 'cumulative' }}">
        <div class="text-muted small">Cumulative</div>
        <div class="fs-6 fw-bold text-danger">{{ stats.cumulative }}</div>
      </div>
      </a>
    </div>
    <div class="col-auto">
      <a class="card-link" href="/?cat=non_exhaustive&view={{ view_mode }}">
      <div class="card stat-card text-center px-2 py-1 {{ 'active-filter' if category == 'non_exhaustive' }}">
        <div class="text-muted small">Non-exhaustive</div>
        <div class="fs-6 fw-bold" style="color:#6f42c1">{{ stats.non_exhaustive }}</div>
      </div>
      </a>
    </div>
    <div class="col-auto">
      <a class="card-link" href="/?cat=spread_market&view={{ view_mode }}">
      <div class="card stat-card text-center px-2 py-1 {{ 'active-filter' if category == 'spread_market' }}">
        <div class="text-muted small">Spread</div>
        <div class="fs-6 fw-bold" style="color:#0d6efd">{{ stats.spread_market }}</div>
      </div>
      </a>
    </div>
    <div class="col-auto">
      <a class="card-link" href="/?cat=likely_resolved&view={{ view_mode }}">
      <div class="card stat-card text-center px-2 py-1 {{ 'active-filter' if category == 'likely_resolved' }}">
        <div class="text-muted small">Likely resolved</div>
        <div class="fs-6 fw-bold" style="color:#6c3483">{{ stats.likely_resolved }}</div>
      </div>
      </a>
    </div>
    <div class="col-auto">
      <a class="card-link" href="/?cat=ignored&view={{ view_mode }}">
      <div class="card stat-card text-center px-2 py-1 {{ 'active-filter' if category == 'ignored' }}">
        <div class="text-muted small">Ignored</div>
        <div class="fs-6 fw-bold text-muted">{{ stats.ignored }}</div>
      </div>
      </a>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-2 py-1">
        <div class="text-muted small">Last logged</div>
        <div class="fs-6 fw-bold">{{ stats.last_seen_ago }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-2 py-1">
        <div class="text-muted small">Min 24h vol</div>
        <div class="fs-6 fw-bold text-secondary">{{ stats.min_volume_24h }}</div>
      </div>
    </div>
    <div class="col-auto">
      <form method="post" action="/reset-counter/ws"
            onsubmit="return confirm('Reset WS notifications counter?');">
        <input type="hidden" name="cat" value="{{ category }}">
        <input type="hidden" name="view" value="{{ view_mode }}">
        <button type="submit" class="card stat-card text-center px-2 py-1" style="border:1px solid #dee2e6; background:#fff;">
          <div class="text-muted small">WS notifications</div>
          <div class="fs-6 fw-bold text-primary">{{ stats.ws_count }}</div>
          <div class="text-muted small">since {{ stats.ws_since }}</div>
        </button>
      </form>
    </div>
    <div class="col-auto">
      <form method="post" action="/reset-counter/ws_watched"
            onsubmit="return confirm('Reset WS watched counter?');">
        <input type="hidden" name="cat" value="{{ category }}">
        <input type="hidden" name="view" value="{{ view_mode }}">
        <button type="submit" class="card stat-card text-center px-2 py-1" style="border:1px solid #dee2e6; background:#fff;">
          <div class="text-muted small">WS watched</div>
          <div class="fs-6 fw-bold text-info">{{ stats.ws_watched_count }}</div>
          <div class="text-muted small">since {{ stats.ws_watched_since }}</div>
        </button>
      </form>
    </div>
    <div class="col-auto">
      <form method="post" action="/reset-counter/ws_gate_rejects"
            onsubmit="return confirm('Reset WS gate reject counters?');">
        <input type="hidden" name="cat" value="{{ category }}">
        <input type="hidden" name="view" value="{{ view_mode }}">
        <button type="submit" class="card stat-card text-center px-2 py-1" style="border:1px solid #dee2e6; background:#fff;">
          <div class="text-muted small">WS gate rejects</div>
          <div class="fs-6 fw-bold text-danger">{{ stats.ws_gate_reject_total }}</div>
          <div class="text-muted small">
            miss {{ stats.ws_gate_reject_missing_ts }}
            age {{ stats.ws_gate_reject_age }}
            skew {{ stats.ws_gate_reject_skew }}
            stale {{ stats.preflight_reject_stale_snapshot }}
          </div>
        </button>
      </form>
    </div>
  </div>

  <div class="d-flex justify-content-between align-items-center mb-2">
    <form method="post" action="/add-market" class="d-flex gap-1 align-items-center">
      <input type="hidden" name="cat" value="{{ category }}">
      <input type="hidden" name="view" value="{{ view_mode }}">
      <input type="text" name="ticker" placeholder="Add ticker…"
             class="form-control form-control-sm" style="width:210px"
             title="Enter an event ticker (e.g. KXNHLGAME-26MAR21WPGPIT) or market ticker">
      <button type="submit" class="btn btn-sm btn-outline-secondary">+ Watch</button>
    </form>
    <div class="d-flex align-items-center gap-2">
      <a class="btn btn-sm {{ 'btn-primary' if view_mode == 'time' else 'btn-outline-primary' }}"
         href="/?cat={{ category }}&view=time">Time</a>
      <a class="btn btn-sm {{ 'btn-primary' if view_mode == 'market' else 'btn-outline-primary' }}"
         href="/?cat={{ category }}&view=market">Market</a>
      {% if trading_enabled %}
      <a class="btn btn-sm btn-outline-success" href="/authorized">&#9889; Authorized</a>
      <a class="btn btn-sm btn-outline-success" href="/trades">&#128200; Trades</a>
      <a class="btn btn-sm btn-outline-success" href="/attempts">&#128269; Attempts</a>
      {% endif %}
    </div>
  </div>

  <!-- Table -->
  <div class="card rounded-top-0 border-top-0">
    <div class="table-responsive">
      <table class="table table-sm table-hover align-middle mb-0">
        <thead class="table-dark">
          {% if view_mode == 'market' %}
          <tr>
            <th style="width:90px">Last Seen</th>
            <th>Market</th>
            <th style="width:70px">Alerts</th>
            <th style="width:95px">Latest net</th>
            <th style="width:95px">Best net</th>
            <th style="width:70px">Vol 24h</th>
            <th style="width:90px">Sources</th>
            <th style="width:90px">Trade</th>
            <th style="width:52px"></th>
          </tr>
          {% else %}
          <tr>
            <th style="width:80px">Time</th>
            <th>Market</th>
            <th>Legs</th>
            <th style="width:80px">Sum asks</th>
            <th style="width:90px">Net profit</th>
            <th style="width:70px">Vol 24h</th>
            <th style="width:60px">Src</th>
            <th style="width:90px">Trade</th>
            <th style="width:52px"></th>
          </tr>
          {% endif %}
        </thead>
        <tbody>
          {% if view_mode == 'market' %}
          {% for r in rows %}
          <tr class="{{ 'authorized-row' if r.authorized }}">
            <td class="text-muted text-nowrap">{{ r.time_ago }}</td>
            <td class="q">
              <a href="{{ r.kalshi_url }}" target="_blank" rel="noopener"
                 class="text-decoration-none text-dark">{{ r.question }}</a>
              {% if r.closes_in.label %}
                <br><span class="small {{ r.closes_in.css }}">{{ r.closes_in.label }}</span>
              {% endif %}
              {% if r.authorized %}
                <br><span class="badge bg-success">&#9889; AUTHORIZED</span>
              {% endif %}
            </td>
            <td class="text-muted text-nowrap">
              {{ r.count }}
              <button class="btn btn-sm btn-link text-dark p-0 ms-1" type="button"
                      title="Show alerts" data-bs-toggle="collapse"
                      data-bs-target="#alerts-{{ r.row_id }}" aria-expanded="false"
                      aria-controls="alerts-{{ r.row_id }}">+</button>
            </td>
            <td class="{{ 'profit-pos' if r.latest_net_profit >= 0.005 else ('profit-near' if r.latest_net_profit >= 0 else 'profit-neg') }}">
              {{ "%+.3f%%" | format(r.latest_net_profit * 100) }}
            </td>
            <td class="{{ 'profit-pos' if r.best_net_profit >= 0.005 else ('profit-near' if r.best_net_profit >= 0 else 'profit-neg') }}">
              {{ "%+.3f%%" | format(r.best_net_profit * 100) }}
            </td>
            <td class="text-muted text-nowrap">
              {% if r.volume_24h %}{{ "%.0f" | format(r.volume_24h) }}{% else %}—{% endif %}
            </td>
            <td>
              {% for s in r.sources %}
                <span class="badge {{ 'bg-info text-dark' if s == 'WS' else 'bg-secondary' }}">{{ s }}</span>
              {% endfor %}
            </td>
            <td class="text-nowrap">
              {% if r.trade_id %}
                {% if r.trade_status == 'complete' %}
                  <a href="/trades" class="badge bg-success text-decoration-none">&#10003; complete</a>
                {% elif r.trade_status in ('phase1_placed', 'phase2_placed', 'pending') %}
                  <a href="/trades" class="badge bg-warning text-dark text-decoration-none">&#8635; in progress</a>
                {% elif r.trade_status in ('unwind_retry', 'unwind_limit', 'unwind_market', 'unwind_hold') %}
                  <a href="/trades" class="badge bg-warning text-dark text-decoration-none">&#9100; unwinding</a>
                {% elif r.trade_status == 'aborted' %}
                  <a href="/trades" class="badge bg-danger text-decoration-none">&#10007; aborted</a>
                {% else %}
                  <a href="/trades" class="badge bg-primary text-decoration-none">attempted</a>
                {% endif %}
              {% elif r.trader_invoked_at %}
                <span class="badge bg-secondary">preflight fail</span>
              {% else %}
                <span class="text-muted">—</span>
              {% endif %}
            </td>
            <td class="text-nowrap">
              <button class="btn btn-sm btn-link text-primary p-0 me-1" title="Live prices"
                      onclick="showLive({{ r.row_id }})">&#8635;</button>
              {% if trading_enabled and category == 'opportunity' %}
                {% if r.authorized %}
                <form method="post" action="/authorize/{{ r.row_id }}" style="margin:0;display:inline">
                  <input type="hidden" name="cat" value="{{ category }}">
                  <input type="hidden" name="view" value="{{ view_mode }}">
                  <input type="hidden" name="authorize" value="0">
                  <button type="submit" class="btn btn-sm btn-link text-success p-0 me-1"
                          title="Deauthorize trade" style="font-size:1rem">&#9889;</button>
                </form>
                {% else %}
                <form method="post" action="/authorize/{{ r.row_id }}" style="margin:0;display:inline">
                  <input type="hidden" name="cat" value="{{ category }}">
                  <input type="hidden" name="view" value="{{ view_mode }}">
                  <input type="hidden" name="authorize" value="1">
                  <button type="submit" class="btn btn-sm btn-link text-muted p-0 me-1"
                          title="Authorize for trading" style="font-size:1rem">&#9889;</button>
                </form>
                {% endif %}
              {% endif %}
              {% if r.latest_category != 'opportunity' %}
              <form method="post" action="/set-category/{{ r.row_id }}" style="margin:0;display:inline">
                <input type="hidden" name="cat" value="{{ category }}">
                <input type="hidden" name="view" value="{{ view_mode }}">
                <input type="hidden" name="new_cat" value="opportunity">
                <button type="submit" class="btn btn-sm btn-link text-success p-0 me-1" title="Promote to opportunity">&#8679;</button>
              </form>
              {% endif %}
              {% if r.latest_category != 'ignored' %}
              <form method="post" action="/set-category/{{ r.row_id }}" style="margin:0;display:inline">
                <input type="hidden" name="cat" value="{{ category }}">
                <input type="hidden" name="view" value="{{ view_mode }}">
                <input type="hidden" name="new_cat" value="ignored">
                <button type="submit" class="btn btn-sm btn-link text-secondary p-0 me-1" title="Ignore">&#8856;</button>
              </form>
              {% endif %}
              <form method="post" action="/delete/{{ r.row_id }}" style="margin:0;display:inline"
                    onsubmit="return confirm('Delete this row?');">
                <input type="hidden" name="cat" value="{{ category }}">
                <input type="hidden" name="view" value="{{ view_mode }}">
                <button type="submit" class="btn btn-sm btn-link text-danger p-0" title="Delete">&times;</button>
              </form>
            </td>
          </tr>
          <tr class="collapse" id="alerts-{{ r.row_id }}">
            <td colspan="9" class="bg-light">
              <div class="small text-muted mb-2">All alerts for this market (newest first)</div>
              <div class="table-responsive">
                <table class="table table-sm mb-0">
                  <thead>
                    <tr>
                      <th style="width:70px">Time</th>
                      <th style="width:120px">Category</th>
                      <th style="width:90px">Net</th>
                      <th style="width:80px">Sum asks</th>
                      <th style="width:70px">Src</th>
                      <th style="width:70px">Vol</th>
                    </tr>
                  </thead>
                  <tbody>
                    {% for a in r.alerts %}
                    <tr>
                      <td class="text-muted">{{ a.time_ago }}</td>
                      <td>{{ a.category }}</td>
                      <td class="{{ 'profit-pos' if a.net_profit >= 0.005 else ('profit-near' if a.net_profit >= 0 else 'profit-neg') }}">
                        {{ "%+.3f%%" | format(a.net_profit * 100) }}
                      </td>
                      <td>{{ "%.4f" | format(a.sum_asks) }}</td>
                      <td>{{ a.source }}</td>
                      <td class="text-muted">{% if a.volume_24h %}{{ "%.0f" | format(a.volume_24h) }}{% else %}—{% endif %}</td>
                    </tr>
                    {% endfor %}
                  </tbody>
                </table>
              </div>
            </td>
          </tr>
          {% endfor %}
          {% else %}
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
              {% elif r.category == 'likely_resolved' %}
                <br><span class="badge" style="background:#6c3483">LIKELY RESOLVED</span>
              {% elif r.category == 'ignored' %}
                <br><span class="badge bg-secondary">IGNORED</span>
              {% endif %}
              {% if r.authorized %}
                <br><span class="badge bg-success">&#9889; AUTHORIZED</span>
              {% endif %}
            </td>
            <td class="legs">
              {% set legs = r.legs | default([]) %}
              {% for leg in legs[:10] %}
              <div>
                <span class="text-muted">{{ leg.outcome[:18] }}</span>
                ask=<strong>{{ "%.4f" | format(leg.price) }}</strong>
                <span class="text-muted">({{ "%.0f" | format(leg.size) }})</span>
              </div>
              {% endfor %}
              {% if legs | length > 10 %}
              <div class="text-muted">…and {{ legs | length - 10 }} more</div>
              {% endif %}
            </td>
            <td>{{ "%.4f" | format(r.sum_asks) }}</td>
            <td class="{{ 'profit-pos' if r.net_profit >= 0.005 else ('profit-near' if r.net_profit >= 0 else 'profit-neg') }}">
              {{ "%+.3f%%" | format(r.net_profit * 100) }}
            </td>
            <td class="text-muted text-nowrap">
              {% if r.volume_24h %}{{ "%.0f" | format(r.volume_24h) }}{% else %}—{% endif %}
            </td>
            <td>
              <span class="badge {{ 'bg-info text-dark' if r.source == 'WS' else 'bg-secondary' }}">
                {{ r.source }}
              </span>
            </td>
            <td class="text-nowrap">
              {% if r.trade_id %}
                {% if r.trade_status == 'complete' %}
                  <a href="/trades" class="badge bg-success text-decoration-none">&#10003; complete</a>
                {% elif r.trade_status in ('phase1_placed', 'phase2_placed', 'pending') %}
                  <a href="/trades" class="badge bg-warning text-dark text-decoration-none">&#8635; in progress</a>
                {% elif r.trade_status in ('unwind_retry', 'unwind_limit', 'unwind_market', 'unwind_hold') %}
                  <a href="/trades" class="badge bg-warning text-dark text-decoration-none">&#9100; unwinding</a>
                {% elif r.trade_status == 'aborted' %}
                  <a href="/trades" class="badge bg-danger text-decoration-none">&#10007; aborted</a>
                {% elif r.trade_status == 'preflight_failed' %}
                  <a href="/trades" class="badge bg-secondary text-decoration-none">preflight fail</a>
                {% elif r.trade_status %}
                  <a href="/trades" class="badge bg-secondary text-decoration-none">{{ r.trade_status }}</a>
                {% else %}
                  <a href="/trades" class="badge bg-primary text-decoration-none">attempted</a>
                {% endif %}
              {% elif r.trader_invoked_at %}
                <span class="badge bg-secondary" title="Invoked at {{ r.trader_invoked_at }}">preflight fail</span>
              {% else %}
                <span class="text-muted">—</span>
              {% endif %}
            </td>
            <td class="text-nowrap">
              <button class="btn btn-sm btn-link text-primary p-0 me-1" title="Live prices"
                      onclick="showLive({{ r.row_id }})">&#8635;</button>
              {% if r.category == 'opportunity' and trading_enabled %}
                {% if r.authorized %}
                <form method="post" action="/authorize/{{ r.row_id }}" style="margin:0;display:inline">
                  <input type="hidden" name="cat" value="{{ category }}">
                  <input type="hidden" name="view" value="{{ view_mode }}">
                  <input type="hidden" name="authorize" value="0">
                  <button type="submit" class="btn btn-sm btn-link text-success p-0 me-1"
                          title="Deauthorize trade" style="font-size:1rem">&#9889;</button>
                </form>
                {% else %}
                <form method="post" action="/authorize/{{ r.row_id }}" style="margin:0;display:inline">
                  <input type="hidden" name="cat" value="{{ category }}">
                  <input type="hidden" name="view" value="{{ view_mode }}">
                  <input type="hidden" name="authorize" value="1">
                  <button type="submit" class="btn btn-sm btn-link text-muted p-0 me-1"
                          title="Authorize for trading" style="font-size:1rem">&#9889;</button>
                </form>
                {% endif %}
              {% endif %}
              {% if r.category != 'opportunity' %}
              <form method="post" action="/set-category/{{ r.row_id }}" style="margin:0;display:inline">
                <input type="hidden" name="cat" value="{{ category }}">
                <input type="hidden" name="view" value="{{ view_mode }}">
                <input type="hidden" name="new_cat" value="opportunity">
                <button type="submit" class="btn btn-sm btn-link text-success p-0 me-1" title="Promote to opportunity">&#8679;</button>
              </form>
              {% endif %}
              {% if r.category != 'ignored' %}
              <form method="post" action="/set-category/{{ r.row_id }}" style="margin:0;display:inline">
                <input type="hidden" name="cat" value="{{ category }}">
                <input type="hidden" name="view" value="{{ view_mode }}">
                <input type="hidden" name="new_cat" value="ignored">
                <button type="submit" class="btn btn-sm btn-link text-secondary p-0 me-1" title="Ignore">&#8856;</button>
              </form>
              {% endif %}
              <form method="post" action="/delete/{{ r.row_id }}" style="margin:0;display:inline"
                    onsubmit="return confirm('Delete this row?');">
                <input type="hidden" name="cat" value="{{ category }}">
                <input type="hidden" name="view" value="{{ view_mode }}">
                <button type="submit" class="btn btn-sm btn-link text-danger p-0" title="Delete">&times;</button>
              </form>
            </td>
          </tr>
          {% endfor %}
          {% endif %}
          {% if not rows %}
          <tr>
            <td colspan="9" class="text-center text-muted py-5">No records yet.</td>
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

_TRADING_NAV = """
  <div class="d-flex justify-content-between align-items-center mb-3">
    <h5 class="mb-0 fw-bold">{{ page_title }}</h5>
    <div class="d-flex align-items-center gap-3">
      <span class="text-muted small">{{ now }}</span>
      <button class="btn btn-sm btn-link text-muted p-0" onclick="location.reload()" title="Refresh">&#8635;</button>
    </div>
  </div>
  <ul class="nav nav-tabs mb-3">
    <li class="nav-item">
      <a class="nav-link" href="/?cat=opportunity">&#128270; Opportunities</a>
    </li>
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if active_tab == 'authorized' }}" href="/authorized">&#9889; Authorized</a>
    </li>
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if active_tab == 'trades' }}" href="/trades">&#128200; Trades</a>
    </li>
    <li class="nav-item">
      <a class="nav-link {{ 'active fw-semibold' if active_tab == 'attempts' }}" href="/attempts">&#128269; Attempts</a>
    </li>
  </ul>
"""

AUTHORIZED_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kalshi Arb — Authorized</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { font-size: 0.875rem; background: #f8f9fa; }
    .profit-pos { color: #198754; font-weight: 700; }
    .profit-neg { color: #6c757d; }
    tr.attempting { background: #f0fff4 !important; }
  </style>
</head>
<body>
<div class="container-fluid py-3 px-4">
  """ + _TRADING_NAV + """
  <div class="card">
    <div class="table-responsive">
      <table class="table table-sm table-hover align-middle mb-0">
        <thead class="table-dark">
          <tr>
            <th style="width:50px">ID</th>
            <th>Market</th>
            <th style="width:90px">Net profit</th>
            <th style="width:110px">Closes in</th>
            <th style="width:130px">Trade status</th>
            <th style="width:60px"></th>
          </tr>
        </thead>
        <tbody>
          {% for r in rows %}
          <tr class="{{ 'attempting' if not r.trade_id }}">
            <td class="text-muted">{{ r.opp_id }}</td>
            <td>
              <a href="{{ r.kalshi_url }}" target="_blank" rel="noopener"
                 class="text-decoration-none text-dark fw-semibold">{{ r.title }}</a>
              {% if r.closes_in.label %}
                <br><span class="small {{ r.closes_in.css }}">{{ r.closes_in.label }}</span>
              {% endif %}
            </td>
            <td class="{{ 'profit-pos' if r.net_profit >= 0.005 else 'profit-neg' }}">
              {{ "%+.3f%%" | format(r.net_profit * 100) }}
            </td>
            <td>
              {% if r.closes_in.label %}
                <span class="small {{ r.closes_in.css }}">{{ r.closes_in.label }}</span>
              {% else %}
                <span class="text-muted">—</span>
              {% endif %}
            </td>
            <td>
              {% if not r.trade_id %}
                <span class="badge bg-success">attempting…</span>
              {% else %}
                <span class="text-muted small">trade #{{ r.trade_id }}</span>
                {% if r.trade_status == 'complete' %}
                  <span class="badge bg-success ms-1">complete</span>
                {% elif r.trade_status == 'aborted' %}
                  <span class="badge bg-danger ms-1">aborted</span>
                {% elif r.trade_status and r.trade_status.startswith('unwind') %}
                  <span class="badge bg-warning text-dark ms-1">{{ r.trade_status }}</span>
                {% elif r.trade_status %}
                  <span class="badge bg-primary ms-1">{{ r.trade_status }}</span>
                {% endif %}
              {% endif %}
            </td>
            <td>
              <form method="post" action="/authorize/{{ r.opp_id }}" style="margin:0">
                <input type="hidden" name="authorize" value="0">
                <input type="hidden" name="next" value="/authorized">
                <button type="submit" class="btn btn-sm btn-outline-danger p-0 px-1"
                        title="Deauthorize">&#9889; off</button>
              </form>
            </td>
          </tr>
          {% endfor %}
          {% if not rows %}
          <tr>
            <td colspan="6" class="text-center text-muted py-5">No authorized opportunities.</td>
          </tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
  <div class="text-muted small mt-2">{{ rows | length }} authorized opportunity/ies. Auto-refreshes every 5s.</div>
</div>
<script>setTimeout(function() { location.reload(); }, 5000);</script>
</body>
</html>"""

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
    tr.attempting { background: #f0fff4 !important; }
  </style>
</head>
<body>
<div class="container-fluid py-3 px-4">
  """ + _TRADING_NAV + """
  <!-- Stats -->
  <div class="row g-2 mb-3">
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Attempting</div>
        <div class="fs-5 fw-bold text-success">{{ stats.attempting }}</div>
      </div>
    </div>
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
        <div class="text-muted small">Aborted</div>
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
            <th style="width:120px">Status</th>
            <th>Opportunity</th>
            <th style="width:90px">Started</th>
            <th style="width:90px">Completed</th>
            <th>Legs / Fills</th>
            <th style="width:90px">Net PnL</th>
            <th>Notes</th>
            <th style="width:70px"></th>
          </tr>
        </thead>
        <tbody>
          {% for t in trades %}
          <tr class="{{ 'attempting' if t.is_attempting }}">
            <td class="text-muted">{{ t.id or '—' }}</td>
            <td>
              {% if t.is_attempting %}
                <span class="text-muted small">—</span>
              {% elif t.demo_mode %}
                <span class="badge bg-info text-dark">DEMO</span>
              {% else %}
                <span class="badge bg-danger">LIVE</span>
              {% endif %}
            </td>
            <td>
              {% if t.is_attempting %}
                <span class="badge bg-success">attempting…</span>
              {% elif t.status == 'settled' %}
                <span class="badge bg-success">&#9654; settled early</span>
              {% elif t.status == 'complete' %}
                <span class="badge bg-success">complete</span>
              {% elif t.status in ('phase1_placed', 'phase2_placed') %}
                <span class="badge bg-primary">{{ t.status }}</span>
              {% elif t.status == 'phase1_filled' %}
                <span class="badge bg-info text-dark">phase1 filled</span>
              {% elif t.status == 'aborted' %}
                <span class="badge bg-danger">aborted</span>
              {% elif t.status.startswith('unwind') %}
                <span class="badge bg-warning text-dark">{{ t.status }}</span>
              {% else %}
                <span class="badge bg-secondary">{{ t.status }}</span>
              {% endif %}
            </td>
            <td class="small-mono">
              {% if t.kalshi_url %}
                <a href="{{ t.kalshi_url }}" target="_blank" rel="noopener"
                   class="text-decoration-none text-dark">{{ t.opp_title }}</a>
              {% else %}{{ t.opp_title }}{% endif %}
            </td>
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
              {% if t.status == 'settled' and t.exit_pnl is not none %}
                <span class="text-success fw-bold"
                      title="Theoretical at expiry: {{ '%+.4f' | format(t.net_pnl) if t.net_pnl is not none else '?' }}">
                  {{ "%+.4f" | format(t.exit_pnl) }}
                  <small class="text-muted fw-normal">early</small>
                </span>
              {% elif t.net_pnl is not none %}
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
            <td>
              {% if t.is_attempting and t.opp_id %}
              <form method="post" action="/authorize/{{ t.opp_id }}" style="margin:0;display:inline">
                <input type="hidden" name="authorize" value="0">
                <input type="hidden" name="next" value="/trades">
                <button type="submit" class="btn btn-sm btn-outline-danger p-0 px-1"
                        title="Deauthorize">&#9889; off</button>
              </form>
              {% endif %}
              {% if not t.is_attempting and t.id %}
              <form method="post" action="/delete-trade/{{ t.id }}" style="margin:0;display:inline"
                    onsubmit="return confirm('Delete trade #{{ t.id }}?');">
                <button type="submit" class="btn btn-sm btn-link text-danger p-0"
                        title="Delete">&times;</button>
              </form>
              {% endif %}
            </td>
          </tr>
          {% endfor %}
          {% if not trades %}
          <tr>
            <td colspan="10" class="text-center text-muted py-5">No trades yet.</td>
          </tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
  <div class="text-muted small mt-2">Showing {{ trades | length }} rows. Auto-refreshes every 10s.</div>

</div>
<script>
setTimeout(function() { location.reload(); }, 10000);
</script>
</body>
</html>"""

ATTEMPTS_TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Kalshi Arb — Attempts</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { font-size: 0.875rem; background: #f8f9fa; }
    .stat-card { min-width: 130px; }
    td.small-mono { font-size: 0.78rem; font-family: monospace; }
  </style>
</head>
<body>
<div class="container-fluid py-3 px-4">
  """ + _TRADING_NAV + """
  <div class="d-flex justify-content-end align-items-center mb-2">
    <form method="post" action="/clear-attempts"
          onsubmit="return confirm('Clear ALL event-driven attempts? This cannot be undone.');">
      <button type="submit" class="btn btn-sm btn-outline-danger">Clear All Attempts</button>
    </form>
  </div>
  <div class="row g-2 mb-3">
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Event kicks</div>
        <div class="fs-5 fw-bold">{{ stats.total }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Preflight failed</div>
        <div class="fs-5 fw-bold text-danger">{{ stats.preflight_failed }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Order attempted</div>
        <div class="fs-5 fw-bold text-warning">{{ stats.order_attempted }}</div>
      </div>
    </div>
    <div class="col-auto">
      <div class="card stat-card text-center px-3 py-2">
        <div class="text-muted small">Completed</div>
        <div class="fs-5 fw-bold text-success">{{ stats.complete }}</div>
      </div>
    </div>
  </div>
  <div class="card mb-3">
    <div class="card-header py-2 fw-semibold">Top preflight reasons</div>
    <div class="card-body py-2">
      {% if reasons %}
        {% for r in reasons %}
          <span class="badge bg-secondary me-2 mb-2">{{ r.reason }} ({{ r.n }})</span>
        {% endfor %}
      {% else %}
        <span class="text-muted">No preflight failures logged.</span>
      {% endif %}
    </div>
  </div>
  <div class="card">
    <div class="table-responsive">
      <table class="table table-sm table-hover align-middle mb-0">
        <thead class="table-dark">
          <tr>
            <th style="width:60px">#</th>
            <th style="width:90px">When</th>
            <th>Market</th>
            <th style="width:110px">Preflight</th>
            <th style="width:100px">Order?</th>
            <th style="width:160px">Reason</th>
            <th style="width:120px">Final status</th>
            <th style="width:70px">Trade</th>
            <th style="width:52px"></th>
          </tr>
        </thead>
        <tbody>
          {% for a in attempts %}
          <tr>
            <td class="text-muted">{{ a.id }}</td>
            <td class="text-muted text-nowrap">{{ a.when }}</td>
            <td>
              {% if a.kalshi_url %}
                <a href="{{ a.kalshi_url }}" target="_blank" rel="noopener"
                   class="text-decoration-none text-dark">{{ a.market_title or a.market_key }}</a>
              {% else %}{{ a.market_title or a.market_key }}{% endif %}
              {% if a.market_key %}
              <div class="text-muted small">{{ a.market_key }}</div>
              {% endif %}
            </td>
            <td>
              {% if a.preflight_ok == 1 %}
                <span class="badge bg-success">ok</span>
              {% elif a.preflight_ok == 0 %}
                <span class="badge bg-danger">failed</span>
              {% else %}
                <span class="badge bg-secondary">n/a</span>
              {% endif %}
            </td>
            <td>
              {% if a.order_attempted %}
                <span class="badge bg-warning text-dark">yes</span>
              {% else %}
                <span class="badge bg-secondary">no</span>
              {% endif %}
            </td>
            <td class="text-muted">{{ a.preflight_reason or "—" }}</td>
            <td>{{ a.final_status or "—" }}</td>
            <td class="text-muted">{{ a.trade_id or "—" }}</td>
            <td class="text-nowrap">
              <form method="post" action="/delete-attempt/{{ a.id }}" style="margin:0;display:inline"
                    onsubmit="return confirm('Delete this attempt row?');">
                <button type="submit" class="btn btn-sm btn-link text-danger p-0" title="Delete">&times;</button>
              </form>
            </td>
          </tr>
          {% endfor %}
          {% if not attempts %}
          <tr><td colspan="9" class="text-center text-muted py-5">No event-driven attempts yet.</td></tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
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


_KALSHI_HOST = "demo.kalshi.co" if DEMO_MODE else "kalshi.com"


def _kalshi_url(event_ticker: str, ticker: str, event_slug: str = "") -> str:
    """Build the canonical Kalshi market URL from stored fields."""
    def _event_root(s: str) -> str:
        s = (s or "").strip()
        # Normalize dated series/event tickers like KXBTC15M-26MAR230845 -> KXBTC15M.
        return re.sub(r"-\d.*$", "", s)

    et = event_ticker or ticker
    et_from_ticker = False
    if et.upper() == ticker.upper():
        et_from_ticker = True
        et_lower = _event_root(ticker).lower()
    else:
        et_lower = _event_root(et).lower()
    if "spread" in et_lower:
        et_lower = re.sub(r"spread", "game", et_lower)
        return f"https://{_KALSHI_HOST}/markets/{et_lower}"
    slug = (event_slug or "").strip().lower()
    et_norm = (event_ticker or "").strip().lower()
    # Slugs that just mirror event ticker aren't useful URL path segments.
    if slug and slug not in {et_norm, _event_root(event_ticker).lower(), (ticker or "").lower()}:
        return f"https://{_KALSHI_HOST}/markets/{et_lower}/{event_slug}/{ticker.lower()}"
    if ticker and (not et_from_ticker):
        # Distinct event + market tickers: placeholder middle segment still resolves.
        return f"https://{_KALSHI_HOST}/markets/{et_lower}/market/{ticker.lower()}"
    if ticker and et_from_ticker and re.search(r"-\d", ticker):
        # Single ticker contains date/series suffix (e.g. ...-26MAR...): treat as market leaf.
        return f"https://{_KALSHI_HOST}/markets/{et_lower}/market/{ticker.lower()}"
    else:
        return f"https://{_KALSHI_HOST}/markets/{et_lower}"


def _url_leaf_ticker(ticker: str, event_ticker: str, outcome_tickers_raw) -> str:
    """
    Pick a market ticker for deep-linking.
    When stored ticker is event-level, use a single outcome ticker if available.
    """
    et = event_ticker or ticker or ""
    tk = ticker or ""
    outcome_tickers = []
    try:
        if isinstance(outcome_tickers_raw, str):
            outcome_tickers = json.loads(outcome_tickers_raw or "[]")
        elif isinstance(outcome_tickers_raw, list):
            outcome_tickers = outcome_tickers_raw
    except Exception:
        outcome_tickers = []

    if (not tk or tk.upper() == et.upper()) and len(outcome_tickers) == 1:
        return outcome_tickers[0]
    return tk or et


def _active_authorized_market_keys() -> set[str]:
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        rows = con.execute("""
            SELECT market_key
            FROM authorized_markets
            WHERE active = 1
              AND successful_at IS NULL
        """).fetchall()
        con.close()
        return {r["market_key"] for r in rows if r["market_key"]}
    except Exception:
        return set()


def _get_rows(category=None, limit: int = 200) -> list:
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        q = """
            SELECT o.*, t.status AS trade_status
            FROM opportunities o
            LEFT JOIN trades t ON o.trade_id = t.id
        """
        params: list = []
        if category:
            q += " WHERE o.category = ?"
            params.append(category)
        q += " ORDER BY o.detected_at DESC LIMIT ?"
        params.append(limit)
        raw = con.execute(q, params).fetchall()
        con.close()
    except Exception:
        return []

    active_auth = _active_authorized_market_keys()
    rows = []
    for r in raw:
        try:
            outcomes = json.loads(r["outcomes"])
            prices   = json.loads(r["ask_prices"])
            sizes    = json.loads(r["ask_sizes"])
            outcome_tickers = json.loads(r["outcome_tickers"] or "[]")
            legs = [
                {"outcome": o, "price": float(p), "size": float(s)}
                for o, p, s in zip(outcomes, prices, sizes)
            ]
        except Exception:
            outcome_tickers = []
            legs = []
        ticker       = r["ticker"]
        event_ticker = r["event_ticker"] or r["ticker"]
        raw_slug     = r["event_slug"] if "event_slug" in r.keys() and r["event_slug"] else ""
        link_ticker = _url_leaf_ticker(ticker, event_ticker, outcome_tickers)
        kalshi_url = _kalshi_url(event_ticker, link_ticker, raw_slug)
        market_key = r["market_key"] if "market_key" in r.keys() and r["market_key"] else canonical_market_key(
            ticker, event_ticker, r["outcome_tickers"] if "outcome_tickers" in r.keys() else []
        )
        rows.append({
            "row_id":        r["id"],
            "detected_at":   r["detected_at"],
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
            "authorized":         market_key in active_auth,
            "trade_id":           r["trade_id"] if "trade_id" in r.keys() else None,
            "trade_status":       r["trade_status"] if "trade_status" in r.keys() else None,
            "trader_invoked_at":  r["trader_invoked_at"] if "trader_invoked_at" in r.keys() else None,
            "volume_24h":         r["volume_24h"] if "volume_24h" in r.keys() else 0,
            "market_key":         market_key,
        })
    return rows


def _get_market_groups(category=None, limit: int = 400) -> list:
    rows = _get_rows(category=category, limit=limit)
    groups: dict[str, dict] = {}
    for r in rows:
        mk = r.get("market_key") or str(r.get("row_id"))
        g = groups.get(mk)
        if g is None:
            groups[mk] = {
                "latest": r,
                "count": 1,
                "best_net_profit": r["net_profit"],
                "sources": {r["source"]},
                "alerts": [r],
            }
            continue
        g["count"] += 1
        g["best_net_profit"] = max(g["best_net_profit"], r["net_profit"])
        g["sources"].add(r["source"])
        g["alerts"].append(r)
        if r.get("detected_at", "") > g["latest"].get("detected_at", ""):
            g["latest"] = r

    result = []
    for mk, g in groups.items():
        latest = g["latest"]
        alerts_sorted = sorted(
            g["alerts"],
            key=lambda a: a.get("detected_at") or "",
            reverse=True,
        )
        result.append({
            "market_key": mk,
            "row_id": latest["row_id"],
            "detected_at": latest.get("detected_at"),
            "time_ago": latest["time_ago"],
            "question": latest["question"],
            "kalshi_url": latest["kalshi_url"],
            "closes_in": latest["closes_in"],
            "authorized": latest["authorized"],
            "trade_id": latest["trade_id"],
            "trade_status": latest["trade_status"],
            "trader_invoked_at": latest["trader_invoked_at"],
            "volume_24h": latest["volume_24h"],
            "latest_net_profit": latest["net_profit"],
            "best_net_profit": g["best_net_profit"],
            "count": g["count"],
            "sources": sorted(g["sources"]),
            "latest_category": latest.get("category", ""),
            "alerts": alerts_sorted,
        })

    result.sort(key=lambda x: x.get("detected_at") or "", reverse=True)
    return result


def _get_stats() -> dict:
    try:
        con = sqlite3.connect(DB_PATH)
        total         = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        opps          = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='opportunity'").fetchone()[0]
        near_miss     = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='near_miss'").fetchone()[0]
        cumulative    = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='cumulative'").fetchone()[0]
        non_exhaustive = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='non_exhaustive'").fetchone()[0]
        spread_market  = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='spread_market'").fetchone()[0]
        ignored          = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='ignored'").fetchone()[0]
        likely_resolved  = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='likely_resolved'").fetchone()[0]
        last          = con.execute("SELECT MAX(detected_at) FROM opportunities").fetchone()[0]
        tracked_cache = con.execute("SELECT COUNT(*) FROM markets_cache").fetchone()[0]
        try:
            ws_row = con.execute("SELECT count, started_at FROM ws_counter WHERE id = 1").fetchone()
        except sqlite3.OperationalError:
            ws_row = None
        try:
            ws_watched_row = con.execute("SELECT count, started_at FROM ws_counter_watched WHERE id = 1").fetchone()
        except sqlite3.OperationalError:
            ws_watched_row = None
        try:
            metric_rows = con.execute("""
                SELECT metric, count
                FROM metric_counters
                WHERE metric IN (
                    'ws_gate_reject_missing_ts',
                    'ws_gate_reject_age',
                    'ws_gate_reject_skew',
                    'preflight_reject_stale_snapshot',
                    'ws_gate_pass'
                )
            """).fetchall()
        except sqlite3.OperationalError:
            metric_rows = []
        con.close()
        # tracked markets count from arb_bot (same-process mode), else fallback to DB cache
        try:
            import arb_bot
            tracked = len(getattr(arb_bot, "markets_by_ticker", {}) or {})
            if tracked <= 0:
                tracked = tracked_cache
        except Exception:
            tracked = tracked_cache
        min_vol = int(MIN_VOLUME_24H) if MIN_VOLUME_24H > 0 else "off"
        ws_count = int(ws_row[0]) if ws_row and ws_row[0] is not None else 0
        ws_since = _time_ago(ws_row[1]) if ws_row and ws_row[1] else "—"
        ws_watched_count = int(ws_watched_row[0]) if ws_watched_row and ws_watched_row[0] is not None else 0
        ws_watched_since = _time_ago(ws_watched_row[1]) if ws_watched_row and ws_watched_row[1] else "—"
        metric_map = {r[0]: int(r[1] or 0) for r in metric_rows}
        ws_gate_reject_missing_ts = metric_map.get("ws_gate_reject_missing_ts", 0)
        ws_gate_reject_age = metric_map.get("ws_gate_reject_age", 0)
        ws_gate_reject_skew = metric_map.get("ws_gate_reject_skew", 0)
        preflight_reject_stale_snapshot = metric_map.get("preflight_reject_stale_snapshot", 0)
        ws_gate_pass = metric_map.get("ws_gate_pass", 0)
        ws_gate_reject_total = (
            ws_gate_reject_missing_ts
            + ws_gate_reject_age
            + ws_gate_reject_skew
            + preflight_reject_stale_snapshot
        )
        return {"total": total, "opps": opps, "near_miss": near_miss,
                "cumulative": cumulative, "non_exhaustive": non_exhaustive,
                "spread_market": spread_market, "ignored": ignored,
                "likely_resolved": likely_resolved, "last_seen_ago": _time_ago(last),
                "tracked_markets": tracked, "min_volume_24h": min_vol,
                "ws_count": ws_count, "ws_since": ws_since,
                "ws_watched_count": ws_watched_count, "ws_watched_since": ws_watched_since,
                "ws_gate_reject_missing_ts": ws_gate_reject_missing_ts,
                "ws_gate_reject_age": ws_gate_reject_age,
                "ws_gate_reject_skew": ws_gate_reject_skew,
                "preflight_reject_stale_snapshot": preflight_reject_stale_snapshot,
                "ws_gate_pass": ws_gate_pass,
                "ws_gate_reject_total": ws_gate_reject_total}
    except Exception:
        return {"total": 0, "opps": 0, "near_miss": 0,
                "cumulative": 0, "non_exhaustive": 0, "spread_market": 0,
                "ignored": 0, "likely_resolved": 0, "last_seen_ago": "—",
                "tracked_markets": "—", "min_volume_24h": "—",
                "ws_count": 0, "ws_since": "—",
                "ws_watched_count": 0, "ws_watched_since": "—",
                "ws_gate_reject_missing_ts": 0, "ws_gate_reject_age": 0,
                "ws_gate_reject_skew": 0, "preflight_reject_stale_snapshot": 0,
                "ws_gate_pass": 0, "ws_gate_reject_total": 0}


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    cat   = request.args.get("cat", "")
    view_mode = request.args.get("view", "time")
    if view_mode not in ("time", "market"):
        view_mode = "time"
    rows  = _get_market_groups(category=cat or None) if view_mode == "market" else _get_rows(category=cat or None)
    stats = _get_stats()
    now   = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return render_template_string(TEMPLATE, rows=rows, stats=stats,
                                  category=cat, now=now, view_mode=view_mode,
                                  trading_enabled=TRADING_ENABLED)


@app.route("/clear", methods=["POST"])
def clear_all():
    cat = request.form.get("cat", "")
    view_mode = request.form.get("view", "time")
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
    return redirect(url_for("index", cat=cat, view=view_mode) if cat else url_for("index", view=view_mode))


@app.route("/reset-counter/<string:name>", methods=["POST"])
def reset_counter(name: str):
    cat = request.form.get("cat", "")
    view_mode = request.form.get("view", "time")
    now = datetime.now(timezone.utc).isoformat()
    try:
        con = sqlite3.connect(DB_PATH)
        if name == "ws":
            con.execute(
                "UPDATE ws_counter SET count = 0, started_at = ?, updated_at = ? WHERE id = 1",
                (now, now),
            )
        elif name == "ws_watched":
            con.execute(
                "UPDATE ws_counter_watched SET count = 0, started_at = ?, updated_at = ? WHERE id = 1",
                (now, now),
            )
        elif name == "ws_gate_rejects":
            con.execute("""
                UPDATE metric_counters
                SET count = 0, started_at = ?, updated_at = ?
                WHERE metric IN (
                    'ws_gate_reject_missing_ts',
                    'ws_gate_reject_age',
                    'ws_gate_reject_skew',
                    'preflight_reject_stale_snapshot'
                )
            """, (now, now))
        con.commit()
        con.close()
    except Exception:
        pass
    return redirect(url_for("index", cat=cat, view=view_mode) if cat else url_for("index", view=view_mode))


@app.route("/set-category/<int:row_id>", methods=["POST"])
def set_category(row_id: int):
    cat     = request.form.get("cat", "")
    view_mode = request.form.get("view", "time")
    new_cat = request.form.get("new_cat", "")
    allowed = {"opportunity", "near_miss", "cumulative", "non_exhaustive",
               "spread_market", "ignored", "likely_resolved"}
    if new_cat in allowed:
        try:
            con = sqlite3.connect(DB_PATH)
            con.execute("UPDATE opportunities SET category = ? WHERE id = ?", (new_cat, row_id))
            con.commit()
            con.close()
        except Exception:
            pass
    return redirect(url_for("index", cat=cat, view=view_mode) if cat else url_for("index", view=view_mode))


@app.route("/delete/<int:row_id>", methods=["POST"])
def delete_row(row_id: int):
    cat = request.form.get("cat", "")
    view_mode = request.form.get("view", "time")
    try:
        con = sqlite3.connect(DB_PATH)
        if view_mode == "market":
            mk_row = con.execute(
                "SELECT market_key FROM opportunities WHERE id = ?",
                (row_id,),
            ).fetchone()
            market_key = mk_row[0] if mk_row else ""
            if market_key:
                con.execute("DELETE FROM opportunities WHERE market_key = ?", (market_key,))
            else:
                con.execute("DELETE FROM opportunities WHERE id = ?", (row_id,))
        else:
            con.execute("DELETE FROM opportunities WHERE id = ?", (row_id,))
        con.commit()
        con.close()
    except Exception:
        pass
    return redirect(url_for("index", cat=cat, view=view_mode) if cat else url_for("index", view=view_mode))


@app.route("/add-market", methods=["POST"])
def add_market():
    """Manually add a market (by event ticker or market ticker) to the opportunities table."""
    from fetcher import fetch_markets_by_event_or_ticker, fetch_market_prices
    from db import save_opportunity
    query = request.form.get("ticker", "").strip().upper()
    cat   = request.form.get("cat", "")
    view_mode = request.form.get("view", "time")
    back  = url_for("index", cat=cat, view=view_mode) if cat else url_for("index", view=view_mode)
    if not query:
        return redirect(back)
    try:
        markets = fetch_markets_by_event_or_ticker(query)
        if not markets:
            logger.warning("add-market: no markets found for %s", query)
            return redirect(back)

        tickers = [m["ticker"] for m in markets]
        prices  = fetch_market_prices(tickers)

        ask_prices, ask_sizes, outcomes, outcome_tickers = [], [], [], []
        for m in markets:
            p        = prices.get(m["ticker"], {})
            yes_ask  = p.get("yes_ask") if p.get("yes_ask") is not None else (m.get("seed_yes_ask") or 0.5)
            yes_size = p.get("yes_ask_size") or m.get("seed_yes_ask_size") or 0
            ask_prices.append(yes_ask)
            ask_sizes.append(yes_size)
            outcomes.append(m.get("title", m["ticker"]))
            outcome_tickers.append(m["ticker"])

        n            = len(markets)
        way_label    = f"[{n}-way]" if n > 1 else "[binary]"
        sum_asks     = sum(ask_prices)
        gross_profit = 1.0 - sum_asks
        total_fees   = sum(TAKER_FEE_COEFF * p * (1 - p) for p in ask_prices)
        net_profit   = gross_profit - total_fees
        first        = markets[0]
        event_ticker = first.get("event_ticker") or first["ticker"]

        save_opportunity({
            "ticker":          event_ticker,
            "event_ticker":    event_ticker,
            "title":           f"{way_label} {first.get('title', query)}",
            "outcomes":        outcomes,
            "outcome_tickers": outcome_tickers,
            "ask_prices":      ask_prices,
            "ask_sizes":       ask_sizes,
            "sum_asks":        sum_asks,
            "gross_profit":    gross_profit,
            "total_fees":      total_fees,
            "net_profit":      net_profit,
            "taker_fee_coeff": TAKER_FEE_COEFF,
            "source":          "MANUAL",
            "category":        "opportunity",
            "has_zero_size":   any(s <= 0 for s in ask_sizes),
            "close_time":      first.get("close_time"),
            "volume_24h":      min(m.get("volume_24h", 0) for m in markets),
            "event_slug":      "",
        })
    except Exception as exc:
        logger.error("add-market(%s) error: %s", query, exc)
    return redirect(back)


@app.route("/authorize/<int:row_id>", methods=["POST"])
def authorize_row(row_id: int):
    cat       = request.form.get("cat", "")
    view_mode = request.form.get("view", "time")
    authorize = request.form.get("authorize", "1") == "1"
    next_url  = request.form.get("next", "")
    try:
        set_authorized(row_id, authorize)
    except Exception:
        pass
    if next_url in ("/authorized", "/trades"):
        return redirect(next_url)
    return redirect(url_for("index", cat=cat, view=view_mode) if cat else url_for("index", view=view_mode))


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

    # Fallback: if cache misses, query live API for event/market and use returned tickers.
    if not tickers:
        try:
            from fetcher import fetch_markets_by_event_or_ticker
            live_markets = fetch_markets_by_event_or_ticker(event_ticker or row["ticker"])
            tickers = [m.get("ticker") for m in live_markets if m.get("ticker")]
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


def _get_authorized_opps() -> list[dict]:
    """Return latest opportunity per active authorized market."""
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        rows = con.execute("""
            SELECT o.id as opp_id, o.market_key, o.title, o.net_profit, o.close_time,
                   o.ticker, o.event_ticker, o.event_slug, o.outcome_tickers, o.trade_id,
                   t.status as trade_status
            FROM authorized_markets am
            JOIN opportunities o
              ON o.market_key = am.market_key
            JOIN (
                SELECT market_key, MAX(detected_at) AS max_detected
                FROM opportunities
                WHERE category='opportunity'
                GROUP BY market_key
            ) latest
              ON latest.market_key = o.market_key
             AND latest.max_detected = o.detected_at
            LEFT JOIN trades t ON o.trade_id = t.id
            WHERE am.active = 1
              AND am.successful_at IS NULL
            ORDER BY o.detected_at DESC
        """).fetchall()
        con.close()
    except Exception:
        return []

    result = []
    for r in rows:
        event_ticker = r["event_ticker"] or r["ticker"]
        ticker       = r["ticker"]
        event_slug   = r["event_slug"] if "event_slug" in r.keys() else ""
        link_ticker  = _url_leaf_ticker(ticker, event_ticker, r["outcome_tickers"] if "outcome_tickers" in r.keys() else [])
        kalshi_url   = _kalshi_url(event_ticker, link_ticker, event_slug or "")
        result.append({
            "opp_id":       r["opp_id"],
            "market_key":   r["market_key"],
            "title":        r["title"],
            "net_profit":   r["net_profit"],
            "closes_in":    _closes_in(r["close_time"]),
            "kalshi_url":   kalshi_url,
            "trade_id":     r["trade_id"],
            "trade_status": r["trade_status"],
        })
    return result


def _get_trades(limit: int = 100) -> tuple[list, dict]:
    """Return trade rows (with attempting rows prepended) and summary stats."""
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        raw = con.execute("""
            SELECT t.*, o.title as opp_title, o.id as opp_id,
                   o.ticker as opp_ticker, o.event_ticker, o.event_slug, o.outcome_tickers
            FROM trades t
            LEFT JOIN opportunities o ON t.opportunity_id = o.id
            ORDER BY t.id DESC
            LIMIT ?
        """, (limit,)).fetchall()
        attempting_raw = con.execute("""
            SELECT o.id, o.title, o.detected_at, o.ticker, o.event_ticker, o.event_slug, o.outcome_tickers
            FROM authorized_markets am
            JOIN opportunities o ON o.market_key = am.market_key
            JOIN (
                SELECT market_key, MAX(detected_at) AS max_detected
                FROM opportunities
                WHERE category='opportunity'
                GROUP BY market_key
            ) latest
              ON latest.market_key = o.market_key
             AND latest.max_detected = o.detected_at
            LEFT JOIN trades t ON o.trade_id = t.id
            WHERE am.active = 1
              AND am.successful_at IS NULL
              AND (o.trade_id IS NULL OR o.trade_id = 0 OR t.status = 'aborted')
        """).fetchall()
        con.close()
    except Exception:
        return [], {"attempting": 0, "total": 0, "complete": 0, "in_progress": 0,
                    "unwound": 0, "failed": 0, "total_net_pnl": 0.0}

    trades = []
    stats = {"attempting": len(attempting_raw), "total": 0, "complete": 0,
             "in_progress": 0, "unwound": 0, "failed": 0, "total_net_pnl": 0.0}

    # Prepend attempting rows at the top
    for r in attempting_raw:
        link_ticker = _url_leaf_ticker(r["ticker"], r["event_ticker"] or r["ticker"], r["outcome_tickers"] if "outcome_tickers" in r.keys() else [])
        trades.append({
            "id":            None,
            "status":        "attempting",
            "is_attempting": True,
            "demo_mode":     False,
            "opp_id":        r["id"],
            "opp_title":     r["title"],
            "kalshi_url":    _kalshi_url(r["event_ticker"] or r["ticker"], link_ticker, r["event_slug"] or ""),
            "started_ago":   _time_ago(r["detected_at"]),
            "completed_ago": "—",
            "legs":          [],
            "net_pnl":       None,
            "notes":         None,
            "unwind_reason": None,
        })

    in_progress_statuses = {"phase1_placed", "phase1_filled", "phase2_placed", "unwind_retry"}
    unwind_statuses      = {"unwind_limit", "unwind_market", "unwind_hold", "unwind_failed"}

    for r in raw:
        status = r["status"] or ""
        stats["total"] += 1
        if status in ("complete", "settled"):
            stats["complete"] += 1
        elif status in in_progress_statuses:
            stats["in_progress"] += 1
        elif status in unwind_statuses:
            stats["unwound"] += 1
        elif status == "aborted":
            stats["failed"] += 1

        # For settled trades, use exit_pnl as the realized figure
        exit_pnl     = r["exit_pnl"] if "exit_pnl" in r.keys() else None
        realized_pnl = exit_pnl if status == "settled" else r["net_pnl"]
        if realized_pnl is not None:
            stats["total_net_pnl"] += realized_pnl

        try:
            leg_tickers   = json.loads(r["leg_tickers"]   or "[]")
            target_prices = json.loads(r["target_prices"] or "[]")
            fill_prices_l = json.loads(r["fill_prices"]   or "[]")
            fill_counts_l = json.loads(r["fill_counts"]   or "[]")
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

        opp_ticker  = r["opp_ticker"] or ""
        opp_evt_tkr = r["event_ticker"] or opp_ticker
        opp_slug    = r["event_slug"] or ""
        link_ticker = _url_leaf_ticker(opp_ticker, opp_evt_tkr, r["outcome_tickers"] if "outcome_tickers" in r.keys() else [])
        trades.append({
            "id":            r["id"],
            "status":        status,
            "is_attempting": False,
            "demo_mode":     bool(r["demo_mode"]),
            "opp_id":        r["opp_id"],
            "opp_title":     r["opp_title"] or f"opp #{r['opportunity_id']}",
            "kalshi_url":    _kalshi_url(opp_evt_tkr, link_ticker, opp_slug) if link_ticker else "",
            "started_ago":   _time_ago(r["started_at"]),
            "completed_ago": _time_ago(r["completed_at"]) if r["completed_at"] else "—",
            "legs":          legs,
            "net_pnl":       r["net_pnl"],
            "exit_pnl":      exit_pnl,
            "notes":         r["notes"],
            "unwind_reason": r["unwind_reason"],
        })

    return trades, stats


def _get_attempts(limit: int = 250) -> tuple[list, dict, list]:
    """Return event-driven attempt rows, summary stats, and top preflight reasons."""
    default_stats = {"total": 0, "preflight_failed": 0, "order_attempted": 0, "complete": 0}
    try:
        con = sqlite3.connect(DB_PATH)
        con.row_factory = sqlite3.Row
        stats_row = con.execute("""
            SELECT
              COUNT(*) AS total,
              SUM(CASE WHEN preflight_ok = 0 THEN 1 ELSE 0 END) AS preflight_failed,
              SUM(CASE WHEN order_attempted = 1 THEN 1 ELSE 0 END) AS order_attempted,
              SUM(CASE WHEN final_status = 'complete' THEN 1 ELSE 0 END) AS complete
            FROM trade_attempts
            WHERE triggered_by = 'event'
        """).fetchone()
        reasons_raw = con.execute("""
            SELECT preflight_reason AS reason, COUNT(*) AS n
            FROM trade_attempts
            WHERE triggered_by = 'event'
              AND preflight_ok = 0
              AND preflight_reason IS NOT NULL
              AND preflight_reason != ''
            GROUP BY preflight_reason
            ORDER BY n DESC, preflight_reason ASC
            LIMIT 10
        """).fetchall()
        attempts_raw = con.execute("""
            SELECT
              ta.*,
              (
                SELECT o.title
                FROM opportunities o
                WHERE o.market_key = ta.market_key
                ORDER BY o.detected_at DESC
                LIMIT 1
              ) AS market_title,
              (
                SELECT o.ticker
                FROM opportunities o
                WHERE o.market_key = ta.market_key
                ORDER BY o.detected_at DESC
                LIMIT 1
              ) AS ticker,
              (
                SELECT o.event_ticker
                FROM opportunities o
                WHERE o.market_key = ta.market_key
                ORDER BY o.detected_at DESC
                LIMIT 1
              ) AS event_ticker,
              (
                SELECT o.event_slug
                FROM opportunities o
                WHERE o.market_key = ta.market_key
                ORDER BY o.detected_at DESC
                LIMIT 1
              ) AS event_slug
              ,
              (
                SELECT o.outcome_tickers
                FROM opportunities o
                WHERE o.market_key = ta.market_key
                ORDER BY o.detected_at DESC
                LIMIT 1
              ) AS outcome_tickers
            FROM trade_attempts ta
            WHERE ta.triggered_by = 'event'
            ORDER BY ta.triggered_at DESC
            LIMIT ?
        """, (limit,)).fetchall()
        con.close()
    except Exception:
        return [], default_stats, []

    stats = default_stats.copy()
    if stats_row:
        stats = {
            "total": int(stats_row["total"] or 0),
            "preflight_failed": int(stats_row["preflight_failed"] or 0),
            "order_attempted": int(stats_row["order_attempted"] or 0),
            "complete": int(stats_row["complete"] or 0),
        }

    reasons = [{"reason": r["reason"], "n": int(r["n"] or 0)} for r in reasons_raw]
    attempts = []
    for r in attempts_raw:
        ticker = r["ticker"] or ""
        event_ticker = r["event_ticker"] or ticker
        event_slug = r["event_slug"] or ""
        link_ticker = _url_leaf_ticker(ticker, event_ticker, r["outcome_tickers"] if "outcome_tickers" in r.keys() else [])
        kalshi_url = _kalshi_url(event_ticker, link_ticker, event_slug) if link_ticker else ""
        attempts.append({
            "id": r["id"],
            "when": _time_ago(r["triggered_at"]),
            "market_key": r["market_key"] or "—",
            "market_title": r["market_title"] or "",
            "preflight_ok": r["preflight_ok"],
            "order_attempted": bool(r["order_attempted"]),
            "preflight_reason": r["preflight_reason"],
            "final_status": r["final_status"],
            "trade_id": r["trade_id"],
            "kalshi_url": kalshi_url,
        })
    return attempts, stats, reasons


@app.route("/delete-trade/<int:trade_id>", methods=["POST"])
def delete_trade(trade_id: int):
    try:
        con = sqlite3.connect(DB_PATH)
        # Clear linked row pointer only; market-level auth is managed separately.
        con.execute("UPDATE opportunities SET trade_id = NULL WHERE trade_id = ?", (trade_id,))
        con.execute("DELETE FROM trades WHERE id = ?", (trade_id,))
        con.commit()
        con.close()
    except Exception:
        pass
    return redirect(url_for("trades_page"))


@app.route("/clear-attempts", methods=["POST"])
def clear_attempts():
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("DELETE FROM trade_attempts WHERE triggered_by = 'event'")
        con.commit()
        con.close()
    except Exception:
        pass
    return redirect(url_for("attempts_page"))


@app.route("/delete-attempt/<int:attempt_id>", methods=["POST"])
def delete_attempt(attempt_id: int):
    try:
        con = sqlite3.connect(DB_PATH)
        con.execute("DELETE FROM trade_attempts WHERE id = ? AND triggered_by = 'event'", (attempt_id,))
        con.commit()
        con.close()
    except Exception:
        pass
    return redirect(url_for("attempts_page"))


@app.route("/authorized")
def authorized_page():
    rows = _get_authorized_opps()
    now  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return render_template_string(AUTHORIZED_TEMPLATE, rows=rows, now=now,
                                  page_title="Authorized Opportunities",
                                  active_tab="authorized")


@app.route("/trades")
def trades_page():
    trades, stats = _get_trades()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return render_template_string(TRADES_TEMPLATE, trades=trades, stats=stats, now=now,
                                  page_title="Trades", active_tab="trades")


@app.route("/attempts")
def attempts_page():
    attempts, stats, reasons = _get_attempts()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return render_template_string(
        ATTEMPTS_TEMPLATE,
        attempts=attempts,
        stats=stats,
        reasons=reasons,
        now=now,
        page_title="Event-Driven Attempts",
        active_tab="attempts",
    )


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=False)

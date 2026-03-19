"""
Simple web dashboard for the Polymarket arb bot.

Reads from the SQLite database written by arb_bot.py and renders a
live-refreshing HTML page of recent opportunities.

Run:
    python web.py                          # localhost:8080
    gunicorn -b 127.0.0.1:8080 web:app    # production
"""

import json
import sqlite3
from datetime import datetime, timezone

from flask import Flask, render_template_string, request

from config import DB_PATH

app = Flask(__name__)

# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

TEMPLATE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="30">
  <title>Polymarket Arb</title>
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body { font-size: 0.875rem; background: #f8f9fa; }
    .profit-pos  { color: #198754; font-weight: 700; }
    .profit-near { color: #fd7e14; font-weight: 600; }
    .profit-neg  { color: #6c757d; }
    td.legs      { font-size: 0.78rem; line-height: 1.6; white-space: nowrap; }
    .q           { max-width: 380px; word-break: break-word; }
    .stat-card   { min-width: 130px; }
  </style>
</head>
<body>
<div class="container-fluid py-3 px-4">

  <!-- Header -->
  <div class="d-flex justify-content-between align-items-center mb-3">
    <h5 class="mb-0 fw-bold">Polymarket Arb Monitor</h5>
    <span class="text-muted small">auto-refresh 30s &mdash; {{ now }}</span>
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
          </tr>
        </thead>
        <tbody>
          {% for r in rows %}
          <tr>
            <td class="text-muted text-nowrap">{{ r.time_ago }}</td>
            <td class="q">
              <span title="{{ r.question }}">{{ r.question[:80] }}</span>
              {% if r.has_zero_size %}
                <br><span class="badge bg-secondary">zero-size</span>
              {% endif %}
              {% if r.category == 'near_miss' %}
                <br><span class="badge bg-warning text-dark">near-miss</span>
              {% endif %}
            </td>
            <td class="legs">
              {% for leg in r.legs %}
              <div>
                <span class="text-muted">{{ leg.outcome[:18] }}</span>
                ask=<strong>{{ "%.4f" | format(leg.price) }}</strong>
                <span class="text-muted">({{ "%.0f" | format(leg.size) }})</span>
              </div>
              {% endfor %}
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
          </tr>
          {% endfor %}
          {% if not rows %}
          <tr>
            <td colspan="6" class="text-center text-muted py-5">No records yet.</td>
          </tr>
          {% endif %}
        </tbody>
      </table>
    </div>
  </div>
  <div class="text-muted small mt-2">Showing {{ rows | length }} most recent records.</div>

</div>
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
        rows.append({
            "time_ago":    _time_ago(r["detected_at"]),
            "question":    r["question"],
            "legs":        legs,
            "sum_asks":    r["sum_asks"],
            "net_profit":  r["net_profit"],
            "source":      r["source"],
            "category":    r["category"],
            "has_zero_size": r["has_zero_size"],
        })
    return rows


def _get_stats() -> dict:
    try:
        con = sqlite3.connect(DB_PATH)
        total     = con.execute("SELECT COUNT(*) FROM opportunities").fetchone()[0]
        opps      = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='opportunity'").fetchone()[0]
        near_miss = con.execute("SELECT COUNT(*) FROM opportunities WHERE category='near_miss'").fetchone()[0]
        last      = con.execute("SELECT MAX(detected_at) FROM opportunities").fetchone()[0]
        con.close()
        return {"total": total, "opps": opps, "near_miss": near_miss,
                "last_seen_ago": _time_ago(last)}
    except Exception:
        return {"total": 0, "opps": 0, "near_miss": 0, "last_seen_ago": "—"}


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
                                  category=cat, now=now)


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=False)

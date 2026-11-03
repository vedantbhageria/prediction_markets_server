"""Browse the database in a web browser.

    python viewer.py          ->  http://127.0.0.1:8080

Binds to localhost only. Read-only: anything that isn't a SELECT is refused.
"""

import argparse
import json
import math
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from urllib.parse import quote, urlencode
from zoneinfo import ZoneInfo

from flask import (Flask, Response, jsonify, redirect, render_template_string,
                   request)
from psycopg2.extras import RealDictCursor

import db
import monitor
import paths
from db import connection
import fetch as candle_fetch
import poly
import ratelimit
import settings

app = Flask(__name__)


# ---------------------------------------------------------------------------
# Login. When VIEWER_USER and VIEWER_PASSWORD_HASH are set (in .env), every
# page asks for them; unset, the viewer is open, as for local use. Only a hash
# of the password is stored: make one with
#   python -c "from werkzeug.security import generate_password_hash as g; print(g('PASSWORD'))"
# ---------------------------------------------------------------------------

import os
from werkzeug.security import check_password_hash

db.load_env()
LOGIN_USER = os.environ.get("VIEWER_USER")
LOGIN_HASH = os.environ.get("VIEWER_PASSWORD_HASH")
_failures = []                  # times of recent failed logins, all clients
_failures_lock = threading.Lock()
FAILURE_WINDOW, FAILURE_LIMIT = 300, 10


@app.before_request
def require_login():
    if not (LOGIN_USER and LOGIN_HASH):
        return None
    auth = request.authorization
    if auth and auth.username == LOGIN_USER and check_password_hash(LOGIN_HASH, auth.password or ""):
        return None
    if auth:
        # Behind a public link every client can look the same, so failures are
        # counted together: past the limit, every attempt waits, which makes
        # guessing the password slow without locking the owner out.
        now = time.time()
        with _failures_lock:
            _failures[:] = [t for t in _failures if now - t < FAILURE_WINDOW] + [now]
            slow = len(_failures) > FAILURE_LIMIT
        time.sleep(5 if slow else 1)
    return Response("Login required.", 401,
                    {"WWW-Authenticate": 'Basic realm="prediction_markets", charset="UTF-8"'})

# Polymarket keys on condition_id where Kalshi keys on ticker.
POLY = {"poly_events", "poly_markets", "poly_candles"}

# The same links at the top of every page.
NAV_LINKS = [("tables", "/table/kalshi_markets?dim=category"), ("visualise", "/visualise"),
             ("stats", "/stats"), ("status", "/status"), ("manage", "/manage"), ("docs", "/docs")]
app.jinja_env.globals["NAV"] = (
    '<span class="links">'
    + " ".join(f'<a href="{href}">{label}</a>' for label, href in NAV_LINKS)
    + ' <a class="theme" onclick="return pmTheme()" title="light or dark">&#9680; theme</a></span>')
# Light and dark colours for every page; dark follows the system unless the
# theme link picked one (remembered in the browser).
app.jinja_env.globals["THEME"] = """<meta name="color-scheme" content="light dark">
<style>
 :root { --bg:#fff; --ink:#222; --muted:#666; --faint:#999; --line:#ddd; --line2:#eee;
         --soft:#f4f4f4; --soft2:#fafafa; --link:#0645ad; --accent:#2a6099;
         --accent-soft:#f6f9ff; --accent-line:#ccd8ee; --ok:#262; --ok-soft:#e6f6e6;
         --warn:#8a5a00; --warn-soft:#fff4d6; --bad:#a33; --bad-soft:#fff2f2;
         --bad-line:#f0c0c0; --down:#f6d5d4; color-scheme: light; }
 @media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
         --bg:#15171b; --ink:#e4e6ea; --muted:#a3aab4; --faint:#7f8791; --line:#363b44;
         --line2:#2a2e35; --soft:#1f2228; --soft2:#1a1d22; --link:#8cb4ff; --accent:#6fa0d8;
         --accent-soft:#1a2433; --accent-line:#30476a; --ok:#80d38c; --ok-soft:#1b3121;
         --warn:#e9c46e; --warn-soft:#382e14; --bad:#ff8f8f; --bad-soft:#3b1d1d;
         --bad-line:#6d2f2f; --down:#4a2727; color-scheme: dark; } }
 :root[data-theme="dark"] {
         --bg:#15171b; --ink:#e4e6ea; --muted:#a3aab4; --faint:#7f8791; --line:#363b44;
         --line2:#2a2e35; --soft:#1f2228; --soft2:#1a1d22; --link:#8cb4ff; --accent:#6fa0d8;
         --accent-soft:#1a2433; --accent-line:#30476a; --ok:#80d38c; --ok-soft:#1b3121;
         --warn:#e9c46e; --warn-soft:#382e14; --bad:#ff8f8f; --bad-soft:#3b1d1d;
         --bad-line:#6d2f2f; --down:#4a2727; color-scheme: dark; }
 html, body { background: var(--bg); color: var(--ink); }
 a { color: var(--link); }
 input, select, textarea { background: var(--soft2); color: var(--ink);
                           border: 1px solid var(--line); border-radius: 4px; }
 .links a.theme { cursor: pointer; }
</style>
<script>
(function () {
  try { var t = localStorage.getItem('pm-theme'); if (t) document.documentElement.setAttribute('data-theme', t); }
  catch (e) {}
})();
function pmTheme() {
  var root = document.documentElement, now = root.getAttribute('data-theme') ||
      (matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  var next = now === 'dark' ? 'light' : 'dark';
  root.setAttribute('data-theme', next);
  try { localStorage.setItem('pm-theme', next); } catch (e) {}
  window.dispatchEvent(new Event('themechange'));
  return false;
}
</script>
"""


@lru_cache(maxsize=None)
def _columns(name):
    return [r["column_name"] for r in db.query(
        "SELECT column_name FROM information_schema.columns"
        " WHERE table_schema = current_schema() AND table_name = %s"
        " ORDER BY ordinal_position", (name,))]


def key_col(name):
    # The grouping tables key on their own column, not a market id. A table
    # with neither (the API's own, say) is ordered by its first column.
    grouping = {"kalshi_series": "series_ticker", "kalshi_events": "event_ticker",
                "poly_events": "event_id"}
    key = grouping.get(name, "condition_id" if name in POLY else "ticker")
    columns = _columns(name)
    return key if not columns or key in columns else columns[0]


def dim_source(name, dim):
    """(FROM clause, column, key) for slicing table `name` by `dim`.

    A market's category, series and event slug live on its event, so market
    and candle tables slice through a join to the event table.
    """
    if name in ("kalshi_markets", "kalshi_candles"):
        col = {"category": "e.category", "series_ticker": "e.series_ticker"}.get(dim, f"m.{dim}")
        return "kalshi_markets m JOIN kalshi_events e USING (event_ticker)", col, "m.ticker"
    if name in ("poly_markets", "poly_candles"):
        col = {"category": "e.category", "event_slug": "e.slug"}.get(dim, f"m.{dim}")
        return "poly_markets m LEFT JOIN poly_events e USING (event_id)", col, "m.condition_id"
    return f'"{name}" m', f"m.{dim}", f"m.{key_col(name)}"

# Nav order: general -> specific, one group per instrument type. Anything not
# listed here is appended in its own group.
TABLE_GROUPS = [
    ["kalshi_series", "kalshi_events", "kalshi_markets", "kalshi_candles"],
    ["poly_events", "poly_markets", "poly_candles", "poly_trades"],
]

PAGE = """
<!doctype html>
{{ THEME|safe }}
<title>prediction_markets</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 1.5rem; }
 table { border-collapse: collapse; font-size: 13px; }
 th, td { border: 1px solid var(--line); padding: 4px 8px; text-align: left; }
 th { background: var(--line2); position: sticky; top: 0; }
 td { max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
 nav a { margin-right: 4px; }
 nav small { margin-right: 12px; color: var(--muted); }
 nav a.on { font-weight: 700; }
 nav .sep { margin: 0 14px 0 2px; color: var(--line); }
 textarea { width: 100%; height: 60px; font-family: monospace; }
 .err { color: var(--bad); }
 .cols { display: flex; gap: 18px; align-items: flex-start; }
 .cols.no-filters .side { display: none; }
 .cols.no-nearby .nearby { display: none; }
 header .toggles { margin-left: auto; }
 header .toggles button { font-size: 12px; padding: 3px 9px; margin-left: 6px;
   border: 1px solid var(--line); background: var(--soft); border-radius: 3px; cursor: pointer; }
 header .toggles button.off { opacity: .45; }
 header .toggles button:disabled { opacity: .35; cursor: help; }
 .side { width: 250px; flex: none; border-right: 1px solid var(--line); padding-right: 12px;
         max-height: 78vh; overflow-y: auto; }
 .side b { display: block; font-size: 12px; text-transform: uppercase;
           color: var(--muted); margin-bottom: 4px; }
 .side small { color: var(--faint); }
 .detail { background: var(--soft); border-left: 3px solid var(--link); padding: 8px 12px;
           margin-bottom: 10px; }
 header { display: flex; align-items: baseline; gap: 18px; }
 header h2 { margin: 0 0 6px; }
 header .links a { font-size: 13px; margin-right: 12px; }
 .okc { color: var(--ok); }
 .sentinel { padding: 10px 4px; font-size: 12px; color: var(--muted); cursor: pointer;
             text-align: center; border: 1px solid var(--line); border-radius: 4px;
             margin-top: 8px; background: var(--soft2); }
 .msg { background: var(--ok-soft); border-left: 3px solid #4a4; padding: 6px 10px;
        margin-bottom: 8px; font-size: 13px; }
 .msg.err { background: var(--bad-soft); border-left-color: var(--bad); }
 .spin { display: inline-block; width: 9px; height: 9px; margin-right: 5px;
         border: 2px solid #4a4; border-right-color: transparent;
         border-radius: 50%; animation: spin .7s linear infinite; }
 @keyframes spin { to { transform: rotate(360deg); } }
 .search { margin-bottom: 10px; }
 .search input[name=q] { width: 320px; padding: 4px 7px; }
 .search button, .search select { padding: 4px 8px; }
 .search a { font-size: 12px; margin-left: 8px; }
 .fetch { margin-bottom: 10px; }
 .raw { margin-top: 14px; }
 .raw summary { cursor: pointer; font-size: 12px; color: var(--muted); }
 .raw table { margin-top: 6px; font-size: 12px; }
 .raw th { text-align: left; white-space: nowrap; vertical-align: top; }
 .raw td { max-width: 620px; white-space: normal; }
 .btn { display: inline-block; background: var(--link); color: #fff; padding: 5px 11px;
        border-radius: 3px; font-size: 12px; text-decoration: none; }
 .btn:hover { background: var(--link); }
 .idx { color: var(--faint); font-size: 11px; text-align: right; width: 1%;
        white-space: nowrap; }
 th.idx { text-align: right; }
 .main td a { text-decoration: none; }
 .side a { display: block; padding: 2px 4px; font-size: 12px; text-decoration: none;
           white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
 .side a.on { background: var(--link); color: #fff; border-radius: 3px; }
 .side input { width: 100%; margin-bottom: 6px; }
 .dims { margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px solid var(--line2); }
 .dims a { display: inline-block; font-size: 11px; margin: 0 6px 2px 0;
           padding: 1px 5px; border-radius: 3px; background: var(--soft);
           color: var(--ink); }
 .dims a.on { background: var(--link); color: #fff; }
 .main { flex: 1; overflow-x: auto; min-width: 0; }
 .related { width: 290px; flex: none; border-left: 1px solid var(--line); padding-left: 12px;
            max-height: 78vh; overflow-y: auto; }
 .related b { display: block; font-size: 12px; text-transform: uppercase; color: var(--muted); }
 .related a { display: block; font-size: 12px; text-decoration: none; margin-top: 6px; }
 .related .meta { font-size: 11px; color: var(--muted); }
 .sliderow { display: flex; align-items: center; gap: 8px; margin: 6px 0 10px; }
 .sliderow input { flex: 1; }
 .sliderow span { font-size: 11px; color: var(--muted); width: 34px; text-align: right; }
 .dot { display: inline-block; width: 6px; height: 6px; border-radius: 50%; }
 .dot.on { background: #4a4; }
 .cols.no-related .related { display: none; }
 .nearby { width: 300px; flex: none; border-left: 1px solid var(--line); padding-left: 12px;
           max-height: 78vh; overflow-y: auto; }
 .nearby b { display: block; font-size: 12px; text-transform: uppercase;
             color: var(--muted); }
 .nearby .anchor { font-size: 11px; color: var(--faint); margin-bottom: 8px; }
 .nearby a { display: block; font-size: 12px; text-decoration: none;
             margin-top: 6px; }
 .nearby .meta { font-size: 11px; color: var(--muted); }
 .byrow { margin: 6px 0 10px; }
 a.nctl { cursor: pointer; }
 .byrow a { display: inline-block; font-size: 11px; margin: 0 4px 3px 0;
            padding: 2px 6px; border-radius: 3px; background: var(--soft); color: var(--ink);
            text-decoration: none; }
 .byrow a.on { background: var(--link); color: #fff; }
 .widen { margin-top: 12px; border-top: 1px solid var(--line2); padding-top: 6px; }
 .widen a { display: inline-block; margin-right: 8px; font-size: 11px; }
 .widen a.on { font-weight: 700; text-decoration: none; }
 .venue { display: inline-block; width: 13px; height: 13px; line-height: 13px;
          text-align: center; border-radius: 2px; font-size: 9px; font-weight: 700;
          color: #fff; margin-right: 5px; vertical-align: 1px; }
 .v-k { background: var(--accent); }
 .v-p { background: #99542a; }
 /* On a tablet or phone the columns stack, and a nested scrolling panel is
    unusable, so the panels grow with the page instead. */
 @media (max-width: 900px) {
   .cols { flex-direction: column; }
   .side, .nearby { width: auto; max-width: none; max-height: none;
                    overflow-y: visible; border: 0; padding: 0; }
   .nearby { border-top: 1px solid var(--line); padding-top: 10px; }
 }
</style>

<header>
  <h2><a href="/" style="text-decoration:none;color:inherit">prediction_markets</a></h2>
  {{ NAV|safe }}
  <span class="toggles">
    <button type="button" onclick="panel('filters')">filters</button>
    {% if near %}<button type="button" onclick="panel('nearby')">nearby</button>{% else
    %}<button type="button" disabled title="open a market (click a ticker) to see markets near it">nearby</button>{% endif %}
    {% if related %}<button type="button" onclick="panel('related')">related</button>{% else
    %}<button type="button" disabled title="open a market (click a ticker) to see markets worded like it">related</button>{% endif %}
  </span>
</header>
<nav>
{% for group in groups %}
  {% if not loop.first %}<span class="sep">|</span>{% endif %}
  {% for t, n in group %}<a href="/table/{{ t }}"
     class="{{ 'on' if t == table_name else '' }}">{{ t }}</a><small>({{ n }})</small>{% endfor %}
{% endfor %}
</nav>
<hr>

<form method="get" action="/query">
  <textarea name="sql">{{ sql or 'SELECT * FROM kalshi_markets LIMIT 50' }}</textarea>
  <!-- Remembers which table you were on, so the picker survives a query. -->
  <input type="hidden" name="table" value="{{ table_name or '' }}">
  <button type="submit">Run</button>
</form>

{% if error %}<p class="err">{{ error }}</p>{% endif %}

<div class="cols" id="cols">
{% for p in panels %}
  <div class="side">
    {% if p.dims %}
    <div class="dims">
      {% for d in p.dims %}
        <a href="/table/{{ p.table }}?dim={{ d }}"
           class="{{ 'on' if d == p.dim else '' }}">{{ d.replace('_', ' ') }}</a>
      {% endfor %}
    </div>
    {% endif %}
    <b>{{ p.title }}</b>
    <input placeholder="filter..." onkeyup="
      var v = this.value.toUpperCase();
      this.parentNode.querySelectorAll('a').forEach(function (a) {
        a.style.display = a.textContent.toUpperCase().indexOf(v) > -1 ? '' : 'none';
      });">
    <a href="{{ p.all_url }}" class="{{ '' if p.selected else 'on' }}">(all)</a>
    <div class="opts">
    {% for value, label in p.options %}
      <a href="{{ p.url }}{{ value }}" class="{{ 'on' if value == p.selected else '' }}"
         title="{{ label }}">{{ value }}{% if label %} <small>{{ label }}</small>{% endif %}</a>
    {% endfor %}
    </div>
  </div>
{% endfor %}

  <div class="main">
  {% if job %}
    <div class="msg {{ 'err' if job.state == 'error' else '' }}">
      {% if job.state == 'running' %}
        <span class="spin"></span>
        <span id="jobtext">downloading candles&hellip;</span>
        <span class="meta" id="jobclock"></span>
        <script>
        (function () {
          var t0 = Date.now();
          var url = '/job?table={{ table_name }}&ticker={{ selection }}';
          var clock = document.getElementById('jobclock');
          var text = document.getElementById('jobtext');
          var tick = setInterval(function () {
            clock.textContent = ' ' + Math.round((Date.now() - t0) / 1000) + 's';
          }, 1000);
          (function poll() {
            fetch(url).then(function (r) { return r.json(); }).then(function (j) {
              if (j.state === 'running') { setTimeout(poll, 2000); return; }
              clearInterval(tick);
              text.textContent = j.message || 'done';
              location.reload();      // pull in the rows that just landed
            }).catch(function () { setTimeout(poll, 3000); });
          })();
        })();
        </script>
      {% else %}{{ job.message }}{% endif %}
    </div>
  {% elif msg %}<div class="msg">{{ msg }}</div>{% endif %}
  {% if fetchable %}
    <div class="fetch">
      <a class="btn" href="/fetch?table={{ table_name }}&dim={{ dim }}&asset={{ asset_value }}&ticker={{ selection }}">
        download candles for this market</a>
      <span class="meta">from open to close (or now){% if fetch_estimate %}
        &middot; ~{{ fetch_estimate }}{% endif %}</span>
    </div>
  {% endif %}
  {% if detail %}
    <div class="detail">
      <b>{{ detail.title }}</b>{% if detail.yes_sub_title %} &mdash; {{ detail.yes_sub_title }}{% endif %}<br>
      <small>{% if detail.live %}closes {{ detail.close_time }} &middot;
        <b class="okc">open</b>{% if detail.closes_in %} &middot;
        {{ detail.closes_in }} left{% endif %}
      {% else %}closed {{ detail.close_time }} &middot;
        {% if detail.outcome %}settled <b>{{ detail.outcome|upper }}</b>
        {% else %}<b>not settled yet</b>{% endif %}
      {% endif %} &middot;
        volume {{ '{:,.0f}'.format(detail.volume or 0) }}
        {% if detail.bars %}
          &middot; {{ '{:,}'.format(detail.bars) }} candles
          {% if detail.traded is not none %}
            &middot; <b>{{ '{:,}'.format(detail.traded) }} with trades</b>
            ({{ '%.0f'|format(100 * detail.traded / detail.bars) }}%)
          {% endif %}
          {% if detail.first %}<br>data {{ detail.first }} &rarr; {{ detail.last }}{% endif %}
        {% endif %}</small>
    </div>
  {% endif %}
  {% if selection and table_name and table_name.endswith('_candles') and rows %}
    <p><a class="btn" href="/visualise?ticker={{ selection|urlencode }}">chart this market</a></p>
  {% endif %}
  {% if table_name %}
    <form class="search" method="get" action="/table/{{ table_name }}">
      <input type="hidden" name="dim" value="{{ dim }}">
      <input type="hidden" name="asset" value="{{ asset_value }}">
      <input type="hidden" name="ticker" value="{{ ticker_value }}">
      <input name="q" value="{{ q or '' }}" placeholder="search tickers and titles">
      <button type="submit">search</button>
      {% if q %}<a href="/table/{{ table_name }}?dim={{ dim }}&asset={{ asset_value }}&ticker={{ ticker_value }}">clear</a>{% endif %}
      <select name="limit" onchange="this.form.submit()">
        {% for n in [200, 500, 2000, 10000] %}
          <option value="{{ n }}" {{ 'selected' if n == limit else '' }}>{{ '{:,}'.format(n) }} rows</option>
        {% endfor %}
        <option value="all" {{ 'selected' if limit is none else '' }}>show all</option>
      </select>
    </form>
  {% endif %}
  {% if rows %}
    <p><span id="rowcount">{{ '{:,}'.format(rows|length) }}</span> rows{% if selection %} &mdash; <b>{{ selection }}</b>{% endif %}{% if q %} matching &ldquo;{{ q }}&rdquo;{% endif %}</p>
    <table id="dtable">
      <tr><th class="idx">#</th>{% for c in cols %}<th>{{ c }}</th>{% endfor %}</tr>
      {% for r in rows %}<tr>
        <td class="idx">{{ loop.index }}</td>
        {% for c in cols %}
          {% if c in ('ticker', 'condition_id') and candle_table %}
            <td><a href="/table/{{ candle_table }}?{% if 'asset' in r and r['asset'] %}asset={{ r['asset'] }}&amp;{% endif %}ticker={{ r[c] }}">{{ r[c] }}</a></td>
          {% else %}
            <td>{{ r[c] }}</td>
          {% endif %}
        {% endfor %}
      </tr>{% endfor %}
    </table>
    {% if table_more %}
      <div class="sentinel" id="tmore">load more</div>
      <script>
      (function () {
        var el = document.getElementById('tmore');
        var tbl = document.getElementById('dtable');
        var counter = document.getElementById('rowcount');
        var offset = {{ rows|length }}, busy = false, done = false;
        var qs = {{ table_qs|tojson }};
        function load() {
          if (busy || done) return;
          busy = true; el.textContent = 'loading\u2026';
          fetch('/table/rows?' + qs + '&offset=' + offset)
            .then(function (r) { return r.json(); })
            .then(function (d) {
              if (!d.count) { done = true; el.textContent = 'end of data'; return; }
              tbl.insertAdjacentHTML('beforeend', d.html);
              offset += d.count;
              counter.textContent = offset.toLocaleString();
              busy = false;
              if (d.count < d.page) { done = true; el.textContent = 'end of data'; }
              else { el.textContent = 'load more'; near(); }
            })
            .catch(function () { busy = false; el.textContent = 'load more (retry)'; });
        }
        function near() {
          var b = el.getBoundingClientRect();
          if (b.top <= (window.innerHeight || 0) + 400) load();
        }
        window.addEventListener('scroll', near, {passive: true});
        window.addEventListener('resize', near, {passive: true});
        el.addEventListener('click', load);
        near();
      })();
      </script>
    {% endif %}
  {% elif rows is not none %}
    <p>no rows</p>
  {% endif %}
  {% if raw_doc %}
    <details class="raw">
      <summary>raw payload ({{ raw_doc|length }} fields)</summary>
      <table>
        {% for k, v in raw_doc.items()|sort %}
          <tr><th>{{ k }}</th><td>{{ v }}</td></tr>
        {% endfor %}
      </table>
    </details>
  {% endif %}
  </div>

{% if related %}
  <div class="related">
    <b>related wording</b>
    <div class="anchor">trigram similarity &middot; tune the bar</div>
    <div class="sliderow">
      <input type="range" id="thr" min="5" max="95" step="5"
             value="{{ (related.threshold * 100) | round | int }}">
      <span id="thrval">{{ (related.threshold * 100) | round | int }}%</span>
    </div>
    <div id="relrows">
      {% if related.loading %}<span class="meta">loading&hellip;</span>{% else %}
      {% for m in related.rows %}
        <a href="{{ m.href }}" title="{{ m.title }}"><span class="venue {{ 'v-k' if m.venue == 'kalshi' else 'v-p' }}">{{ 'K' if m.venue == 'kalshi' else 'P' }}</span>{{ m.title[:70] }}{% if m.title|length > 70 %}&hellip;{% endif %}</a>
        <span class="meta"><b>{{ '%.2f'|format(m.score) }}</b>
          {% if m.got %}<span class="dot on" title="candles downloaded"></span>{% endif %}
          {{ m.closes or '' }}{% if m.shared %} &middot; {{ m.shared|join(', ') }}{% endif %}</span>
      {% else %}<span class="meta">nothing above this threshold</span>{% endfor %}
      {% endif %}
    </div>
    <script>
    (function () {
      var slider = document.getElementById('thr');
      var label = document.getElementById('thrval');
      var box = document.getElementById('relrows');
      var timer = null, ctrl = null;
      function esc(s) { return (s || '').replace(/[&<>"]/g, function (c) {
        return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]; }); }
      function draw(rows) {
        if (!rows.length) { box.innerHTML = '<span class="meta">nothing above this threshold</span>'; return; }
        box.innerHTML = rows.map(function (m) {
          return '<a href="' + m.href + '" title="' + esc(m.title) + '">' +
            '<span class="venue ' +
            (m.venue === 'kalshi' ? 'v-k">K' : 'v-p">P') + '</span>' +
            esc(m.title.slice(0, 70)) +
            (m.title.length > 70 ? '…' : '') + '</a>' +
            '<span class="meta"><b>' + m.score.toFixed(2) + '</b> ' +
            (m.got ? '<span class="dot on"></span>' : '') + ' ' + (m.closes || '') +
            (m.shared.length ? ' &middot; ' + esc(m.shared.join(', ')) : '') + '</span>';
        }).join('');
      }
      function load() {
        if (ctrl) ctrl.abort();            // drop the in-flight request
        ctrl = new AbortController();
        box.style.opacity = 0.5;
        fetch('/related?table={{ table_name }}&ticker={{ selection|urlencode }}&t=' +
              (slider.value / 100), {signal: ctrl.signal})
          .then(function (r) { return r.json(); })
          .then(function (d) { box.style.opacity = 1; draw(d.rows); })
          .catch(function () {});
      }
      slider.addEventListener('input', function () {
        label.textContent = slider.value + '%';
        clearTimeout(timer);
        timer = setTimeout(load, 200);
      });
      load();                              // the page does not wait for it
    })();
    </script>
  </div>
{% endif %}
{% if near %}
  <div class="nearby">
    <b>nearby markets</b>
    <div class="anchor" id="nearhead">
      {% if near.loading %}same expiry or odds{% else %}
      {{ near.kalshi_n }} Kalshi, {{ near.poly_n }} Polymarket
      {% if near.same_category and near.category %}&middot; {{ near.category }}{% endif %}<br>
      {% if 'time' in near.by %}expires {{ near.when }} &plusmn;{{ near.hours }}h{% endif %}
      {% if 'price' in near.by and near.anchor_last is not none %}
        &middot; odds {{ '%.0f'|format(near.anchor_last * 100) }}% &plusmn;{{ near.points }}pt{% endif %}
      {% endif %}
    </div>

    <div class="byrow">
      {% for b, label in [('time', 'same expiry'), ('price', 'same odds'),
                          ('price,time', 'expiry + odds')] %}
        <a class="nctl" data-qs="by={{ b }}&near={{ near.hours }}&pts={{ near.points }}&cat={{ '1' if near.same_category else '0' }}"
           " class="nctl {{ 'on' if b == near.by else '' }}">{{ label }}</a>
      {% endfor %}
    </div>

    <div id="nearrows">
    {% if near.loading %}<span class="meta">loading&hellip;</span>{% else %}
    {% for m in near.rows %}
      <a href="{{ m.href }}"><span class="venue {{ 'v-k' if m.venue == 'Kalshi' else 'v-p' }}">{{ m.venue[0] }}</span>{{ m.title }}{% if m.last is not none %} <b>{{ '%.0f'|format(m.last * 100) }}%</b>{% endif %}</a>
      <span class="meta">{{ m.close }} &middot; vol {{ '{:,.0f}'.format(m.volume or 0) }}</span>
    {% else %}
      <span class="meta">nothing matches on this measure</span>
    {% endfor %}
    {% endif %}
    </div>
    <script>
    (function () {
      var box = document.getElementById('nearrows');
      var head = document.getElementById('nearhead');
      var base = 'table={{ table_name }}&ticker={{ selection|urlencode }}';
      var state = {by: '{{ near.by }}', near: '{{ near.hours }}',
                   pts: '{{ near.points }}',
                   cat: '{{ '1' if near.same_category else '0' }}'};
      function esc(s) { return (s || '').replace(/[&<>"]/g, function (c) {
        return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]; }); }

      function draw(d) {
        head.textContent = d.kalshi_n + ' Kalshi, ' + d.poly_n + ' Polymarket' +
          (d.category ? ' \u00b7 ' + d.category : '');
        if (!d.rows.length) {
          box.innerHTML = '<span class="meta">nothing matches on this measure</span>';
          return;
        }
        box.innerHTML = d.rows.map(function (m) {
          return '<a href="' + m.href + '"><span class="venue ' +
            (m.venue === 'Kalshi' ? 'v-k">K' : 'v-p">P') + '</span>' +
            esc(m.title) + (m.last != null ? ' <b>' + Math.round(m.last * 100) + '%</b>' : '') +
            '</a><span class="meta">' + esc(m.close) +
            ' \u00b7 vol ' + Math.round(m.volume || 0).toLocaleString() + '</span>';
        }).join('');
      }

      function highlight() {
        document.querySelectorAll('a.nctl').forEach(function (o) {
          var q = o.dataset.qs || '';
          var on = q.split('&').every(function (kv) {
            var p = kv.split('='); return !p[0] || state[p[0]] === p[1];
          });
          o.classList.toggle('on', on);
        });
      }
      function load() {
        box.style.opacity = 0.5;
        var qs = base + '&by=' + state.by + '&near=' + state.near +
                 '&pts=' + state.pts + '&cat=' + state.cat;
        fetch('/nearby?' + qs).then(function (r) { return r.json(); })
          .then(function (d) { box.style.opacity = 1; draw(d); })
          .catch(function () { box.style.opacity = 1; });
      }

      document.querySelectorAll('a.nctl').forEach(function (a) {
        a.addEventListener('click', function (e) {
          e.preventDefault();                       // no page reload
          (a.dataset.qs || '').split('&').forEach(function (kv) {
            var p = kv.split('='); if (p[0]) state[p[0]] = p[1];
          });
          highlight();
          load();
        });
      });
      highlight();
      load();                                       // the page does not wait for it
    })();
    </script>

    <div class="widen">
      {% if 'time' in near.by %}
        {% for h in [1, 3, 6, 24] %}
          <a class="nctl" data-qs="by={{ near.by }}&near={{ h }}&pts={{ near.points }}&cat={{ '1' if near.same_category else '0' }}"
             " class="nctl {{ 'on' if near.hours == h else '' }}">&plusmn;{{ h }}h</a>
        {% endfor %}<br>
      {% endif %}
      {% if 'price' in near.by %}
        {% for pt in [2, 5, 10, 20] %}
          <a class="nctl" data-qs="by={{ near.by }}&near={{ near.hours }}&pts={{ pt }}&cat={{ '1' if near.same_category else '0' }}"
             " class="nctl {{ 'on' if near.points == pt else '' }}">&plusmn;{{ pt }}pt</a>
        {% endfor %}<br>
      {% endif %}
      <a class="nctl" data-qs="by={{ near.by }}&near={{ near.hours }}&pts={{ near.points }}&cat=1"
         " class="nctl {{ 'on' if near.same_category else '' }}">this category</a>
      <a class="nctl" data-qs="by={{ near.by }}&near={{ near.hours }}&pts={{ near.points }}&cat=0"
         " class="nctl {{ '' if near.same_category else 'on' }}">all categories</a>
    </div>
  </div>
{% endif %}
</div>

<script>
// Panel visibility, remembered across pages.
function panel(which) {
  var cols = document.getElementById('cols');
  var cls = 'no-' + which;
  cols.classList.toggle(cls);
  localStorage.setItem(cls, cols.classList.contains(cls) ? '1' : '');
  marks();
}
function marks() {
  var cols = document.getElementById('cols');
  var b = document.querySelectorAll('header .toggles button');
  if (b[0]) b[0].classList.toggle('off', cols.classList.contains('no-filters'));
  if (b[1]) b[1].classList.toggle('off', cols.classList.contains('no-nearby'));
  if (b[2]) b[2].classList.toggle('off', cols.classList.contains('no-related'));
}
(function () {
  var cols = document.getElementById('cols');
  if (!cols) return;
  ['no-filters', 'no-nearby', 'no-related'].forEach(function (c) {
    if (localStorage.getItem(c)) cols.classList.add(c);
  });
  marks();
})();

</script>
"""


def render(**kwargs):
    base = dict(groups=table_groups(), rows=None, cols=[], sql=None, error=None,
                panels=[], selection=None, table_name=None, candle_table=None,
                detail=None, near=None, related=None, msg=None, fetchable=False, dim="",

                asset_value="", ticker_value="", job=None, raw_doc=None, q="", limit=200,
                table_more=False, table_qs="",
                fetch_estimate=None)
    base.update(kwargs)
    return render_template_string(PAGE, **base)


_COUNTS = {"at": 0.0, "rows": None}


# Bookkeeping, not data: the status page shows what they hold.
# The programs' own bookkeeping, not data to browse: what is fetched and what is
# still to do (tracking, scans, observations, Polymarket's feed and the minutes
# waiting for their candle), the search's records and vectors, the daily
# check, and leftovers of retired features.
HIDDEN_TABLES = ("kalshi_observations", "kalshi_scans", "kalshi_titles", "kalshi_tracking",
                 "poly_tracking", "poly_feed", "poly_dirty", "propositions", "templates",
                 "template_vectors", "checks", "kalshi_perp_markets", "kalshi_perp_candles")


def table_list(ttl=60):
    """Table names with row counts, cached for `ttl` seconds.

    The database's own estimates (TimescaleDB's for the candle tables): exact
    counts meant reading every table on every page, which grows with the data.
    """
    if _COUNTS["rows"] is not None and time.monotonic() - _COUNTS["at"] < ttl:
        return _COUNTS["rows"]
    rows = [(r["name"], int(r["n"])) for r in db.query("""
        SELECT c.relname AS name,
               CASE WHEN h.hypertable_name IS NOT NULL
                    THEN approximate_row_count(c.oid::regclass)
                    ELSE GREATEST(c.reltuples, 0)::bigint END AS n
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        LEFT JOIN timescaledb_information.hypertables h
               ON h.hypertable_schema = n.nspname AND h.hypertable_name = c.relname
        WHERE n.nspname = 'public' AND c.relkind = 'r' AND left(c.relname, 4) <> 'api_'
          AND c.relname <> ALL(%s)
        ORDER BY c.relname
    """, (list(HIDDEN_TABLES),))]
    _COUNTS.update(at=time.monotonic(), rows=rows)
    return rows


def table_groups():
    """Tables in reading order, grouped by instrument type."""
    counts = dict(table_list())
    groups = []
    for group in TABLE_GROUPS:
        found = [(name, counts.pop(name)) for name in group if name in counts]
        if found:
            groups.append(found)
    if counts:  # anything unexpected still shows up
        groups.append(sorted(counts.items()))
    return groups


def run(sql):
    """Run one read-only statement."""
    stripped = sql.strip().rstrip(";")
    if not stripped.lower().startswith(("select", "with")):
        raise ValueError("only SELECT queries are allowed")
    if ";" in stripped:
        raise ValueError("one statement at a time")
    if "limit" not in stripped.lower():
        stripped += " LIMIT 500"
    return db.query(stripped)


STATS_PAGE = """
<!doctype html>
{{ THEME|safe }}
<title>stats</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 1050px;
        padding: 0 1rem; color: var(--ink); line-height: 1.5; }
 header { display: flex; align-items: baseline; gap: 18px; }
 header h2 { margin: 0 0 6px; } header h2 a { text-decoration: none; color: inherit; }
 header .links a { font-size: 13px; margin-right: 12px; }
 .two { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }
 .col { border: 1px solid var(--line); border-radius: 6px; padding: 14px 16px; }
 .col.k { border-top: 3px solid var(--accent); } .col.p { border-top: 3px solid #99542a; }
 .col h3 { margin: 0 0 2px; font-size: 16px; }
 .col h3 a { font-size: 12px; font-weight: normal; margin-left: 8px; }
 .col .sub { color: var(--muted); font-size: 12px; margin-bottom: 12px; }
 .big { font-size: 22px; font-weight: 700; }
 dl { display: grid; grid-template-columns: auto 1fr; gap: 3px 12px; margin: 0 0 14px; }
 dt { color: var(--muted); font-size: 13px; }
 dd { margin: 0; text-align: right; font-variant-numeric: tabular-nums; }
 table { border-collapse: collapse; width: 100%; font-size: 13px; }
 th, td { border-bottom: 1px solid var(--line2); padding: 3px 4px; text-align: right; }
 th:first-child, td:first-child { text-align: left; }
 th { color: var(--muted); font-weight: normal; font-size: 12px; }
 .bar { background: var(--accent-line); display: inline-block; height: 8px; border-radius: 2px; }
 .bar.p { background: var(--bad-soft); }
 .note { background: var(--soft); border-left: 3px solid var(--line); padding: 10px 14px;
         font-size: 13px; margin: 20px 0 0; }
 h4 { margin: 16px 0 4px; font-size: 13px; color: var(--muted); }
 @media (max-width: 800px) { .two { grid-template-columns: 1fr; } }
</style>

<header>
  <h2><a href="/">prediction_markets</a> &middot; stats</h2>
  {{ NAV|safe }}
</header>

<div class="two">
  <div class="col k">
    <h3>Kalshi <a href="https://kalshi.com" target="_blank" rel="noopener">kalshi.com &rarr;</a></h3>
    <div class="sub">Regulated exchange &middot; binary contracts settling $0 or $1</div>
    <dl>
      <dt>series</dt><dd>{{ '{:,}'.format(k.series) }}</dd>
      <dt>events</dt><dd>{{ '{:,}'.format(k.events) }}</dd>
      <dt>markets</dt><dd class="big">{{ '{:,}'.format(k.markets) }}</dd>
      <dt>candles downloaded</dt><dd>{{ '{:,}'.format(k.candles) }}</dd>
      <dt>markets with candles</dt><dd>{{ '{:,}'.format(k.covered) }}
        ({{ '%.1f'|format(100 * k.covered / k.markets) if k.markets else '0.0' }}%)</dd>
      <dt>if every market were fetched</dt><dd>~{{ '{:,}'.format(k.estimate) }}</dd>
    </dl>
    <h4>markets by sector</h4>
    <table>
      <tr><th>sector</th><th>markets</th><th></th></tr>
      {% for r in k.sectors %}
      <tr><td>{{ r.category or 'uncategorised' }}</td>
          <td>{{ '{:,}'.format(r.n) }}</td>
          <td style="width:38%"><span class="bar" style="width:{{ r.pct }}%"></span></td></tr>
      {% endfor %}
    </table>
  </div>

  <div class="col p">
    <h3>Polymarket <a href="https://polymarket.com" target="_blank" rel="noopener">polymarket.com &rarr;</a></h3>
    <div class="sub">On-chain order book &middot; one token per outcome</div>
    <dl>
      <dt>events</dt><dd>{{ '{:,}'.format(p.events) }}</dd>
      <dt>markets</dt><dd class="big">{{ '{:,}'.format(p.markets) }}</dd>
      <dt>candles downloaded</dt><dd>{{ '{:,}'.format(p.candles) }}</dd>
      <dt>markets with candles</dt><dd>{{ '{:,}'.format(p.covered) }}
        ({{ '%.1f'|format(100 * p.covered / p.markets) if p.markets else '0.0' }}%)</dd>
      <dt>if every market were fetched</dt><dd>~{{ '{:,}'.format(p.estimate) }}</dd>
    </dl>
    <h4>markets by sector</h4>
    <table>
      <tr><th>sector</th><th>markets</th><th></th></tr>
      {% for r in p.sectors %}
      <tr><td>{{ r.category or 'uncategorised' }}</td>
          <td>{{ '{:,}'.format(r.n) }}</td>
          <td style="width:38%"><span class="bar p" style="width:{{ r.pct }}%"></span></td></tr>
      {% endfor %}
    </table>
  </div>
</div>

<div class="note">
  <b>How the estimate works.</b> Both venues quote at most one candle per minute,
  so the ceiling is how long their markets have been open: every market's
  open-to-close span in minutes, capped at now for markets still running. That is
  {{ '{:,}'.format(k.minutes) }} market-minutes on Kalshi and
  {{ '{:,}'.format(p.minutes) }} on Polymarket. Kalshi is discounted to
  {{ '%.0f'|format(100 * k.fill) }}% of that because it only returns a candle when
  the market traded or was quoted, and most of its catalogue is thin. Polymarket
  forward-fills, so its series are dense and the ceiling is close to the real
  number. These are orders of magnitude, not forecasts.
</div>
"""


TABLE_ROWS = """
{% for r in rows %}<tr>
  <td class="idx">{{ (start or 0) + loop.index }}</td>
  {% for c in cols %}
    {% if c in ('ticker', 'condition_id') and candle_table %}
      <td><a href="/table/{{ candle_table }}?{% if 'asset' in r and r['asset'] %}asset={{ r['asset'] }}&amp;{% endif %}ticker={{ r[c] }}">{{ r[c] }}</a></td>
    {% else %}
      <td>{{ r[c] }}</td>
    {% endif %}
  {% endfor %}
</tr>{% endfor %}
"""

TABLE_PAGE = 300   # rows per lazy-loaded page on the table views

PAGE_ROWS = 300   # rows per lazy-loaded page

MANAGE_ROW = """
{% macro row_html(r, i) -%}
<tr class="{{ '' if r.candles else 'empty' }}">
  <td><input type="checkbox" class="pick" name="id" value="{{ r.venue }}:{{ r.id }}"></td>
  <td class="n idx">{{ i }}</td>
  <td><span class="venue {{ 'v-k' if r.venue == 'kalshi' else 'v-p' }}">{{ 'K' if r.venue == 'kalshi' else 'P' }}</span>
    <a href="/table/{{ 'kalshi_candles' if r.venue == 'kalshi' else 'poly_candles' }}?ticker={{ r.id }}">{{ r.title[:58] }}</a></td>
  <td>{{ r.category or '' }}</td>
  <td><span class="pill {{ 'live' if r.active else 'done' }}">{{ 'active' if r.active else 'expired' }}</span></td>
  <td>{% if r.candles %}<span class="pill got">&check; downloaded</span>
      {% else %}<span class="pill none">no candles</span>{% endif %}</td>
  <td>{{ r.opened or '' }}</td>
  <td>{{ r.closes or '' }}</td>
  <td class="n {{ 'has' if r.candles else 'no' }}">{{ '{:,}'.format(r.candles) if r.candles else '-' }}</td>
  <td>{{ r.first or '' }}</td>
  <td>{{ r.last or '' }}</td>
  <td class="n">{{ '{:,.0f}'.format(r.volume or 0) }}</td>
</tr>
{%- endmacro %}
"""

MANAGE_PAGE = """
{% macro row_html(r, i) -%}
<tr class="{{ '' if r.candles else 'empty' }}">
  <td><input type="checkbox" class="pick" name="id" value="{{ r.venue }}:{{ r.id }}"></td>
  <td class="n idx">{{ i }}</td>
  <td><span class="venue {{ 'v-k' if r.venue == 'kalshi' else 'v-p' }}">{{ 'K' if r.venue == 'kalshi' else 'P' }}</span>
    <a href="/table/{{ 'kalshi_candles' if r.venue == 'kalshi' else 'poly_candles' }}?ticker={{ r.id }}">{{ r.title[:58] }}</a></td>
  <td>{{ r.category or '' }}</td>
  <td><span class="pill {{ 'live' if r.active else 'done' }}">{{ 'active' if r.active else 'expired' }}</span></td>
  <td>{% if r.candles %}<span class="pill got">&check; downloaded</span>
      {% else %}<span class="pill none">no candles</span>{% endif %}</td>
  <td>{{ r.opened or '' }}</td>
  <td>{{ r.closes or '' }}</td>
  <td class="n {{ 'has' if r.candles else 'no' }}">{{ '{:,}'.format(r.candles) if r.candles else '-' }}</td>
  <td>{{ r.first or '' }}</td>
  <td>{{ r.last or '' }}</td>
  <td class="n">{{ '{:,.0f}'.format(r.volume or 0) }}</td>
</tr>
{%- endmacro %}
<!doctype html>
{{ THEME|safe }}
<title>manage</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 1.5rem; color: var(--ink); }
 header { display: flex; align-items: baseline; gap: 18px; }
 header h2 { margin: 0 0 6px; } header h2 a { text-decoration: none; color: inherit; }
 header .links a { font-size: 13px; margin-right: 12px; }
 .cards { display: flex; flex-wrap: wrap; gap: 12px; margin: 14px 0 18px; }
 .card { border: 1px solid var(--line); border-radius: 6px; padding: 10px 14px; min-width: 150px; }
 .card .n { font-size: 20px; font-weight: 700; }
 .card .l { font-size: 11px; color: var(--muted); text-transform: uppercase; letter-spacing: .03em; }
 .card.k { border-top: 3px solid var(--accent); } .card.p { border-top: 3px solid #99542a; }
 .card.on { border-top: 3px solid #4a7; }
 table { border-collapse: collapse; width: 100%; font-size: 13px; }
 th, td { border-bottom: 1px solid var(--line2); padding: 4px 7px; text-align: left; }
 th { background: var(--soft); position: sticky; top: 0; font-size: 12px; }
 td.n, th.n { text-align: right; font-variant-numeric: tabular-nums; }
 .controls { display: flex; flex-wrap: wrap; gap: 10px 14px; align-items: flex-end;
             margin-bottom: 14px; padding-bottom: 12px; border-bottom: 1px solid var(--line2); }
 .controls label { display: flex; flex-direction: column; font-size: 11px;
                   text-transform: uppercase; letter-spacing: .03em; color: var(--muted); }
 .controls select, .controls input { margin-top: 3px; padding: 4px 6px; font-size: 13px;
                                     text-transform: none; }
 .controls button { padding: 6px 12px; font-size: 13px; }
 .pill { display: inline-block; padding: 1px 6px; border-radius: 8px; font-size: 11px; }
 .live { background: var(--ok-soft); color: var(--ok); }
 .done { background: var(--line2); color: var(--muted); }
 .has { color: var(--ok); font-weight: 700; }
 .idx { color: var(--faint); font-size: 11px; }
 .pill.got { background: var(--ok-soft); color: var(--ok); font-weight: 700; }
 .pill.none { background: var(--soft); color: var(--faint); }
 tr.empty td { background: var(--soft2); color: var(--muted); }
 tr.empty td a { color: #6a7fa8; }
 .no { color: var(--faint); }
 .refresh { background: var(--accent-soft); border: 1px solid var(--accent-line); border-radius: 6px;
            padding: 12px 14px; margin-top: 22px; }
 .refresh h3 { margin: 0 0 6px; font-size: 14px; color: var(--accent); }
 .refresh button { background: var(--accent); color: #fff; border: 0; padding: 6px 12px;
                   border-radius: 3px; cursor: pointer; font-size: 13px;
                   margin-right: 6px; }
 .refresh button:hover { background: #1d4570; }
 .msg.err { background: var(--bad-soft); border-left-color: var(--bad); }
 .danger { background: var(--bad-soft); border: 1px solid var(--bad-line); border-radius: 6px;
           padding: 12px 14px; margin-top: 22px; }
 .danger h3 { margin: 0 0 6px; font-size: 14px; color: var(--bad); }
 .danger button { background: var(--bad); color: #fff; border: 0; padding: 6px 12px;
                  border-radius: 3px; cursor: pointer; font-size: 13px; }
 .msg { background: var(--ok-soft); border-left: 3px solid #4a4; padding: 6px 10px;
        margin-bottom: 10px; font-size: 13px; }
 .venue { display: inline-block; width: 13px; height: 13px; line-height: 13px;
          text-align: center; border-radius: 2px; font-size: 9px; font-weight: 700;
          color: #fff; margin-right: 5px; }
 .v-k { background: var(--accent); } .v-p { background: #99542a; }
</style>

<header>
  <h2><a href="/">prediction_markets</a> &middot; manage</h2>
  {{ NAV|safe }}
</header>

{% if msg %}<div class="msg">{{ msg }}</div>{% endif %}
{% if refresh_job %}
  <div class="msg {{ 'err' if refresh_job.state == 'error' else '' }}" id="rmsg">
    <b>{{ refresh }}</b>: <span id="rtext">{{ refresh_job.message }}</span>
    <span class="meta" id="rclock"></span>
  </div>
  {% if refresh_job.state == 'running' %}
  <script>
  (function () {
    var t0 = Date.now();
    var clock = document.getElementById('rclock'), text = document.getElementById('rtext');
    var tick = setInterval(function () {
      var s = Math.round((Date.now() - t0) / 1000);
      clock.textContent = '  ' + (s < 90 ? s + 's' : (s / 60).toFixed(1) + ' min');
    }, 1000);
    (function poll() {
      fetch('/job?table=refresh&ticker={{ refresh }}')
        .then(function (r) { return r.json(); })
        .then(function (j) {
          if (j.state === 'running') { text.textContent = j.message; setTimeout(poll, 3000); return; }
          clearInterval(tick);
          text.textContent = j.message || 'done';
          if (j.state !== 'error') location.href = '/manage';
        }).catch(function () { setTimeout(poll, 5000); });
    })();
  })();
  </script>
  {% endif %}
{% endif %}

<div class="cards">
  <div class="card k"><div class="n">{{ '{:,}'.format(s.k_markets) }}</div>
    <div class="l">Kalshi markets</div></div>
  <div class="card k"><div class="n">{{ '{:,}'.format(s.k_with) }}</div>
    <div class="l">with candles</div></div>
  <div class="card p"><div class="n">{{ '{:,}'.format(s.p_markets) }}</div>
    <div class="l">Polymarket markets</div></div>
  <div class="card p"><div class="n">{{ '{:,}'.format(s.p_with) }}</div>
    <div class="l">with candles</div></div>
  <div class="card on"><div class="n">{{ '{:,}'.format(s.active) }}</div>
    <div class="l">still active</div></div>
  <div class="card"><div class="n">{{ '{:,}'.format(s.expired) }}</div>
    <div class="l">expired</div></div>
  <div class="card"><div class="n">{{ s.size }}</div>
    <div class="l">database size</div></div>
</div>

<form class="controls" method="get" action="/manage">
  <label>venue
    <select name="venue" onchange="this.form.submit()">
      {% for v in ['both', 'kalshi', 'poly'] %}
        <option value="{{ v }}" {{ 'selected' if v == venue else '' }}>{{ v }}</option>
      {% endfor %}
    </select>
  </label>
  <label>state
    <select name="state" onchange="this.form.submit()">
      {% for v, l in [('all','all'), ('active','still active'), ('expired','expired'),
                      ('withdata','has candles'), ('nodata','no candles')] %}
        <option value="{{ v }}" {{ 'selected' if v == state else '' }}>{{ l }}</option>
      {% endfor %}
    </select>
  </label>
  <label>category
    <select name="category" onchange="this.form.submit()">
      <option value="">all</option>
      {% for c in categories %}
        <option value="{{ c }}" {{ 'selected' if c == category else '' }}>{{ c }}</option>
      {% endfor %}
    </select>
  </label>
  <label>sort
    <select name="sort" onchange="this.form.submit()">
      {% for v, l in [('candles','candle count'), ('volume','volume'),
                      ('close','close time'), ('open','open time')] %}
        <option value="{{ v }}" {{ 'selected' if v == sort else '' }}>{{ l }}</option>
      {% endfor %}
    </select>
  </label>
  <label>rows
    <select name="limit" onchange="this.form.submit()">
      {% for n in [100, 300, 1000, 3000] %}
        <option value="{{ n }}" {{ 'selected' if n == limit else '' }}>{{ '{:,}'.format(n) }}</option>
      {% endfor %}
      <option value="all" {{ 'selected' if limit is none else '' }}>show all</option>
    </select>
  </label>
  <label style="flex:1; min-width:200px">search
    <input name="q" value="{{ q or '' }}" placeholder="ticker or title">
  </label>
  <button type="submit">apply</button>
</form>

<form method="post" action="/manage/delete" onsubmit="return confirm('Delete the selected markets and all their candles?')">
  {% set got = rows | selectattr('candles') | list | length %}
  <p>{{ '{:,}'.format(rows|length) }} shown &mdash;
     <b class="okc">{{ '{:,}'.format(got) }} with candles</b>,
     {{ '{:,}'.format(rows|length - got) }} without{% if limit is none %}
     <span class="meta">(everything matching the filters)</span>{% endif %}
     &middot; <a href="#" onclick="document.querySelectorAll('.pick').forEach(c=>c.checked=true);return false">select all</a>
     &middot; <a href="#" onclick="document.querySelectorAll('.pick').forEach(c=>c.checked=false);return false">none</a>
     &middot; <button type="submit">delete selected</button></p>
  <table id="mtable">
    <tr>
      <th></th><th class="n">#</th><th>market</th><th>category</th><th>state</th>
      <th>data</th><th>opened</th><th>closes</th><th class="n">candles</th>
      <th>data from</th><th>data to</th><th class="n">volume</th>
    </tr>
    {% for r in rows %}{{ row_html(r, loop.index) }}{% endfor %}
  </table>
  {% if more %}
    <div class="sentinel" id="more">load more</div>
    <script>
    (function () {
      var el = document.getElementById('more');
      var tbl = document.getElementById('mtable');
      var offset = {{ rows|length }}, busy = false, done = false;
      var qs = {{ feed_qs|tojson }};

      function load() {
        if (busy || done) return;
        busy = true;
        el.textContent = 'loading\u2026';
        fetch('/manage/rows?' + qs + '&offset=' + offset)
          .then(function (r) { return r.json(); })
          .then(function (d) {
            if (!d.html) { done = true; el.textContent = 'end of list'; return; }
            tbl.insertAdjacentHTML('beforeend', d.html);
            offset += d.count;
            busy = false;
            el.textContent = d.count < d.page ? 'end of list' : 'load more';
            if (d.count < d.page) done = true;
            else near();          // keep filling while the sentinel is visible
          })
          .catch(function () { busy = false; el.textContent = 'load more (retry)'; });
      }
      function near() {
        var b = el.getBoundingClientRect();
        if (b.top <= (window.innerHeight || 0) + 400) load();
      }
      window.addEventListener('scroll', near, {passive: true});
      window.addEventListener('resize', near, {passive: true});
      el.addEventListener('click', load);
      near();
    })();
    </script>
  {% endif %}
</form>

<div class="refresh">
  <h3>fetch newer markets</h3>
  <form method="post" action="/manage/refresh">
    <button name="which" value="poly" type="submit">Polymarket &mdash; all sectors</button>
    <button name="which" value="kalshi_crypto" type="submit">Kalshi &mdash; crypto only</button>
    <button name="which" value="kalshi_all" type="submit">Kalshi &mdash; every sector</button>
  </form>
  <p style="font-size:12px;color:var(--muted);margin:8px 0 0">
    Runs discovery's listing now instead of waiting for its next pass (every 15 minutes).
    New markets are added and existing rows refreshed; nothing is deleted.
    Polymarket takes a few minutes, Kalshi crypto around ten, every sector over an hour.
  </p>
</div>

<div class="danger">
  <h3>bulk cleanup</h3>
  <form method="post" action="/manage/purge"
        onsubmit="return confirm('Delete every market that closed before the cutoff, plus its candles?')">
    Delete markets that closed more than
    <select name="days">
      {% for d in [7, 14, 30, 60, 90] %}<option value="{{ d }}">{{ d }}</option>{% endfor %}
    </select> days ago
    <select name="venue">
      <option value="both">on both venues</option>
      <option value="kalshi">on Kalshi only</option>
      <option value="poly">on Polymarket only</option>
    </select>
    <label style="font-size:13px; text-transform:none">
      <input type="checkbox" name="candles_only" value="1"> candles only, keep the market rows
    </label>
    <button type="submit">delete</button>
  </form>
  <p style="font-size:12px;color:var(--muted);margin:8px 0 0">
    Candles can always be re-downloaded on demand; market rows come back by re-running
    the scrapers. Nothing here touches the files in data/.
  </p>
</div>
"""


HOME_PAGE = """
<!doctype html>
{{ THEME|safe }}
<title>prediction_markets</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 1000px;
        padding: 0 1rem; color: var(--ink); line-height: 1.5; }
 h1 { margin: 0 0 4px; font-size: 22px; }
 .sub { color: var(--muted); margin-bottom: 22px; font-size: 14px; }
 .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
         gap: 14px; margin-bottom: 30px; }
 .card { border: 1px solid var(--line); border-radius: 6px; padding: 14px 16px;
         text-decoration: none; color: inherit; display: block; }
 .card:hover { border-color: var(--link); background: var(--accent-soft); }
 .card h3 { margin: 0 0 4px; font-size: 15px; }
 .card p { margin: 0 0 8px; font-size: 13px; color: var(--muted); }
 .card .rows { font-size: 12px; color: var(--muted); }
 .card .rows b { color: var(--ink); }
 .k { border-left: 3px solid var(--accent); }
 .p { border-left: 3px solid #99542a; }
 h2 { font-size: 16px; margin: 26px 0 8px; }
 table.cmp { border-collapse: collapse; font-size: 13px; width: 100%; }
 table.cmp th, table.cmp td { border: 1px solid var(--line); padding: 6px 9px;
                              text-align: left; vertical-align: top; }
 table.cmp th { background: var(--soft); }
 .note { background: var(--soft); border-left: 3px solid var(--line); padding: 10px 14px;
         font-size: 13px; margin: 14px 0; }
 code { background: var(--soft); padding: 1px 4px; border-radius: 3px; font-size: 12px; }
 .sub .links a { margin-left: 10px; font-size: 13px; }
</style>

<h1>prediction_markets</h1>
<div class="sub">Kalshi and Polymarket, same database, side by side. &nbsp;{{ NAV|safe }}</div>

<div class="grid">
  <a class="card k" href="/table/kalshi_markets?dim=category">
    <h3>Kalshi markets</h3>
    <p>Binary yes/no contracts. Browse by category, strike type or status.</p>
    <div class="rows"><b>{{ '{:,}'.format(counts.kalshi_markets) }}</b> markets &middot;
      <b>{{ '{:,}'.format(counts.kalshi_series) }}</b> series &middot;
      <b>{{ '{:,}'.format(counts.kalshi_candles) }}</b> candles</div>
  </a>
  <a class="card p" href="/table/poly_markets?dim=category">
    <h3>Polymarket markets</h3>
    <p>The same idea on-chain. Grouped under events rather than series.</p>
    <div class="rows"><b>{{ '{:,}'.format(counts.poly_markets) }}</b> markets &middot;
      <b>{{ '{:,}'.format(counts.poly_events) }}</b> events &middot;
      <b>{{ '{:,}'.format(counts.poly_candles) }}</b> candles</div>
  </a>
  <a class="card" href="/visualise">
    <h3>Visualise</h3>
    <p>Candlestick chart for any market whose candles are downloaded,
       bucketed to a readable number of bars.</p>
  </a>
  <a class="card" href="/manage">
    <h3>Manage data</h3>
    <p>What is downloaded, what is still active, what has expired &mdash; and
       delete or refresh markets you no longer need.</p>
  </a>
  <a class="card" href="/stats">
    <h3>Coverage stats</h3>
    <p>Both venues side by side: events, markets by sector, candles downloaded,
       and how many there would be in total.</p>
  </a>
  <a class="card" href="/status">
    <h3>Status</h3>
    <p>Everything happening now, in one place: live ingestion, the backfill,
       both venues' budgets, Polymarket's trade feed, the database and the API.</p>
  </a>
  <a class="card" href="/docs">
    <h3>Docs</h3>
    <p>How it all works and why: the architecture, every decision, and the
       API with examples.</p>
  </a>
  <a class="card" href="/table/kalshi_candles?dim=category">
    <h3>Candles</h3>
    <p>Price history per market. Anything not downloaded yet has a
       button to fetch it on demand.</p>
  </a>
</div>

<h2>The two venues</h2>
<table class="cmp">
  <tr><th></th><th>Kalshi</th><th>Polymarket</th></tr>
  <tr><td>What it is</td>
      <td>A CFTC-regulated exchange. Contracts settle at $0 or $1.</td>
      <td>An on-chain order book. Each outcome is its own token.</td></tr>
  <tr><td>Identifier</td>
      <td><code>KXBTCD-26AUG1311-T67599.99</code> &mdash; encodes series, date and strike</td>
      <td><code>0xa467b14d…</code> &mdash; a 66-character hash, plus a readable slug</td></tr>
  <tr><td>Grouping</td>
      <td>series &rarr; event &rarr; market</td>
      <td>event &rarr; market &rarr; token</td></tr>
  <tr><td>Price</td>
      <td colspan="2">Both quote 0&ndash;1, and that number is the implied probability.
        43&cent; means the market thinks 43%.</td></tr>
  <tr><td>Candles</td>
      <td>Real OHLCV, with volume and open interest per bar, from Kalshi.</td>
      <td>Real OHLCV made here from every trade (Polymarket has no candle API):
          volume in shares, notional in USDC, and the trade count per bar.</td></tr>
</table>

<h2>Why the expiry dates never line up</h2>
<p>This is the single biggest obstacle to comparing the two, so it is worth
being blunt about it.</p>

<p><b>Kalshi reissues the same question on a schedule.</b> Its crypto ladders
run hourly and every fifteen minutes &mdash; "Bitcoin above $62,000" exists
separately for 18:00, 19:00, 20:00 and so on, each a distinct market that
settles on the hour. A single day of one Bitcoin series is a few hundred
markets.</p>

<p><b>Polymarket lists one market per question.</b> "Will Bitcoin be above
$62,000 on August 15?" is one market that resolves once. There is no hourly
version.</p>

<p>So the same bet exists on both venues at the same price level, but almost
never at the same moment. In this database, matching on strike finds
counterparts; matching on expiry within an hour finds essentially none.</p>

<div class="note">
  Two further wrinkles that widen the gap.
  <b>Polymarket's <code>end_date</code> is the resolution timestamp, not the
  observation date</b> &mdash; "Will Bitcoin reach $73,000 on August 20?" carries an
  <code>end_date</code> of August 21. The loader parses the date out of the question text
  and stores it as <code>stated_date</code>, which is what actually lines up with a
  Kalshi market.
  And <b>the two venues settle at different times of day</b>: Kalshi's hourly
  markets close on the hour, while Polymarket's daily crypto markets resolve at a
  fixed afternoon UTC time.
</div>

<p>Practically: match on <b>strike</b> for crypto, widen the expiry tolerance
rather than expecting exact matches, and fall back to <b>close time</b> for
everything without a strike. Both are in the nearby panel on a market's page.</p>
"""


# The column each venue's markets are sorted by, newest or largest first. Each
# has an index in that order (schema.sql), so a page of /manage reads a few
# hundred rows of it rather than sorting all four million markets.
MANAGE_SORTS = {"volume": ("m.volume", "p.volume"),
                "close": ("m.close_time", "p.end_date"),
                "open": ("m.open_time", None)}
MANAGE_KALSHI = """
    SELECT 'kalshi' AS venue, m.ticker AS id, m.title, e.category,
           m.close_time > now() AS active,
           to_char(m.open_time, 'YYYY-MM-DD HH24:MI') AS opened,
           to_char(m.close_time, 'YYYY-MM-DD HH24:MI') AS closes,
           m.volume, c.n AS candles, to_char(c.first_ts, 'YYYY-MM-DD HH24:MI') AS first,
           to_char(c.last_ts, 'YYYY-MM-DD HH24:MI') AS last, {key} AS k
    FROM {source}
    JOIN kalshi_events e USING (event_ticker)
    WHERE {where} ORDER BY {key} DESC NULLS LAST, m.ticker LIMIT %s"""
MANAGE_POLY = """
    SELECT 'poly' AS venue, p.condition_id AS id, p.question AS title, e.category,
           p.closed IS NOT TRUE AS active, NULL AS opened,
           to_char(p.end_date, 'YYYY-MM-DD HH24:MI') AS closes,
           p.volume, c.n AS candles, to_char(c.first_ts, 'YYYY-MM-DD HH24:MI') AS first,
           to_char(c.last_ts, 'YYYY-MM-DD HH24:MI') AS last, {key} AS k
    FROM {source}
    LEFT JOIN poly_events e USING (event_id)
    WHERE {where} ORDER BY {key} DESC NULLS LAST, p.condition_id LIMIT %s"""


def _manage_rows(venue, state, category, sort, q, limit, offset=0):
    """One page of markets, with their candle coverage.

    Each venue gives its best offset + limit rows in the page's order, walking
    an index; the page is the merge of the two. Sorted by candles, the markets
    with candles come from the counts views (indexed by count) and those
    without after them, in id order."""
    counts_ready()
    like = f"%{q}%" if q else None
    n = offset + limit
    parts = []

    def part(template, source, key, cond, args):
        parts.append((template.format(source=source, key=key, where=" AND ".join(cond)),
                      args + [n]))

    for v, template, key_col, title, cid, counts, closed in (
            ("kalshi", MANAGE_KALSHI, "m.ticker", "m.title", "m.ticker", "kalshi_candle_counts",
             "m.close_time <= now()"),
            ("poly", MANAGE_POLY, "p.condition_id", "p.question", "p.condition_id",
             "poly_candle_counts", "p.closed IS TRUE")):
        if venue not in ("both", v):
            continue
        markets = "kalshi_markets m" if v == "kalshi" else "poly_markets p"
        join = "c.ticker = m.ticker" if v == "kalshi" else "c.condition_id = p.condition_id"
        cond, args = ["true"], []
        if category:
            cond.append("e.category = %s"); args.append(category)
        if q:
            cond.append(f"({cid} ILIKE %s OR {title} ILIKE %s)"); args += [like, like]
        if state == "active":
            cond.append(f"NOT ({closed})")
        elif state == "expired":
            cond.append(closed)
        with_counts = f"{counts} c JOIN {markets} ON {join}"
        without = f"{markets} LEFT JOIN {counts} c ON {join}"
        if sort == "candles":
            if state != "nodata":
                part(template, with_counts, "c.n", cond, args)
            if state != "withdata":
                part(template, without, "NULL::bigint", cond + ["c.n IS NULL"], args)
            continue
        key = MANAGE_SORTS[sort][0 if v == "kalshi" else 1] or "NULL::numeric"
        extra = {"withdata": ["c.n IS NOT NULL"], "nodata": ["c.n IS NULL"]}.get(state, [])
        part(template, without, key, cond + extra, args)

    if not parts:
        return []
    order = {"candles": "candles DESC NULLS LAST", "volume": "volume DESC NULLS LAST",
             "close": "k DESC NULLS LAST", "open": "k DESC NULLS LAST"}[sort]
    # A stable tiebreak keeps paging from repeating or skipping rows when the
    # sort column has ties (it usually does -- volume 0, no candles).
    sql = (" UNION ALL ".join(f"({s})" for s, _ in parts)
           + f" ORDER BY {order}, id LIMIT %s OFFSET %s")
    rows = db.query(sql, tuple(a for _, args in parts for a in args) + (limit, offset))
    # Kalshi's short outcome name lives in the stored listing; read it only for
    # the rows shown, not for every market the query sorted.
    ids = [r["id"] for r in rows if r["venue"] == "kalshi"]
    if ids:
        names = {r["ticker"]: r["sub"] for r in db.query(
            "SELECT ticker, raw->>'yes_sub_title' AS sub FROM kalshi_markets"
            " WHERE ticker = ANY(%s)", (ids,))}
        for r in rows:
            if r["venue"] == "kalshi" and names.get(r["id"]):
                r["title"] = names[r["id"]]
    return rows


def _manage_summary():
    return db.query("""
        SELECT (SELECT count(*) FROM kalshi_markets) k_markets,
               (SELECT count(*) FROM kalshi_candle_counts) k_with,
               (SELECT count(*) FROM poly_markets) p_markets,
               (SELECT count(*) FROM poly_candle_counts) p_with,
               (SELECT count(*) FROM kalshi_markets WHERE close_time > now())
             + (SELECT count(*) FROM poly_markets WHERE closed IS NOT TRUE) active,
               (SELECT count(*) FROM kalshi_markets WHERE close_time <= now())
             + (SELECT count(*) FROM poly_markets WHERE closed IS TRUE) expired,
               pg_size_pretty(pg_database_size(current_database())) size""")[0]


@app.route("/manage")
def manage():
    venue = request.args.get("venue", "both")
    state = request.args.get("state", "all")
    category = request.args.get("category") or None
    sort = request.args.get("sort", "candles")
    q = (request.args.get("q") or "").strip()
    raw_limit = request.args.get("limit", "300")
    if raw_limit == "all":
        limit = None          # no cap -- the whole filtered set
    else:
        try:
            limit = min(int(raw_limit), 3000)
        except ValueError:
            limit = 300

    counts_ready()
    s = _cached(("manage summary",), 900, _manage_summary)

    categories = _cached(("manage categories",), 600, lambda: [r["c"] for r in db.query("""
        SELECT DISTINCT category AS c FROM kalshi_events WHERE category IS NOT NULL
        UNION SELECT DISTINCT category FROM poly_events WHERE category IS NOT NULL
        ORDER BY c""")])

    # Render one page; the rest arrives as the sentinel scrolls into view.
    page = PAGE_ROWS if limit is None else min(limit, PAGE_ROWS)
    rows = _manage_rows(venue, state, category, sort, q, page)
    more = len(rows) == page and (limit is None or limit > page)

    feed_qs = urlencode({"venue": venue, "state": state, "category": category or "",
                         "sort": sort, "q": q,
                         "limit": "all" if limit is None else limit})

    return render_template_string(
        MANAGE_PAGE, s=s, categories=categories, rows=rows, more=more,
        feed_qs=feed_qs, total_cap=limit,
        venue=venue, state=state, category=category, sort=sort, q=q, limit=limit,
        msg=request.args.get("msg"), refresh=request.args.get("refresh"),
        refresh_job=(JOBS.get(f"refresh:{request.args.get('refresh')}")
                     if request.args.get("refresh") else None))


@app.route("/manage/rows")
def manage_rows():
    """Next page of manage rows, rendered server-side and appended by the page."""
    venue = request.args.get("venue", "both")
    state = request.args.get("state", "all")
    category = request.args.get("category") or None
    sort = request.args.get("sort", "candles")
    q = (request.args.get("q") or "").strip()
    raw_limit = request.args.get("limit", "all")
    cap = None if raw_limit == "all" else int(raw_limit)
    offset = int(request.args.get("offset", 0))

    page = PAGE_ROWS if cap is None else max(0, min(PAGE_ROWS, cap - offset))
    if page == 0:
        return jsonify({"html": "", "count": 0, "page": PAGE_ROWS})

    rows = _manage_rows(venue, state, category, sort, q, page, offset)
    tmpl = app.jinja_env.from_string(
        MANAGE_ROW + "{% for r in rows %}{{ row_html(r, offset + loop.index) }}{% endfor %}")
    return jsonify({"html": tmpl.render(rows=rows, offset=offset),
                    "count": len(rows), "page": page})


# Refreshing the catalogue runs the same sync the live collector does, which
# takes from a minute to several, so it runs as a background job like the
# candle downloads. Perpetuals are not collected live, so they still go
# through their scraper and the loader.
REFRESH_JOBS = {"kalshi_crypto": "kalshi markets (crypto)",
                "kalshi_all": "kalshi markets",
                "poly": "polymarket markets"}


def _refresh_steps(which):
    import catalogue
    return {"kalshi_crypto": [lambda: catalogue.sync_kalshi(categories=["Crypto"])],
            "kalshi_all": [catalogue.sync_series, catalogue.sync_kalshi],
            "poly": [catalogue.sync_poly]}[which]


def _run_refresh(job_id, which):
    started = time.monotonic()
    try:
        for step in _refresh_steps(which):
            with JOBS_LOCK:
                JOBS[job_id] = {"state": "running",
                                "message": f"refreshing {REFRESH_JOBS[which]}\u2026"}
            step()
        elapsed = time.monotonic() - started
        with JOBS_LOCK:
            JOBS[job_id] = {"state": "done",
                            "message": f"refreshed in {elapsed/60:.1f} min"}
    except Exception as exc:
        with JOBS_LOCK:
            JOBS[job_id] = {"state": "error", "message": f"{type(exc).__name__}: {exc}"}


@app.route("/manage/refresh", methods=["POST"])
def manage_refresh():
    which = request.form.get("which", "poly")
    if which not in REFRESH_JOBS:
        return redirect("/manage?msg=unknown+refresh+target")
    job_id = f"refresh:{which}"
    with JOBS_LOCK:
        running = JOBS.get(job_id, {}).get("state") == "running"
        if not running:
            JOBS[job_id] = {"state": "running", "message": "starting\u2026"}
    if not running:
        threading.Thread(target=_run_refresh, args=(job_id, which), daemon=True).start()
    return redirect(f"/manage?refresh={which}")


@app.route("/manage/delete", methods=["POST"])
def manage_delete():
    """Remove the ticked markets and everything hanging off them."""
    picked = request.form.getlist("id")
    kalshi = [p.split(":", 1)[1] for p in picked if p.startswith("kalshi:")]
    poly = [p.split(":", 1)[1] for p in picked if p.startswith("poly:")]

    removed = 0
    with connection() as conn:
        with conn.cursor() as cur:
            if kalshi:
                cur.execute("DELETE FROM kalshi_candles WHERE ticker = ANY(%s)", (kalshi,))
                cur.execute("DELETE FROM kalshi_markets WHERE ticker = ANY(%s)", (kalshi,))
                removed += cur.rowcount
            if poly:
                cur.execute("DELETE FROM poly_candles WHERE condition_id = ANY(%s)", (poly,))
                cur.execute("DELETE FROM poly_markets WHERE condition_id = ANY(%s)", (poly,))
                removed += cur.rowcount
    return redirect(f"/manage?msg={quote(f'deleted {removed} markets and their candles')}")


@app.route("/manage/purge", methods=["POST"])
def manage_purge():
    """Drop everything that closed before the cutoff."""
    days = int(request.form.get("days", 30))
    venue = request.form.get("venue", "both")
    candles_only = request.form.get("candles_only") == "1"
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    counts = {"kalshi candles": 0, "kalshi markets": 0,
              "poly candles": 0, "poly markets": 0}
    with connection() as conn:
        with conn.cursor() as cur:
            if venue in ("both", "kalshi"):
                cur.execute("DELETE FROM kalshi_candles WHERE ticker IN"
                            " (SELECT ticker FROM kalshi_markets WHERE close_time < %s)", (cutoff,))
                counts["kalshi candles"] = cur.rowcount
                if not candles_only:
                    cur.execute("DELETE FROM kalshi_markets WHERE close_time < %s", (cutoff,))
                    counts["kalshi markets"] = cur.rowcount
            if venue in ("both", "poly"):
                cur.execute("DELETE FROM poly_candles WHERE condition_id IN"
                            " (SELECT condition_id FROM poly_markets WHERE end_date < %s)",
                            (cutoff,))
                counts["poly candles"] = cur.rowcount
                if not candles_only:
                    cur.execute("DELETE FROM poly_markets WHERE end_date < %s", (cutoff,))
                    counts["poly markets"] = cur.rowcount

    summary = ", ".join(f"{v:,} {k}" for k, v in counts.items() if v)
    return redirect(f"/manage?msg={quote('deleted ' + (summary or 'nothing'))}")


STATUS_PAGE = """
<!doctype html>
{{ THEME|safe }}
<title>status</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 1150px;
        padding: 0 1rem; color: var(--ink); background: var(--bg); line-height: 1.5; }
 header { display: flex; align-items: baseline; gap: 18px; flex-wrap: wrap; }
 header h2 { margin: 0 0 6px; } header h2 a { text-decoration: none; color: inherit; }
 header .links a { font-size: 13px; margin-right: 12px; }
 .bar-nav { font-size: 13px; margin: 4px 0 14px; color: var(--muted); }
 .bar-nav a, .bar-nav b { margin-right: 10px; }
 .jump { font-size: 13px; margin: -6px 0 14px; }
 .jump a { margin-right: 12px; }
 .tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; margin-bottom: 10px; }
 .tile { border: 1px solid var(--line); border-radius: 6px; padding: 10px 14px; border-left-width: 4px; }
 .tile b { display: block; font-size: 22px; font-variant-numeric: tabular-nums; }
 .tile span { color: var(--muted); font-size: 12px; }
 .tile.good { border-left-color: #3a9a5b; } .tile.warn { border-left-color: #d9a400; }
 .tile.bad { border-left-color: #d9534f; } .tile.bad b { color: var(--bad); }
 h3.section { font-size: 17px; margin: 28px 0 0; padding-top: 14px; border-top: 2px solid var(--ink); }
 h3.section small { color: var(--faint); font-weight: normal; font-size: 13px; margin-left: 8px; }
 .col { border: 1px solid var(--line); border-radius: 6px; padding: 12px 16px; margin-top: 12px; min-width: 0; }
 .col h4 { margin: 0 0 8px; font-size: 14px; }
 .col h4 small { color: var(--faint); font-weight: normal; font-size: 12px; margin-left: 6px; }
 .two { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
 svg.chart { display: block; width: 100%; height: auto; }
 .axis { display: flex; justify-content: space-between; font-size: 11px; color: var(--faint); }
 .legend { font-size: 12px; color: var(--muted); margin-top: 6px; }
 .legend i { display: inline-block; width: 10px; height: 10px; border-radius: 2px;
             margin: 0 4px 0 12px; vertical-align: -1px; }
 .legend i:first-child { margin-left: 0; }
 .progress { background: var(--accent-soft); border-radius: 6px; height: 14px; overflow: hidden; }
 .progress div { background: var(--accent); height: 100%; }
 .big { font-size: 26px; font-weight: 700; font-variant-numeric: tabular-nums; }
 .job, .hbar { display: grid; gap: 8px; align-items: center; font-size: 13px; padding: 3px 0; }
 .job { grid-template-columns: minmax(0, 2fr) 3fr auto; border-bottom: 1px solid var(--soft); }
 .hbar { grid-template-columns: minmax(0, 2fr) 3fr 60px; }
 .hbar a, .job a { overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
 .hbar .progress, .job .progress { height: 10px; }
 .hbar span { text-align: right; color: var(--muted); font-variant-numeric: tabular-nums; }
 .job .progress div.done { background: #3a9a5b; } .job .progress div.failed { background: #d9534f; }
 .muted { color: var(--faint); } .s2 { color: var(--ok); } .s4 { color: var(--warn); } .s5 { color: #b3261e; font-weight: 600; }
 details { margin-top: 12px; }
 summary { cursor: pointer; font-weight: 600; font-size: 14px; }
 .scroll { overflow-x: auto; }
 table { border-collapse: collapse; width: 100%; font-size: 13px; }
 th, td { border-bottom: 1px solid var(--line2); padding: 3px 6px; text-align: right; white-space: nowrap; }
 th:first-child, td:first-child, td.l, th.l { text-align: left; }
 th { color: var(--muted); font-weight: normal; font-size: 12px; }
 td.wrap { white-space: normal; word-break: break-all; text-align: left; }
 .chk { display: inline-block; min-width: 34px; text-align: center; border-radius: 8px;
        font-size: 11px; padding: 1px 6px; font-weight: 700; }
 .chk-ok { background: var(--ok-soft); color: var(--ok); } .chk-warn { background: var(--warn-soft); color: var(--warn); }
 .chk-fail { background: var(--bad-soft); color: var(--bad); }
 @media (max-width: 800px) { .two { grid-template-columns: 1fr; } }
</style>

<header>
  <h2><a href="/">prediction_markets</a> &middot; status</h2>
  {{ NAV|safe }}
</header>
<div class="bar-nav">
  window:
  {% for w in windows %}{% if w == window %}<b>{{ w }}</b>{% else %}<a href="?window={{ w }}{% if not refresh %}&refresh=off{% endif %}">{{ w }}</a>{% endif %}{% endfor %}
  &middot;
  {% if refresh %}updating every 30 s &middot; <a href="?window={{ window }}&refresh=off">pause</a>
  {% else %}paused &middot; <a href="?window={{ window }}">resume</a>{% endif %}
  &middot; <span class="muted">as of <span id="asof">{{ now }}</span></span>
</div>
<div id="live">
<div class="jump"><a href="#live">live ingestion</a><a href="#backfill">backfill</a><a href="#budget">Kalshi budget</a><a href="#poly">Polymarket</a><a href="#check">daily check</a><a href="#discovery">discovery</a><a href="#database">database</a><a href="#api">API</a></div>

<div class="tiles">
  <div class="tile {{ 'bad' if c.down else 'good' }}"><b>{{ 'down' if c.down else 'up' }}</b>
    <span>Kalshi{% if c.down %} &middot; since {{ c.down }}{% endif %}</span></div>
  <div class="tile {{ 'good' if c.live and c.live.behind < 240 else 'warn' if c.live and c.live.behind < 900 else 'bad' }}">
    <b>{{ (c.live.behind|string + ' s') if c.live else '—' }}</b><span>live candles behind the clock</span></div>
  <div class="tile"><b>{{ '{:,}'.format(c.live.candles) if c.live else '—' }}</b>
    <span>candles in live's last cycle{% if c.live %} &middot; {{ c.live.seconds }} s{% endif %}</span></div>
  <div class="tile"><b>{{ '{:,}'.format(c.hot.n) if c.hot else '—' }}</b><span>hot markets, fetched every minute</span></div>
  <div class="tile {{ 'warn' if c.factor and c.factor < 1 else '' }}"><b>{{ c.used_now }}<span style="font-size:14px;color:var(--faint)"> / {{ c.limit }}</span></b>
    <span>Kalshi requests last minute{% if c.factor and c.factor < 1 %} &middot; cut to {{ '%.0f'|format(100 * c.factor) }}%{% endif %}</span></div>
  <div class="tile {{ 'good' if bf.left == 0 else '' }}"><b>{{ ('%.1f'|format(bf.pct) + '%') if bf.left is not none else '—' }}</b><span>backfill done</span></div>
</div>
<div class="tiles">
  <div class="tile"><b>{{ d.size }}</b><span>database on disk</span></div>
  <div class="tile"><b>{{ d.candles }}</b><span>candles stored</span></div>
  <div class="tile"><b>{{ d.markets }}</b><span>markets &middot; {{ d.open }} open</span></div>
  <div class="tile"><b>{{ d.connections }}</b><span>database connections in use</span></div>
  {% if not missing %}
  <div class="tile"><b>{{ '{:,}'.format(t.n) }}</b><span>API requests &middot; {{ window }}</span></div>
  <div class="tile {{ 'bad' if t.errors else 'good' }}"><b>{{ '{:,}'.format(t.errors) }}</b>
    <span>API errors &middot; typical {{ ms(t.p50) }}</span></div>
  {% endif %}
</div>

<h3 class="section" id="live">Live ingestion <small>live.py: hot markets every minute, others once they trade</small></h3>
<div class="two">
  <div class="col">
    <h4>Candles written per minute</h4>
    {{ m.candles_chart|safe }}
    <div class="axis"><span>{{ first_label }}</span><span>now</span></div>
  </div>
  <div class="col">
    <h4>Seconds behind the clock <small>about 120 s is normal</small></h4>
    {{ m.behind_chart|safe }}
    <div class="axis"><span>{{ first_label }}</span><span>now</span></div>
  </div>
</div>

<h3 class="section" id="backfill">Backfill <small>backfill.py: every market's history, on leftover budget</small></h3>
<div class="col">
  {% if bf.left is not none %}
  <div class="two" style="gap:24px;align-items:center">
    <div>
      <div class="big">{{ '%.1f'|format(bf.pct) }}% <span style="font-size:14px;font-weight:normal;color:var(--muted)">of markets have their history</span></div>
      <div class="progress"><div style="width:{{ bf.pct }}%"></div></div>
      <div class="muted" style="font-size:13px;margin-top:6px">{{ '{:,}'.format(bf.left) }} markets left &middot; {{ bf.pace|safe }}</div>
    </div>
    <div><h4 style="margin:0 0 4px;font-size:13px">Markets still to fetch</h4>{{ bf.chart|safe }}</div>
  </div>
  {% else %}<span class="muted">no count yet: the first appears within ten minutes of the viewer starting</span>{% endif %}
  <h4 style="margin-top:14px">Backfill requests per minute</h4>
  {{ m.backfill_chart|safe }}
  <div class="axis"><span>{{ first_label }}</span><span>now</span></div>
</div>

<h3 class="section" id="budget">Kalshi request budget <small>every program together &middot; dashed line = the limit</small></h3>
<div class="col">
  {{ m.budget_chart|safe }}
  <div class="axis"><span>{{ first_label }}</span><span>now</span></div>
  <div class="legend">{% for name, colour in m.legend %}<i style="background:{{ colour }}"></i>{{ name }}{% endfor %}
    {% if m.down %}<i style="background:var(--down)"></i>Kalshi unreachable{% endif %}</div>
</div>

<h3 class="section" id="poly">Polymarket <small>trades.py reads every trade; aggregate.py makes the candles</small></h3>
<div class="tiles">
  <div class="tile {{ 'bad' if p.down else 'good' }}"><b>{{ 'down' if p.down else 'up' }}</b>
    <span>Polymarket &middot; {{ 'through the VPN' if p.route == 'vpn' else 'direct' }}</span></div>
  <div class="tile {{ 'good' if p.behind is not none and p.behind < 600 else 'warn' if p.behind is not none and p.behind < 1800 else 'bad' if p.behind is not none else '' }}">
    <b>{{ (p.behind|string + ' s') if p.behind is not none else '—' }}</b><span>trade feed behind the clock &middot; claimed 30 s behind its newest trade</span></div>
  <div class="tile"><b>{{ '{:,}'.format(p.recent) if p.recent is not none else '—' }}</b><span>trades stored in the last 5 min</span></div>
  <div class="tile {{ 'warn' if p.behind_markets else '' }}"><b>{{ '{:,}'.format(p.behind_markets) if p.behind_markets is not none else '—' }}</b><span>markets to catch up after a gap</span></div>
  <div class="tile {{ 'warn' if p.waiting and p.waiting > 5000 else '' }}"><b>{{ '{:,}'.format(p.waiting) if p.waiting is not none else '—' }}</b><span>minutes waiting for their candle</span></div>
  <div class="tile"><b>{{ p.used }}<span style="font-size:14px;color:var(--faint)"> / {{ p.limit }}</span></b><span>Polymarket requests last minute</span></div>
</div>
<div class="col">
  {% if p.tracked %}
  <div class="big">{{ '%.1f'|format(p.pct) }}% <span style="font-size:14px;font-weight:normal;color:var(--muted)">of Polymarket markets have their history</span></div>
  <div class="progress"><div style="width:{{ p.pct }}%"></div></div>
  <div class="muted" style="font-size:13px;margin-top:6px">{{ '{:,}'.format(p.left) }} markets left, about one request each &middot;
    {{ p.trades }} trades stored</div>
  {% else %}<span class="muted">no Polymarket markets tracked yet: discovery adds them on its next pass</span>{% endif %}
</div>

<h3 class="section" id="check">Daily check <small>check.py, every day at 09:00 UTC: is yesterday's data right, complete and current?</small></h3>
<div class="col scroll">
  {% if checks %}
  <div class="muted" style="font-size:13px;margin-bottom:6px">last run {{ when(checks[0].run_at) }} &middot; pushed to your phone through ntfy</div>
  <table>
    {% for r in checks %}
    <tr><td><span class="chk chk-{{ r.status }}">{{ r.status }}</span></td><td>{{ r.name }}</td>
        <td class="wrap" style="word-break:normal">{{ r.value }}</td></tr>
    {% endfor %}
  </table>
  {% else %}<span class="muted">no run yet: the first is at 09:00 UTC (or run <code>python check.py --now</code>)</span>{% endif %}
</div>

<h3 class="section" id="discovery">Discovery and scoring <small>discover.py lists every market; activity.py scores each scan</small></h3>
<div class="col scroll">
  <table>
    <tr><th>scan</th><th>started</th><th>listing took</th><th>scored</th></tr>
    {% for s in scans %}
    <tr><td>{{ s.scan_id }}</td><td>{{ when(s.started_at) }}</td>
        <td>{{ s.took or 'still running' }}</td><td>{{ when(s.scored_at) if s.scored_at else '—' }}</td></tr>
    {% else %}<tr><td colspan="4" class="muted">no scans yet</td></tr>{% endfor %}
  </table>
</div>

<h3 class="section" id="database">Database <small>PostgreSQL with TimescaleDB</small></h3>
<div class="two">
  <div class="col">
    <h4>Candles stored <small>every 10 minutes</small></h4>
    {{ m.stored_chart|safe }}
    <div class="axis"><span>{{ first_label }}</span><span>now</span></div>
  </div>
  <div class="col">
    <h4>Space by table</h4>
    {% for r in d.tables %}
    <div class="hbar"><span style="text-align:left;color:var(--ink)">{{ r.name }}</span>
      <div class="progress"><div style="width:{{ r.pct }}%"></div></div><span>{{ r.size }}</span></div>
    {% endfor %}
  </div>
</div>

<h3 class="section" id="api">API <small>who calls it, and how fast it answers</small></h3>
{% if missing %}
<div class="col muted">No API requests recorded yet.</div>
{% else %}
<div class="col">
  <h4>Requests <small>per {{ bucket_label }}, by caller &middot; red mark = errors</small></h4>
  {{ calls.chart|safe }}
  <div class="axis"><span>{{ first_label }}</span><span>now</span></div>
  <div class="legend">{% for name, colour in calls.legend %}<i style="background:{{ colour }}"></i>{{ name }}{% endfor %}</div>
</div>
<div class="col">
  <h4>Response time <small>typical and slowest 5%, per {{ bucket_label }}</small></h4>
  {{ speed_chart|safe }}
  <div class="axis"><span>{{ first_label }}</span><span>now</span></div>
  <div class="legend"><i style="background:var(--accent)"></i>typical<i style="background:#e07b39"></i>slowest 5%</div>
</div>
<div class="two">
  <div class="col">
    <h4>Most asked-for markets</h4>
    {% for r in tickers %}
    <div class="hbar"><a href="/table/kalshi_candles?ticker={{ r.t|urlencode }}" title="{{ r.t }}">{{ r.t }}</a>
      <div class="progress"><div style="width:{{ r.pct }}%"></div></div><span>{{ '{:,}'.format(r.n) }}</span></div>
    {% else %}<span class="muted">none in this window</span>{% endfor %}
  </div>
  <div class="col">
    <h4>Backfill jobs <small>pushed through the API, latest first</small></h4>
    {% for j in jobs %}
    <div class="job">
      <a href="/table/kalshi_candles?ticker={{ j.ticker|urlencode }}" title="{{ j.error or j.ticker }}">{{ j.ticker }}</a>
      <div class="progress"><div class="{{ j.status }}" style="width:{{ j.pct }}%"></div></div>
      <span class="{{ 's5' if j.status == 'failed' else 's2' if j.status == 'done' else '' }}">{{ j.status }}
        &middot; {{ '{:,}'.format(j.candles) }} candles</span>
    </div>
    {% else %}<span class="muted">none yet</span>{% endfor %}
  </div>
</div>

<details>
  <summary>Details: callers, endpoints, latest requests, errors</summary>
  <div class="col">
    <h4>Who is calling</h4>
    <div class="scroll"><table>
      <tr><th>key</th><th class="l">machine / client</th><th class="l">ip</th><th>requests</th><th>errors</th><th>last</th></tr>
      {% for r in callers %}
      <tr><td>{{ r.name }}</td><td class="l">{{ r.user_agent or '' }}</td><td class="l">{{ r.client_ip or '' }}</td>
          <td>{{ '{:,}'.format(r.n) }}</td><td>{{ r.e }}</td><td>{{ when(r.last) }}</td></tr>
      {% else %}<tr><td colspan="6" class="muted">no requests in this window</td></tr>{% endfor %}
    </table></div>
  </div>
  <div class="col">
    <h4>Endpoints</h4>
    <div class="scroll"><table>
      <tr><th>endpoint</th><th>requests</th><th>errors</th><th>typical</th><th>slowest 5%</th><th>rows</th><th>sent</th></tr>
      {% for r in endpoints %}
      <tr><td>{{ r.method }} {{ r.ep }}</td><td>{{ '{:,}'.format(r.n) }}</td><td>{{ r.e }}</td>
          <td>{{ ms(r.p50) }}</td><td>{{ ms(r.p95) }}</td><td>{{ '{:,}'.format(r.rows_sent) }}</td><td>{{ size(r.bytes) }}</td></tr>
      {% else %}<tr><td colspan="7" class="muted">none in this window</td></tr>{% endfor %}
    </table></div>
  </div>
  <div class="col">
    <h4>Latest requests <small>the last 50, whatever the window</small></h4>
    <div class="scroll"><table>
      <tr><th>time</th><th class="l">key</th><th class="l">request</th><th>status</th><th>ms</th><th>rows</th><th>sent</th><th class="l">client</th></tr>
      {% for r in recent %}
      <tr><td>{{ when(r.at) }}</td><td class="l">{{ r.name or '—' }}</td>
          <td class="wrap">{{ r.method }} {{ r.path }}{% if r.query %}<span class="muted">?{{ r.query }}</span>{% endif %}</td>
          <td class="s{{ r.status // 100 }}">{{ r.status }}{% if r.error %} {{ r.error }}{% endif %}</td>
          <td>{{ '%.0f'|format(r.ms) }}</td><td>{{ '{:,}'.format(r.row_count) if r.row_count is not none else '' }}</td>
          <td>{{ size(r.bytes) if r.bytes else '' }}</td><td class="l muted">{{ r.user_agent or '' }}</td></tr>
      {% else %}<tr><td colspan="8" class="muted">nothing yet</td></tr>{% endfor %}
    </table></div>
  </div>
  <div class="col">
    <h4>Recent errors <small>in this window</small></h4>
    <div class="scroll"><table>
      <tr><th>time</th><th class="l">request</th><th>status</th></tr>
      {% for r in errors %}
      <tr><td>{{ when(r.at) }}</td><td class="wrap">{{ r.method }} {{ r.path }}{% if r.query %}<span class="muted">?{{ r.query }}</span>{% endif %}</td>
          <td class="s{{ r.status // 100 }}">{{ r.status }} {{ r.error or '' }}</td></tr>
      {% else %}<tr><td colspan="3" class="muted">no errors</td></tr>{% endfor %}
    </table></div>
  </div>
</details>
{% endif %}
</div>
{% if refresh %}
<script>
// Every 30 s, the page's numbers are fetched again and swapped in place:
// no reload, no jump in the scroll, open details stay open; nothing while
// the tab is hidden.
(function () {
  setInterval(function () {
    if (document.hidden) return;
    fetch(location.href).then(function (r) { return r.text(); }).then(function (html) {
      var next = new DOMParser().parseFromString(html, 'text/html').getElementById('live');
      var cur = document.getElementById('live');
      if (!next || !cur) return;
      var open = Array.prototype.map.call(cur.querySelectorAll('details[open]'),
                                          function (d) { return d.id; });
      open.forEach(function (id) { var d = id && next.querySelector('#' + id); if (d) d.open = true; });
      cur.replaceWith(next);
      var clock = document.getElementById('asof');
      if (clock) clock.textContent = new Date().toTimeString().slice(0, 8);
    }).catch(function () {});
  }, 30000);
})();
</script>
{% endif %}
"""

# window: (length in seconds, seconds per bar, what a bar is called)
USAGE_WINDOWS = {"1h": (3600, 60, "1 min"), "24h": (86400, 600, "10 min"),
                 "7d": (7 * 86400, 3600, "1 hour"), "30d": (30 * 86400, 6 * 3600, "6 hours")}
USAGE_TZ = ZoneInfo(os.environ.get("PGTIMEZONE", "Asia/Kolkata"))
PROGRAM_COLOURS = [("discover", "#9aa3ad"), ("hot", "#e07b39"), ("warm", "#d9a400"),
                   ("api", "#8e6bbf"), ("backfill", "var(--accent)")]
CALLER_COLOURS = ["#2a6099", "#e07b39", "#3a9a5b", "#8e6bbf", "#c9a227", "#9aa3ad"]
CHART_W, CHART_H = 1000, 150


def _ms(value):
    if value is None:
        return "—"
    return f"{value:.0f} ms" if value < 1000 else f"{value / 1000:.1f} s"


def _size(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or unit == "TB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def _count(n):
    """1,234 / 1.2M / 3.4B."""
    if n is None:
        return "—"
    for div, suffix in ((1e9, "B"), (1e6, "M")):
        if n >= div:
            return f"{n / div:.1f}{suffix}"
    return f"{n:,.0f}"


def _when(dt):
    if dt is None:
        return ""
    local = dt.astimezone(USAGE_TZ)
    recent = datetime.now(USAGE_TZ) - local < timedelta(hours=20)
    return local.strftime("%H:%M:%S" if recent else "%d %b %H:%M")


def _query(params):
    return "&".join(f"{k}={v}" for k, v in (params or {}).items())


def _nice(top):
    """A round number at or above `top`, for an axis."""
    if top <= 0:
        return 1
    step = 10 ** int(math.floor(math.log10(top)))
    for m in (1, 2, 2.5, 5, 10):
        if m * step >= top:
            return m * step
    return 10 * step


def _bars_svg(buckets, colours, limit=None, shade=None, marks=None, height=CHART_H):
    """Stacked bars. buckets: [{name: value}] oldest first; colours: [(name,
    colour)] bottom first; limit: [value] per bucket, drawn dashed; shade:
    [bool] per bucket, tinted red behind; marks: [bool] per bucket, a red
    tick on top. Returns the <svg> as a string."""
    n = len(buckets) or 1
    top = _nice(max([sum(b.values()) for b in buckets] + [max(limit or [0])] + [0]) * 1.05)
    w = CHART_W / n
    out = [f'<svg class="chart" viewBox="0 0 {CHART_W} {height + 14}" role="img">']
    for frac in (0.5, 1):
        y = height - height * frac
        out.append(f'<line x1="0" x2="{CHART_W}" y1="{y:.1f}" y2="{y:.1f}" stroke="var(--line2)"/>'
                   f'<text x="2" y="{y + 11:.1f}" font-size="11" fill="var(--faint)">{_count(top * frac)}</text>')
    for i, b in enumerate(buckets):
        x = i * w
        if shade and shade[i]:
            out.append(f'<rect x="{x:.2f}" y="0" width="{w:.2f}" height="{height}" fill="var(--down)"/>')
        y = height
        for name, colour in colours:
            v = b.get(name, 0)
            if v <= 0:
                continue
            h = height * v / top
            y -= h
            out.append(f'<rect x="{x:.2f}" y="{y:.2f}" width="{max(w - 0.6, 0.4):.2f}" height="{h:.2f}"'
                       f' fill="{colour}"><title>{name}: {v:,.1f}</title></rect>')
        if marks and marks[i]:
            out.append(f'<rect x="{x:.2f}" y="{y - 4:.2f}" width="{max(w - 0.6, 1.5):.2f}" height="3"'
                       f' fill="#d9534f"/>')
    if limit:
        pts = " ".join(f"{i * w:.1f},{height - height * v / top:.1f} {(i + 1) * w:.1f},"
                       f"{height - height * v / top:.1f}" for i, v in enumerate(limit))
        out.append(f'<polyline points="{pts}" fill="none" stroke="#b3261e" stroke-width="1.5"'
                   f' stroke-dasharray="6 4"/>')
    out.append(f'<line x1="0" x2="{CHART_W}" y1="{height}" y2="{height}" stroke="var(--line)"/></svg>')
    return "".join(out)


def _lines_svg(series, height=CHART_H, unit=""):
    """Lines. series: [(colour, [value or None] oldest first)]; gaps break the line."""
    values = [v for _, vals in series for v in vals if v is not None]
    top = _nice(max(values + [0]) * 1.05)
    n = max([len(vals) for _, vals in series] + [1])
    step = CHART_W / max(n - 1, 1)
    out = [f'<svg class="chart" viewBox="0 0 {CHART_W} {height + 14}" role="img">']
    for frac in (0.5, 1):
        y = height - height * frac
        out.append(f'<line x1="0" x2="{CHART_W}" y1="{y:.1f}" y2="{y:.1f}" stroke="var(--line2)"/>'
                   f'<text x="2" y="{y + 11:.1f}" font-size="11" fill="var(--faint)">{_count(top * frac)}{unit}</text>')
    for colour, vals in series:
        run = []
        for i, v in enumerate(vals + [None]):
            if v is None:
                if len(run) == 1:
                    out.append(f'<circle cx="{run[0][0]:.1f}" cy="{run[0][1]:.1f}" r="2" fill="{colour}"/>')
                elif run:
                    pts = " ".join(f"{x:.1f},{y:.1f}" for x, y in run)
                    out.append(f'<polyline points="{pts}" fill="none" stroke="{colour}" stroke-width="1.8"/>')
                run = []
            else:
                run.append((i * step, height - height * v / top))
    out.append(f'<line x1="0" x2="{CHART_W}" y1="{height}" y2="{height}" stroke="var(--line)"/></svg>')
    return "".join(out)


def _caller(name, agent):
    """'laptop · BHAGE-PC' from a key name and a User-Agent."""
    agent = agent or ""
    m = re.match(r"pmapi/[\d.]+ \((.+)\)", agent)
    who = m.group(1) if m else (agent.split("/")[0] or "unknown client")
    return f"{name or 'no valid key'} · {who}"


def _collection():
    """What live, the hot list and the budget say right now, from Redis."""
    out = {"limit": settings.KALSHI_REQUESTS_PER_MIN, "down": None, "live": None, "hot": None,
           "factor": None, "used_now": "—"}
    try:
        r = ratelimit.client()
        host = settings.KALSHI_API_URL.split("/")[2]
        down = r.get(f"{settings.REDIS_PREFIX}down:{host}")
        if down:
            out["down"] = _when(datetime.fromtimestamp(float(down), timezone.utc))
        live = r.get(ratelimit.KEY + "live")
        if live:
            live = json.loads(live)
            out["live"] = {"behind": round(time.time() - live["minute"]),
                           "candles": live["hot"]["candles"] + live["warm"]["candles"],
                           "seconds": live["seconds"]}
        hot = r.get(ratelimit.KEY + "hot")
        if hot:
            hot = json.loads(hot)
            out["hot"] = {"n": len(hot["tickers"]), "cap": hot["cap"]}
        out["factor"] = ratelimit.factor()
        out["used_now"] = sum(ratelimit.used(s, 1) or 0 for s in ratelimit.SHARE)
    except Exception:
        pass
    return out


def _poly():
    """Polymarket's side, from Redis and the bookkeeping tables."""
    out = {"down": None, "route": None, "behind": None, "recent": None, "behind_markets": None,
           "waiting": None, "used": "—", "limit": settings.POLY_REQUESTS_PER_MIN, "tracked": 0}
    try:
        r = ratelimit.client()
        out["down"] = r.get(f"{settings.REDIS_PREFIX}down:data-api.polymarket.com")
        out["route"] = r.get(f"{settings.REDIS_PREFIX}route:data-api.polymarket.com") or (
            "vpn" if settings.VPN_PROXY else "direct")
        out["used"] = sum(ratelimit.used(s, 1, "poly") or 0 for s in ratelimit.SHARE)
    except Exception:
        pass
    try:
        now = db.query("""
            SELECT EXTRACT(EPOCH FROM now() - (SELECT covered_to FROM poly_feed)) AS behind,
                   (SELECT count(*) FROM poly_trades WHERE ts > now() - interval '5 minutes') AS recent,
                   (SELECT count(*) FROM poly_dirty) AS waiting""")[0]
        out.update(behind=None if now["behind"] is None else round(now["behind"]),
                   recent=now["recent"], waiting=now["waiting"])

        def counts():
            c = db.query(f"""
                SELECT count(*) AS tracked,
                       count(*) FILTER (WHERE {poly.HISTORY_DONE}) AS done,
                       count(*) FILTER (WHERE {poly.HISTORY_DONE} AND {poly.BEHIND}) AS behind
                FROM poly_tracking t""")[0]
            c["trades"] = _count(db.query("SELECT approximate_row_count('poly_trades') AS n")[0]["n"])
            return c
        c = _cached(("poly counts",), 120, counts)
        out.update(tracked=c["tracked"], left=c["tracked"] - c["done"],
                   behind_markets=c["behind"], trades=c["trades"],
                   pct=100 * c["done"] / c["tracked"] if c["tracked"] else 0)
    except Exception:
        pass
    return out


def _checks():
    """The newest daily check, failures first (none before the first run)."""
    try:
        return db.query("SELECT run_at, name, status, value FROM checks"
                        " WHERE run_at = (SELECT max(run_at) FROM checks)"
                        " ORDER BY CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, name")
    except Exception:
        return []


def _database():
    """Sizes and counts, from the database's own bookkeeping: instant."""
    def compute():
        sizes = db.query("""
            SELECT 'kalshi_candles' AS name, hypertable_size('kalshi_candles') AS b
            UNION ALL
            SELECT relname, pg_total_relation_size(c.oid) FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public' AND c.relname IN ('kalshi_markets', 'kalshi_tracking',
                  'kalshi_events', 'kalshi_titles', 'api_requests', 'kalshi_candle_counts',
                  'poly_markets', 'poly_tracking', 'poly_events', 'poly_candle_counts',
                  'propositions', 'templates')
            UNION ALL
            SELECT 'poly_trades', hypertable_size('poly_trades')
            UNION ALL
            SELECT 'poly_candles', hypertable_size('poly_candles')""")
        sizes.sort(key=lambda r: -(r["b"] or 0))
        top = max([r["b"] or 0 for r in sizes] or [1]) or 1
        markets = db.query("SELECT GREATEST(reltuples, 0)::bigint AS n FROM pg_class"
                           " WHERE oid = 'kalshi_markets'::regclass")[0]["n"]
        open_now = db.query("SELECT count(*) AS n FROM kalshi_markets WHERE close_time > now()")[0]["n"]
        return {"size": _size(db.query("SELECT pg_database_size(current_database()) AS n")[0]["n"]),
                "candles": _count(db.query("SELECT approximate_row_count('kalshi_candles') AS n")[0]["n"]),
                "markets": _count(markets), "open": _count(open_now),
                "tables": [{"name": r["name"].replace("_", " "), "size": _size(r["b"]),
                            "pct": round(100 * (r["b"] or 0) / top)} for r in sizes]}
    d = dict(_cached(("database",), 120, compute))
    d["connections"] = db.query("SELECT count(*) AS n FROM pg_stat_activity"
                                " WHERE datname = current_database()")[0]["n"]
    return d


def _monitor_charts(starts, step):
    """Live, backfill, budget and storage charts, from monitor.py's minute files."""
    lines = monitor.read(starts[0])
    by_bucket = {}
    for line in lines:
        by_bucket.setdefault(line["t"] // step * step, []).append(line)
    budget, limit, down, behind, candles, backfill = [], [], [], [], [], []
    for s in starts:
        got = by_bucket.get(s, [])
        minutes = max(len(got), 1)
        totals = {}
        for line in got:
            for k, v in line.get("used", {}).items():
                totals[k] = totals.get(k, 0) + v
        budget.append({k: v / minutes for k, v in totals.items()})
        backfill.append({"backfill": totals.get("backfill", 0) / minutes})
        limit.append(max([line.get("limit", 0) * line.get("factor", 1) for line in got] or
                         [settings.KALSHI_REQUESTS_PER_MIN]))
        down.append(any(line.get("up") is False for line in got))
        lags = [line["behind"] for line in got if "behind" in line]
        behind.append(sum(lags) / len(lags) if lags else None)
        made = [line["lc"] for line in got if "lc" in line]
        candles.append({"live": sum(made) / len(made)} if made else {})

    counts = [(line["t"], line["left"]) for line in lines if "left" in line]
    stored = [(line["t"], line["cs"]) for line in lines if "cs" in line]
    bf = {"left": None}
    if counts:
        left = counts[-1][1]
        total = max(db.query("SELECT reltuples::bigint AS n FROM pg_class"
                             " WHERE oid = 'kalshi_tracking'::regclass")[0]["n"] or 0, left)
        recent = [c for c in counts if c[0] >= counts[-1][0] - 6 * 3600]
        rate = ((recent[0][1] - recent[-1][1]) / ((recent[-1][0] - recent[0][0]) / 3600)
                if len(recent) > 1 and recent[-1][0] > recent[0][0] else 0)
        if left == 0:
            pace = "complete"
        elif rate > 0:
            hours = left / rate
            pace = (f"{rate:,.0f} markets an hour over the last {(recent[-1][0] - recent[0][0]) / 3600:.0f} h"
                    f" &middot; about {hours:.0f} h to go" if hours < 72 else
                    f"{rate:,.0f} markets an hour &middot; about {hours / 24:.1f} days to go")
        else:
            pace = "not moving in the last 6 h" if len(recent) > 1 else "measuring the pace"
        bf = {"left": left, "pct": max(0.0, min(100.0, 100 * (1 - left / total))) if total else 0,
              "pace": pace, "chart": _lines_svg([("var(--accent)", [c[1] for c in counts])], height=90)}
    return {"budget_chart": _bars_svg(budget, PROGRAM_COLOURS, limit=limit, shade=down),
            "legend": PROGRAM_COLOURS, "down": any(down),
            "behind_chart": _lines_svg([("var(--accent)", behind)], unit=" s"),
            "candles_chart": _bars_svg(candles, [("live", "#e07b39")]),
            "backfill_chart": _bars_svg(backfill, [("backfill", "var(--accent)")], height=100),
            "stored_chart": _lines_svg([("var(--accent)", [c[1] for c in stored] or [None])])}, bf


@app.route("/api-usage")
def api_usage():
    """The old address of the status page."""
    return redirect("/status" + (f"?{request.query_string.decode()}" if request.query_string else ""))


@app.route("/status")
def status_page():
    window = request.args.get("window", "24h")
    if window not in USAGE_WINDOWS:
        window = "24h"
    length, step, bucket_label = USAGE_WINDOWS[window]
    refresh = request.args.get("refresh") != "off"
    end = int(time.time() // step * step)
    starts = list(range(end - (length // step - 1) * step, end + step, step))
    # While the history is shorter than the window, the charts start where it
    # does instead of leaving most of their width empty (at least 12 bars).
    first = [line["t"] for line in monitor.read(starts[0])[:1]]
    api_table = db.query("SELECT to_regclass('api_requests') IS NOT NULL AS ok")[0]["ok"]
    if api_table:
        at = db.query("SELECT EXTRACT(EPOCH FROM min(at)) AS t FROM api_requests"
                      " WHERE at >= to_timestamp(%s)", (starts[0],))[0]["t"]
        first += [float(at)] if at is not None else []
    if first:
        starts = starts[-max(12, len([s for s in starts if s + step > min(first)])):]
    charts, bf = _monitor_charts(starts, step)
    scans = db.query("SELECT scan_id, started_at, finished_at, scored_at FROM kalshi_scans"
                     " ORDER BY scan_id DESC LIMIT 6")
    for s in scans:
        s["took"] = (f"{(s['finished_at'] - s['started_at']).total_seconds() / 60:.0f} min"
                     if s["finished_at"] else None)
    page = dict(windows=list(USAGE_WINDOWS), window=window, refresh=refresh,
                bucket_label=bucket_label, now=datetime.now(USAGE_TZ).strftime("%H:%M:%S"),
                ms=_ms, size=_size, when=_when, c=_collection(), d=_database(), p=_poly(),
                missing=False,
                m=charts, bf=bf, scans=scans, checks=_checks(),
                first_label=_when(datetime.fromtimestamp(starts[0], timezone.utc)))
    if not api_table:
        return render_template_string(STATUS_PAGE, **page, missing=True)

    since = {"length": length, "step": step}
    where = "r.at >= now() - make_interval(secs => %(length)s)"
    t = db.query(f"""
        SELECT count(*) AS n, count(*) FILTER (WHERE status >= 400) AS errors,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95,
               COALESCE(sum(bytes), 0) AS bytes, COALESCE(sum(row_count), 0) AS rows_sent
        FROM api_requests r WHERE {where}""", since)[0]
    per = db.query(f"""
        SELECT floor(EXTRACT(EPOCH FROM r.at) / %(step)s) * %(step)s AS b, k.name, r.user_agent,
               count(*) AS n, count(*) FILTER (WHERE r.status >= 400) AS e
        FROM api_requests r LEFT JOIN api_keys k USING (key_id)
        WHERE {where} GROUP BY 1, 2, 3""", since)
    totals = {}
    for r in per:
        who = _caller(r["name"], r["user_agent"])
        totals[who] = totals.get(who, 0) + r["n"]
    named = sorted(totals, key=totals.get, reverse=True)[:5]
    legend = list(zip(named, CALLER_COLOURS))
    if len(totals) > 5:
        legend.append(("others", CALLER_COLOURS[5]))
    buckets = {s: {} for s in starts}
    errors = {s: 0 for s in starts}
    for r in per:
        s = int(r["b"])
        if s not in buckets:
            continue
        who = _caller(r["name"], r["user_agent"])
        who = who if who in named else "others"
        buckets[s][who] = buckets[s].get(who, 0) + r["n"]
        errors[s] += r["e"]
    calls = {"chart": _bars_svg([buckets[s] for s in starts], legend,
                                marks=[errors[s] > 0 for s in starts]), "legend": legend}
    speed = {int(r["b"]): r for r in db.query(f"""
        SELECT floor(EXTRACT(EPOCH FROM r.at) / %(step)s) * %(step)s AS b,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95
        FROM api_requests r WHERE {where} GROUP BY 1""", since)}
    speed_chart = _lines_svg([("var(--accent)", [speed[s]["p50"] if s in speed else None for s in starts]),
                              ("#e07b39", [speed[s]["p95"] if s in speed else None for s in starts])],
                             unit=" ms")
    tickers = db.query(f"""
        SELECT tk AS t, count(*) AS n FROM api_requests r, unnest(r.tickers) AS tk
        WHERE {where} GROUP BY tk ORDER BY n DESC LIMIT 12""", since)
    most = max([r["n"] for r in tickers] or [1])
    tickers = [dict(r, pct=round(100 * r["n"] / most)) for r in tickers]
    jobs = [dict(j, pct=100 if j["status"] == "done" else
                 round(100 * j["requests_done"] / j["requests_total"]) if j["requests_total"] else 0)
            for j in db.query("SELECT * FROM api_jobs ORDER BY job_id DESC LIMIT 12")]
    callers = db.query(f"""
        SELECT COALESCE(k.name, '(no valid key)') AS name, r.user_agent, r.client_ip,
               count(*) AS n, count(*) FILTER (WHERE r.status >= 400) AS e, max(r.at) AS last
        FROM api_requests r LEFT JOIN api_keys k USING (key_id)
        WHERE {where} GROUP BY 1, 2, 3 ORDER BY n DESC LIMIT 20""", since)
    endpoints = db.query(f"""
        SELECT r.method, COALESCE(r.endpoint, r.path) AS ep, count(*) AS n,
               count(*) FILTER (WHERE r.status >= 400) AS e,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY r.ms) AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY r.ms) AS p95,
               COALESCE(sum(r.row_count), 0) AS rows_sent, COALESCE(sum(r.bytes), 0) AS bytes
        FROM api_requests r WHERE {where} GROUP BY 1, 2 ORDER BY n DESC LIMIT 25""", since)
    recent = db.query("""
        SELECT r.*, k.name FROM api_requests r LEFT JOIN api_keys k USING (key_id)
        ORDER BY r.at DESC LIMIT 50""")
    errs = db.query(f"""
        SELECT r.* FROM api_requests r WHERE {where} AND r.status >= 400
        ORDER BY r.at DESC LIMIT 20""", since)
    for r in recent + errs:
        r["query"] = _query(r["params"])
    return render_template_string(
        STATUS_PAGE, **page, t=t, calls=calls, speed_chart=speed_chart, tickers=tickers,
        jobs=jobs, callers=callers, endpoints=endpoints, recent=recent, errors=errs)


_cache = {}
_cache_lock = threading.Lock()


def _cached(key, ttl, compute):
    """compute(), remembered for `ttl` seconds."""
    now = time.monotonic()
    with _cache_lock:
        hit = _cache.get(key)
    if hit and now - hit[0] < ttl:
        return hit[1]
    value = compute()
    with _cache_lock:
        _cache[key] = (now, value)
    return value


def keep_fresh(stop):
    """The slow numbers, worked out every 10 minutes in the background so no
    page waits for them: each market's candle count, the stats page, the
    manage page's totals and the visualise picker."""
    while not stop.is_set():
        try:
            with connection() as conn:
                with conn.cursor() as cur:
                    refresh_counts(cur)
            for key, compute in ((("stats",), _stats_data), (("manage summary",), _manage_summary),
                                 (("plottable",), _plottable)):
                value = compute()
                with _cache_lock:
                    _cache[key] = (time.monotonic(), value)
        except Exception as exc:
            print(f"background refresh failed: {type(exc).__name__}: {exc}", flush=True)
        stop.wait(600)


@app.route("/docs")
def docs():
    """The architecture and API document (docs/architecture.html), as it is."""
    return Response((paths.ROOT / "docs" / "architecture.html").read_text(encoding="utf-8"),
                    mimetype="text/html")


@app.route("/stats")
def stats():
    return render_template_string(STATS_PAGE, **_cached(("stats",), 900, _stats_data))


def _stats_data():
    def sectors(source):
        rows = db.query(f"SELECT e.category, count(*) n FROM {source}"
                        " GROUP BY 1 ORDER BY n DESC LIMIT 12")
        top = max([r["n"] for r in rows] or [1])
        return [dict(category=r["category"], n=r["n"],
                     pct=round(100 * r["n"] / top)) for r in rows]

    k = db.query("SELECT count(*) markets, count(DISTINCT e.series_ticker) series,"
                 " count(DISTINCT m.event_ticker) events"
                 " FROM kalshi_markets m JOIN kalshi_events e USING (event_ticker)")[0]
    kc = db.query("SELECT count(*) candles, count(DISTINCT ticker) covered"
                  " FROM kalshi_candles")[0]
    km = db.query("SELECT COALESCE(sum(GREATEST(0, EXTRACT(EPOCH FROM"
                  " (LEAST(close_time, now()) - open_time)) / 60)), 0)::bigint AS m"
                  " FROM kalshi_markets WHERE open_time IS NOT NULL"
                  " AND close_time IS NOT NULL")[0]["m"]

    p = db.query("SELECT count(*) markets, count(DISTINCT event_id) events"
                 " FROM poly_markets")[0]
    pc = db.query("SELECT count(*) candles, count(DISTINCT condition_id) covered"
                  " FROM poly_candles")[0]
    pm = db.query("SELECT COALESCE(sum(GREATEST(0, EXTRACT(EPOCH FROM"
                  " (LEAST(end_date, now()) - COALESCE((raw->>'startDate')::timestamptz,"
                  "  end_date - interval '30 days'))) / 60)), 0)::bigint AS m"
                  " FROM poly_markets WHERE end_date IS NOT NULL")[0]["m"]

    # Kalshi only returns a candle when the market traded or was quoted, so its
    # ceiling is discounted by roughly what the downloaded sample fills in.
    fill = 0.15
    return dict(
        k=dict(k, **kc, minutes=km, fill=fill, estimate=int(km * fill),
               sectors=sectors("kalshi_markets m JOIN kalshi_events e USING (event_ticker)")),
        p=dict(p, **pc, minutes=pm, estimate=pm,
               sectors=sectors("poly_markets m LEFT JOIN poly_events e USING (event_id)")))


@app.route("/")
def index():
    counts = dict(table_list())
    for name in ("kalshi_markets", "kalshi_series", "kalshi_events", "kalshi_candles", "poly_markets",
                 "poly_events", "poly_candles"):
        counts.setdefault(name, 0)
    return render_template_string(HOME_PAGE, counts=counts)


# Columns you can slice a table by. First entry is the default.
DIMENSIONS = {
    "kalshi_markets": ["category", "strike_type", "status", "result", "series_ticker"],
    "poly_markets": ["category", "strike_type", "closed", "event_slug"],
    "kalshi_candles": ["category", "strike_type", "status"],
    "poly_candles": ["category", "strike_type"],
    "kalshi_series": ["category", "frequency"],
    "kalshi_events": ["category", "series_ticker"],
    "poly_events": ["category"],
}


def build_panels(name, value, ticker, dim="", limit=200):
    """Left panel: pick a dimension, then a value, then a single market."""
    dims = DIMENSIONS.get(name, [])
    if not dims:
        return []
    dim = dim if dim in dims else dims[0]

    # Candle tables hold only ids and numbers, so they slice on their
    # market's (or its event's) columns; counts are of markets.
    source, col, _ = dim_source(name, dim)
    base = f"/table/{name}?dim={dim}"

    rows = _cached(("panel", source, col), 300, lambda: db.query(
        f"SELECT {col}::text AS v, count(*) n FROM {source}"
        f" WHERE {col} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 300"))
    panels = [dict(title=dim.replace("_", " "), dims=dims, dim=dim, table=name,
                   options=[(r["v"], f"{r['n']:,}") for r in rows],
                   selected=value, url=f"{base}&asset=", all_url=base)]
    if not value or name in ("kalshi_series", "poly_events"):
        return panels

    # Second level: the markets carrying that value, busiest first.
    return panels


NEAREST_TITLES = 60        # distinct titles fetched before the threshold is applied
NEAREST_KEEP = 600         # seconds a title's nearest titles are remembered
_nearest = {}              # title -> (fetched at, [(title, score)]), nearest first
_nearest_lock = threading.Lock()


def nearest_titles(text):
    """The NEAREST_TITLES distinct Kalshi titles closest to `text`, with their
    similarity. Finding them is the slow part (~0.4 s over half a million
    titles) and does not depend on the slider, so they are remembered: moving
    the slider then costs only the market lookup."""
    now = time.monotonic()
    with _nearest_lock:
        hit = _nearest.get(text)
    if hit and now - hit[0] < NEAREST_KEEP:
        return hit[1]
    rows = db.query("SELECT title, similarity(title, %s) AS score FROM kalshi_titles"
                    " ORDER BY title <-> %s LIMIT %s", (text, text, NEAREST_TITLES))
    found = [(r["title"], float(r["score"])) for r in rows]
    with _nearest_lock:
        if len(_nearest) > 2000:
            _nearest.clear()
        _nearest[text] = (now, found)
    return found


_related_pool = ThreadPoolExecutor(max_workers=8, thread_name_prefix="related")


def related_markets(name, ident, threshold=0.25, limit=25, same_venue=True):
    """Markets whose wording resembles this one, on either venue.

    Uses trigram similarity (pg_trgm), which scores 0-1 on how much text two
    titles share. The threshold is exposed as a slider because the useful value
    depends entirely on the market: Kalshi ladder titles are near-identical to
    their siblings and need a high bar, while a Polymarket question phrased
    differently about the same event may only score 0.2.

    Titles are deduplicated -- a Kalshi day-ladder has hundreds of markets with
    one title, and listing them all would bury anything genuinely related. On
    Kalshi that is done by matching kalshi_titles, one row per distinct title,
    nearest first through its GiST index, and then taking each title's busiest
    market: well under a second, where scoring every market took seconds.
    """
    if name in POLY:
        anchor = db.query("SELECT question AS t FROM poly_markets"
                          " WHERE condition_id = %s", (ident,))
    else:
        anchor = db.query("SELECT title AS t FROM kalshi_markets WHERE ticker = %s", (ident,))
    if not anchor or not anchor[0]["t"]:
        return None
    text = anchor[0]["t"]

    # Rank within each venue before merging. A global top-N fills up with
    # Kalshi ladder siblings (they score 0.7-1.0 against each other) and the
    # Polymarket market asking the same question -- which scores ~0.4 because
    # it is worded differently -- never makes the cut.
    per = max(6, limit // 2)
    args = {"t": text, "id": ident, "thr": threshold, "per": per}

    def kalshi_side():
        scores = {t: sc for t, sc in nearest_titles(text) if sc >= threshold}
        with connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT DISTINCT ON (m.title) 'kalshi' AS venue, m.ticker AS id,
                           m.title, e.category, m.volume,
                           to_char(m.close_time, 'YYYY-MM-DD HH24:MI') AS closes,
                           EXISTS (SELECT 1 FROM kalshi_candles c
                                   WHERE c.ticker = m.ticker) AS got
                    FROM kalshi_markets m JOIN kalshi_events e USING (event_ticker)
                    WHERE m.title = ANY(%(titles)s) AND m.ticker <> %(id)s
                    ORDER BY m.title, m.volume DESC NULLS LAST""",
                            dict(args, titles=list(scores)))
                found = [dict(r, score=scores[r["title"]]) for r in cur.fetchall()]
        found.sort(key=lambda r: (-r["score"], -float(r["volume"] or 0)))
        return found[:per]

    def poly_side():
        with connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # The % operator filters at pg_trgm's own similarity_threshold
                # (0.3 by default), so a slider set lower than that would
                # silently do nothing. Set it so the index and the slider agree.
                cur.execute("SET LOCAL pg_trgm.similarity_threshold = %s", (threshold,))
                cur.execute("""
                    SELECT * FROM (
                        SELECT DISTINCT ON (p.question) 'poly' AS venue, p.condition_id AS id,
                               p.question AS title, e.category, p.volume,
                               similarity(p.question, %(t)s) AS score,
                               to_char(p.end_date, 'YYYY-MM-DD HH24:MI') AS closes,
                               EXISTS (SELECT 1 FROM poly_candles c
                                       WHERE c.condition_id = p.condition_id) AS got
                        FROM poly_markets p LEFT JOIN poly_events e USING (event_id)
                        WHERE p.question %% %(t)s AND p.condition_id <> %(id)s
                        ORDER BY p.question, p.volume DESC NULLS LAST) x
                    WHERE score >= %(thr)s
                    ORDER BY score DESC, volume DESC NULLS LAST LIMIT %(per)s""", args)
                return cur.fetchall()

    # The two venues are searched at the same time, each on its own
    # connection: the panel waits for the slower one, not for both in turn.
    sides = []
    if same_venue or name in POLY:
        sides.append(kalshi_side)
    if same_venue or name not in POLY:
        sides.append(poly_side)
    rows = [r for part in _related_pool.map(lambda f: f(), sides) for r in part]

    # Show which words drove the match, so a low score is interpretable.
    stop = {"the","a","an","will","be","by","on","in","at","to","of","and","or",
            "is","it","for","before","after","than","this","that","price","2026","2027"}
    anchor_words = {w for w in re.findall(r"[a-z0-9$,\.]+", text.lower())
                    if len(w) > 2 and w not in stop}
    out = []
    for r in rows:
        words = {w for w in re.findall(r"[a-z0-9$,\.]+", (r["title"] or "").lower())
                 if len(w) > 2 and w not in stop}
        shared = sorted(anchor_words & words)[:4]
        out.append(dict(r, score=float(r["score"]), shared=shared,
                        href=("/table/poly_candles?ticker=" if r["venue"] == "poly"
                              else "/table/kalshi_candles?ticker=") + r["id"]))
    return {"anchor": text, "threshold": threshold, "rows": out}


@app.route("/nearby")
def nearby_feed():
    """Nearby-market list for the current settings.

    Exists so changing a tolerance does not reload the page -- on a market with
    100k candles that meant re-running the whole candle query to redraw a side
    panel.
    """
    name = request.args.get("table", "kalshi_candles")
    ident = request.args.get("ticker", "")
    try:
        data = closing_near(name, ident,
                            hours=float(request.args.get("near", 1)),
                            same_category=request.args.get("cat", "1") != "0",
                            by=request.args.get("by", "time"),
                            points=float(request.args.get("pts", 5)))
    except ValueError:
        data = None
    if not data:
        return jsonify({"rows": [], "kalshi_n": 0, "poly_n": 0})
    return jsonify({
        "rows": data["rows"], "kalshi_n": data["kalshi_n"], "poly_n": data["poly_n"],
        "by": data["by"], "hours": data["hours"],
        "points": data["points"], "when": data["when"],
        "category": data["category"] if data["same_category"] else None,
        "anchor_last": data["anchor_last"],
    })


@app.route("/related")
def related_feed():
    """Related-market list at a given threshold, re-fetched as the slider moves."""
    name = request.args.get("table", "kalshi_candles")
    ident = request.args.get("ticker", "")
    try:
        threshold = min(max(float(request.args.get("t", 0.25)), 0.05), 0.95)
    except ValueError:
        threshold = 0.25
    data = related_markets(name, ident, threshold=threshold)
    if not data:
        return jsonify({"rows": []})
    return jsonify({"threshold": threshold, "rows": data["rows"]})


# A market's price is its last trade, worked out when asked: the newest
# candle if it is at least as new as the listing discovery last read, else the
# last price in that listing (Kalshi's own, up to one scan old). `lc` is the
# newest candle, `t` the market's tracking row.
LAST_PRICE = """CASE WHEN lc.ts IS NOT NULL AND (t.volume_measured_at IS NULL
                         OR lc.ts >= t.volume_measured_at - interval '1 minute')
                    THEN lc.close
                    ELSE COALESCE(NULLIF(m.raw->>'last_price_dollars', '')::numeric, lc.close)
               END"""
NEWEST_CANDLE = """LEFT JOIN LATERAL (SELECT c.ts, c.close FROM kalshi_candles c
                                    WHERE c.ticker = m.ticker
                                    ORDER BY c.ts DESC LIMIT 1) lc ON true"""


def closing_near(name, ident, hours=1, same_category=True, by="time",
                 points=5.0, limit=40):
    """Markets near this one, by whichever measure you pick.

    by = time    resolve within `hours` of each other
         price   currently trade within `points` percentage points -- among
                 markets still open, since a closed market's price is final
    Combine with ",", e.g. "price,time". Restricted to the anchor's category
    by default -- a coincidence in an unrelated sector is not a comparison.
    """
    if name in POLY:
        anchor = db.query(
            "SELECT COALESCE(p.stated_date::timestamptz, p.end_date) AS t,"
            " p.question AS title, e.category,"
            " (SELECT c.close FROM poly_candles c WHERE c.condition_id = p.condition_id"
            "   ORDER BY c.ts DESC LIMIT 1) AS last"
            " FROM poly_markets p LEFT JOIN poly_events e USING (event_id)"
            " WHERE p.condition_id = %s", (ident,))
    else:
        anchor = db.query(f"""
            SELECT m.close_time AS t, m.title, e.category, {LAST_PRICE} AS last
            FROM kalshi_markets m JOIN kalshi_events e USING (event_ticker)
            LEFT JOIN kalshi_tracking t USING (ticker)
            {NEWEST_CANDLE}
            WHERE m.ticker = %s""", (ident,))
    if not anchor:
        return None

    a = anchor[0]
    # "+" in a query string decodes to a space, so the separator is a comma;
    # accept either so old links keep working. Strike is no longer offered.
    wants = {w for w in by.replace("+", ",").replace(" ", ",").split(",")
             if w in ("time", "price")} or {"time"}
    by = ",".join(sorted(wants, key=lambda w: w != "price"))
    category = a["category"] if same_category else None

    # Narrow in SQL where a cheap index exists; the rest is filtered below.
    where_k, args_k = ["true"], []
    where_p, args_p = ["true"], []
    if category:
        where_k.append("e.category = %s")
        where_p.append("e.category = %s")
        args_k.append(category)
        args_p.append(category)
    if "time" in wants and a["t"]:
        span = timedelta(hours=hours)
        where_k.append("m.close_time BETWEEN %s AND %s")
        where_p.append("COALESCE(p.stated_date::timestamptz, p.end_date) BETWEEN %s AND %s")
        args_k += [a["t"] - span, a["t"] + span]
        args_p += [a["t"] - span, a["t"] + span]
    if "price" in wants:
        if a["last"] is None:
            return dict(_near_result(a, same_category, by, hours, points), rows=[])
        # The listing's price narrows the field, with room for it to be one
        # scan behind; the exact test is below, on the newer price where
        # there is one. A market with no listing price goes on to that test.
        where_k += ["m.close_time > now()",
                    "(NULLIF(m.raw->>'last_price_dollars', '') IS NULL"
                    " OR abs((m.raw->>'last_price_dollars')::numeric - %s) <= %s)"]
        args_k += [a["last"], (points + 10) / 100]
        where_p.append("NOT COALESCE(p.closed, false)")

    pool = 600
    kalshi = db.query(f"""
        SELECT x.id, x.title, x.category, x.close, x.volume, 'Kalshi' AS venue,
               {LAST_PRICE.replace("m.raw->>'last_price_dollars'", "x.listed")} AS last
        FROM (SELECT m.ticker AS id, COALESCE(m.raw->>'yes_sub_title', m.title) AS title,
                     e.category, m.close_time AS close, m.volume,
                     m.raw->>'last_price_dollars' AS listed
              FROM kalshi_markets m JOIN kalshi_events e USING (event_ticker)
              WHERE {' AND '.join(where_k)}
              ORDER BY m.volume DESC NULLS LAST LIMIT {pool}) x
        LEFT JOIN kalshi_tracking t ON t.ticker = x.id
        LEFT JOIN LATERAL (SELECT c.ts, c.close FROM kalshi_candles c
                           WHERE c.ticker = x.id ORDER BY c.ts DESC LIMIT 1) lc ON true""",
        tuple(args_k))

    poly = db.query(f"""
        SELECT p.condition_id AS id, p.question AS title, e.category,
               COALESCE(p.stated_date::timestamptz, p.end_date) AS close,
               p.volume, 'Polymarket' AS venue,
               (SELECT c.close FROM poly_candles c WHERE c.condition_id = p.condition_id
                 ORDER BY c.ts DESC LIMIT 1) AS last
        FROM poly_markets p LEFT JOIN poly_events e USING (event_id)
        WHERE {' AND '.join(where_p)}
        ORDER BY p.volume DESC NULLS LAST LIMIT {pool}""", tuple(args_p))

    rows = []
    for r in list(kalshi) + list(poly):
        if r["id"] == ident:
            continue
        gap = abs((r["close"] - a["t"]).total_seconds()) if (r["close"] and a["t"]) else None
        if "price" in wants:
            if r["last"] is None:
                continue
            if abs(float(r["last"]) - float(a["last"])) * 100 > points:
                continue
        href = ("/table/poly_candles?ticker=" if r["venue"] == "Polymarket"
                else "/table/kalshi_candles?ticker=") + r["id"]
        rows.append(dict(r, close=str(r["close"])[:16], href=href, gap=gap or 0,
                         last=float(r["last"]) if r["last"] is not None else None))

    # Sort by whichever measure was asked for.
    if "price" in wants:
        rows.sort(key=lambda r: (abs(r["last"] - float(a["last"])), -(float(r["volume"] or 0))))
    else:
        rows.sort(key=lambda r: (r["gap"], -(float(r["volume"] or 0))))
    return dict(_near_result(a, same_category, by, hours, points), rows=rows[:limit],
                kalshi_n=sum(1 for r in rows if r["venue"] == "Kalshi"),
                poly_n=sum(1 for r in rows if r["venue"] == "Polymarket"))


def _near_result(a, same_category, by, hours, points):
    return {"anchor": a["title"], "category": a["category"],
            "same_category": same_category, "by": by, "hours": hours, "points": points,
            "anchor_last": float(a["last"]) if a["last"] is not None else None,
            "when": str(a["t"])[:16], "rows": [], "kalshi_n": 0, "poly_n": 0}


def fetch_candles(name, ident):
    """Download one market's history now and store it: Kalshi's candles, or
    Polymarket's trades (aggregate.py makes their candles). Returns (how many
    were new, a problem or None).

    Runs from the market's own start to its close, or now if it is still open.
    """
    now = int(datetime.now(timezone.utc).timestamp())

    if name in POLY:
        # Its whole trade list, as backfill and the API read it; aggregate.py
        # makes the candles from the trades.
        if not db.query("SELECT 1 FROM poly_tracking WHERE condition_id = %s", (ident,)):
            return 0, "market not tracked yet: discovery adds it on its next pass"
        h = db.query("SELECT EXTRACT(EPOCH FROM history_from) AS h FROM poly_tracking"
                     " WHERE condition_id = %s", (ident,))[0]["h"]
        got = poly.fetch_market(ident, since=float(h) - 60)
        if got is None:
            return 0, "Polymarket did not answer; try again"
        return got, None if got else "no new trades: everything listed is already stored"

    # The same trades-only fetch live.py and backfill.py use, which also moves
    # the market's bookkeeping forward.
    found = db.query("SELECT open_time, close_time FROM kalshi_markets"
                     " WHERE ticker = %s", (ident,))
    if not found:
        return 0, "market not found"
    row = found[0]
    lo = int(row["open_time"].timestamp()) if row["open_time"] else now - 30 * 86400
    lo = max(lo, now - settings.BACKFILL_DAYS * 86400)
    hi = (min(int(row["close_time"].timestamp()) + 120, now)
          if row["close_time"] else now)
    hi = candle_fetch.minute(hi - settings.SETTLE_SECONDS)
    if hi <= lo:
        return 0, "market has no elapsed time yet"
    total = 0
    for planned in candle_fetch.plan([(ident, candle_fetch.minute(lo), hi)]):
        n = candle_fetch.run(planned)
        if n is None:
            return total, "Kalshi did not answer; try again"
        total += n
    return total, None if total else "no trades in this market's history"


# Downloads run on a worker thread so the request returns immediately; the
# page polls this registry instead of holding the connection open.
JOBS = {}
JOBS_LOCK = threading.Lock()


def _run_fetch(job_id, name, ident):
    started = time.monotonic()
    try:
        count, problem = fetch_candles(name, ident)
        elapsed = time.monotonic() - started
        state = (("error", problem) if problem else
                 ("done", f"downloaded {count:,} {'trades' if name in POLY else 'candles'}"
                          f" in {elapsed:.0f}s"))
    except Exception as exc:
        state = ("error", f"{type(exc).__name__}: {exc}")
    with JOBS_LOCK:
        JOBS[job_id] = {"state": state[0], "message": state[1]}


@app.route("/job")
def job_state():
    """Progress for one download, polled by the page."""
    key = f"{request.args.get('table','')}:{request.args.get('ticker','')}"
    with JOBS_LOCK:
        job = JOBS.get(key)
    return jsonify(job or {"state": "idle", "message": ""})


@app.route("/fetch")
def fetch():
    name = request.args.get("table", "kalshi_candles")
    ident = request.args.get("ticker", "")
    job_id = f"{name}:{ident}"

    with JOBS_LOCK:
        running = JOBS.get(job_id, {}).get("state") == "running"
        if not running:
            JOBS[job_id] = {"state": "running", "message": "downloading..."}
    if not running:
        threading.Thread(target=_run_fetch, args=(job_id, name, ident),
                         daemon=True).start()

    return redirect(f"/table/{name}?dim={request.args.get('dim', '')}"
                    f"&asset={request.args.get('asset', '')}&ticker={ident}&job=1")


def _human_delta(delta):
    """Rough "94 days" / "6 hours" -- enough to see at a glance that it is open."""
    seconds = int(delta.total_seconds())
    if seconds < 3600:
        return f"{max(seconds // 60, 1)} min"
    if seconds < 86400:
        return f"{seconds // 3600} hours"
    return f"{seconds // 86400} days"


def _table_query(name, asset, ticker, dim, q):
    """WHERE clause and params for a table view, shared by the page and feed."""
    key = key_col(name)
    where, params = "", ()

    if ticker:
        where, params = f" WHERE {key} = %s", (ticker,)
    elif asset and dim in DIMENSIONS.get(name, []):
        # Through the market and its event, which is where most dimensions live.
        source, col, src_key = dim_source(name, dim)
        where = f" WHERE {key} IN (SELECT {src_key} FROM {source} WHERE {col}::text = %s)"
        params = (asset,)

    if q:
        text_cols = ({"kalshi_markets": ["ticker", "title"],
                      "poly_markets": ["condition_id", "question", "slug"],
                      "kalshi_series": ["series_ticker", "title"],
                      "kalshi_events": ["event_ticker", "title", "sub_title"],
                      "poly_events": ["event_id", "title", "slug"]}
                     .get(name, [key]))
        clause = " OR ".join(f"{c}::text ILIKE %s" for c in text_cols)
        where += (" AND " if where else " WHERE ") + f"({clause})"
        params = tuple(params) + tuple(f"%{q}%" for _ in text_cols)

    # Candles are a time series, newest first -- the recent end is what anyone
    # opening a market wants to see. Everything else orders by its primary key.
    order = " ORDER BY ts DESC" if name.endswith("_candles") else f" ORDER BY {key}"
    return where, params, order


def _table_rows(name, asset, ticker, dim, q, limit, offset=0):
    where, params, order = _table_query(name, asset, ticker, dim, q)
    return db.query(f'SELECT * FROM "{name}"{where}{order}'
                    f" LIMIT {int(limit)} OFFSET {int(offset)}", params or None)


@app.route("/table/rows")
def table_rows():
    """Next page of a table view, rendered as <tr> and appended by the page."""
    name = request.args.get("table", "kalshi_markets")
    if name not in [t for t, _ in table_list()]:
        return jsonify({"html": "", "count": 0})
    offset = int(request.args.get("offset", 0))
    cap = request.args.get("limit", "all")
    page = TABLE_PAGE if cap == "all" else max(0, min(TABLE_PAGE, int(cap) - offset))
    if page == 0:
        return jsonify({"html": "", "count": 0, "page": TABLE_PAGE})

    rows = _table_rows(name, request.args.get("asset"), request.args.get("ticker"),
                       request.args.get("dim", ""), (request.args.get("q") or "").strip(),
                       page, offset)
    cols = [c for c in (rows[0].keys() if rows else []) if c != "raw"]
    candle_table = {"kalshi_markets": "kalshi_candles", "poly_markets": "poly_candles"}.get(name)
    tmpl = app.jinja_env.from_string(TABLE_ROWS)
    return jsonify({"html": tmpl.render(rows=rows, cols=cols, candle_table=candle_table,
                                        start=offset),
                    "count": len(rows), "page": page})


@app.route("/table/<name>")
def table(name):
    if name not in [t for t, _ in table_list()]:
        return render(error=f"no such table: {name}")

    dims = DIMENSIONS.get(name, [])
    dim = request.args.get("dim") or (dims[0] if dims else "")
    dim = dim if dim in dims else (dims[0] if dims else "")
    asset = request.args.get("asset")
    ticker = request.args.get("ticker")

    q = (request.args.get("q") or "").strip()
    raw_limit = request.args.get("limit", "")
    if raw_limit == "all":
        cap = None
    else:
        try:
            cap = min(int(raw_limit) if raw_limit else
                      (500 if (asset or ticker) else 200), 10000)
        except ValueError:
            cap = 200
    limit = TABLE_PAGE if cap is None else min(cap, TABLE_PAGE)

    rows = _table_rows(name, asset, ticker, dim, q, limit)
    table_more = len(rows) == limit and (cap is None or cap > limit)
    table_qs = urlencode({"table": name, "asset": asset or "", "ticker": ticker or "",
                          "dim": dim or "", "q": q,
                          "limit": "all" if cap is None else cap})

    # Clicking a ticker in a market table jumps to that market's candles.
    candle_table = {"kalshi_markets": "kalshi_candles", "poly_markets": "poly_candles"}.get(name)

    # A candle table shows only ids and numbers, so say what the market is.
    detail = None
    if ticker and name == "kalshi_candles":
        found = db.query("SELECT title, result, close_time, volume,"
                         " raw -> 'yes_sub_title' AS yes_sub_title"
                         " FROM kalshi_markets WHERE ticker = %s", (ticker,))
        detail = found[0] if found else None
    elif ticker and name == "poly_candles":
        found = db.query("SELECT question AS title, end_date AS close_time, volume,"
                         " slug AS yes_sub_title, NULL AS result, closed AS is_closed"
                         " FROM poly_markets WHERE condition_id = %s", (ticker,))
        detail = found[0] if found else None

    # Volume alone hides how the trading was spread: 780 contracts could be one
    # minute or seven. Count the candles, and how many of them actually traded.
    if detail:
        if name == "poly_candles":
            spread = db.query("SELECT count(*) AS bars, NULL::bigint AS traded,"
                              " min(ts) AS first, max(ts) AS last"
                              " FROM poly_candles WHERE condition_id = %s", (ticker,))
        else:
            spread = db.query("SELECT count(*) AS bars,"
                              " count(*) FILTER (WHERE volume > 0) AS traded,"
                              " min(ts) AS first, max(ts) AS last"
                              " FROM kalshi_candles WHERE ticker = %s", (ticker,))
        detail = dict(detail, **spread[0])

        # "closed" and "settled" are different things, and an empty result means
        # the market has not settled -- not that it settled to an outcome called
        # OPEN, which is what the page used to claim.
        close = detail.get("close_time")
        detail["live"] = bool(close and close > datetime.now(close.tzinfo))
        detail["outcome"] = (detail.get("result") or "").strip() or None
        if name == "poly_candles":
            detail["live"] = detail["live"] and not detail.get("is_closed")
        detail["closes_in"] = _human_delta(close - datetime.now(close.tzinfo))             if detail["live"] else None

    # Related wording and nearby markets fill themselves in from /related and
    # /nearby once the page is up, so the page never waits for them.
    near = related = None
    if ticker and name in ("kalshi_markets", "poly_markets", "kalshi_candles", "poly_candles"):
        related = {"threshold": 0.25, "rows": [], "loading": True}
        near = {"by": "time", "hours": 1, "points": 5, "same_category": True,
                "loading": True, "rows": []}

    # Offer a download when a single market is open and has nothing stored.
    fetchable = bool(ticker) and name.endswith("_candles") and not rows
    fetch_estimate = None
    if fetchable:
        # One request covers ~4000 candles, and a candle is a minute, so the
        # cost follows how long the market has been open.
        span = db.query(
            "SELECT EXTRACT(EPOCH FROM (LEAST(close_time, now()) - open_time))/60 AS mins"
            " FROM kalshi_markets WHERE ticker = %s" if name not in POLY else
            "SELECT EXTRACT(EPOCH FROM (LEAST(end_date, now())"
            " - COALESCE((raw->>'startDate')::timestamptz, end_date - interval '30 days')))/60"
            " AS mins FROM poly_markets WHERE condition_id = %s", (ticker,))
        mins = float(span[0]["mins"]) if span and span[0]["mins"] else 0
        if mins > 0:
            seconds = max(1, round(mins / 4000 * 0.8))
            fetch_estimate = (f"{seconds}s" if seconds < 90
                              else f"{seconds/60:.0f} min")
            fetch_estimate += f", {mins/1440:.0f} days of history" if mins > 2880 else ""
    job = None
    if ticker and request.args.get("job"):
        with JOBS_LOCK:
            job = JOBS.get(f"{name}:{ticker}")
        if job and job["state"] == "done" and rows:
            with JOBS_LOCK:
                JOBS.pop(f"{name}:{ticker}", None)
        if job and job["state"] == "running":
            fetchable = False

    # raw is the full API payload -- tens of fields per row. It is worth
    # keeping (36 of its keys exist nowhere else) but rendering 500 copies of
    # it makes a 2 MB page, so it is shown only for a single market.
    cols = list(rows[0].keys()) if rows else []
    raw_doc = None
    if "raw" in cols and ticker:
        raw_doc = rows[0].get("raw")
    if "raw" in cols:
        cols.remove("raw")

    return render(rows=rows, cols=cols, raw_doc=raw_doc,
                  panels=build_panels(name, asset, ticker, dim),
                  selection=ticker or asset, table_name=name,
                  candle_table=candle_table, detail=detail, near=near,
                  related=related,
                  msg=request.args.get("msg"), fetchable=fetchable, job=job,
                  fetch_estimate=fetch_estimate,
                  q=q, limit=cap, table_more=table_more, table_qs=table_qs,
                  dim=dim, asset_value=asset or "", ticker_value=ticker or "",
                  sql=f'SELECT * FROM "{name}" LIMIT 200')


VISUALISE_PAGE = """
<!doctype html>
{{ THEME|safe }}
<title>visualise</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 1.5rem; color: var(--ink); }
 header { display: flex; align-items: baseline; gap: 18px; }
 header h2 { margin: 0 0 10px; }
 header .links a { font-size: 13px; margin-right: 12px; }
 form.pick { margin-bottom: 12px; display: flex; gap: 8px; align-items: center;
             flex-wrap: wrap; }
 form.pick input[name=ticker] { width: 340px; padding: 5px 8px; font-size: 13px; }
 form.pick select, form.pick button { padding: 5px 8px; font-size: 13px; }
 .hint { font-size: 12px; color: var(--muted); margin-bottom: 14px; }
 .wrap { position: relative; border: 1px solid var(--line); border-radius: 4px;
         padding: 8px; background: var(--bg); }
 canvas { display: block; width: 100%; height: 520px; }
 #tip { position: absolute; pointer-events: none; display: none; background: var(--bg);
        border: 1px solid var(--line); border-radius: 4px; padding: 6px 9px; font-size: 12px;
        box-shadow: 0 1px 4px rgba(0,0,0,.15); white-space: nowrap; z-index: 2; }
 #tip b { display: block; margin-bottom: 3px; }
 #tip span { color: var(--muted); }
 .bar { font-size: 12px; color: var(--muted); margin-top: 8px; }
 .bar b { color: var(--ink); }
 .err { color: var(--bad); }
 .avail { margin-top: 26px; font-size: 13px; }
 .avail table { border-collapse: collapse; font-size: 13px; }
 .avail th, .avail td { border: 1px solid var(--line); padding: 4px 9px; text-align: left; }
 .avail th { background: var(--soft); }
 .venue { display: inline-block; width: 15px; height: 15px; line-height: 15px;
          text-align: center; border-radius: 3px; font-size: 10px; color: #fff;
          margin-right: 5px; }
 .v-k { background: var(--accent); } .v-p { background: #99542a; }
</style>

<header>
  <h2><a href="/" style="text-decoration:none;color:inherit">prediction_markets</a> &middot; visualise</h2>
  {{ NAV|safe }}
</header>

<form class="pick" method="get" action="/visualise">
  <input name="ticker" list="tickers" value="{{ ticker or '' }}"
         placeholder="ticker or condition id" autocomplete="off">
  <datalist id="tickers">
    {% for a in available %}<option value="{{ a.id }}">{{ a.title or '' }}</option>{% endfor %}
  </datalist>
  <select name="interval">
    {% for v, label in intervals %}
      <option value="{{ v }}" {{ 'selected' if v == interval else '' }}>{{ label }}</option>
    {% endfor %}
  </select>
  <button type="submit">plot</button>
</form>

{% if error %}<p class="err">{{ error }}</p>{% endif %}

{% if ticker and not error %}
  <div class="hint">{{ head }}</div>
  <div class="wrap">
    <canvas id="chart"></canvas>
    <div id="tip"></div>
  </div>
  <div class="bar" id="status">loading&hellip;</div>
  <script>
  (function () {
    var cv = document.getElementById('chart'), tip = document.getElementById('tip');
    var C = function (name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); };
    var status = document.getElementById('status'), ctx = cv.getContext('2d');
    var bars = [], lo = 0, hi = 1, vmax = 1, geom = null;
    var PAD = {l: 54, r: 14, t: 12, b: 26}, VOL = 70;

    function layout() {
      var dpr = window.devicePixelRatio || 1;
      var w = cv.clientWidth, h = cv.clientHeight;
      cv.width = w * dpr; cv.height = h * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      var priceH = h - PAD.t - PAD.b - VOL - 10;
      geom = {w: w, h: h, priceTop: PAD.t, priceH: priceH,
              volTop: PAD.t + priceH + 10, volH: VOL,
              left: PAD.l, plotW: w - PAD.l - PAD.r};
    }
    function y(v) { return geom.priceTop + (hi - v) / (hi - lo || 1) * geom.priceH; }
    function x(i) { return geom.left + (i + 0.5) * (geom.plotW / bars.length); }

    function draw() {
      layout();
      ctx.clearRect(0, 0, geom.w, geom.h);
      if (!bars.length) return;
      var bw = geom.plotW / bars.length, body = Math.max(1, Math.min(bw * 0.7, 14));

      ctx.strokeStyle = C('--line2'); ctx.fillStyle = C('--faint');
      ctx.font = '11px system-ui'; ctx.textAlign = 'right';
      for (var g = 0; g <= 4; g++) {
        var v = lo + (hi - lo) * g / 4, py = Math.round(y(v)) + 0.5;
        ctx.beginPath(); ctx.moveTo(geom.left, py);
        ctx.lineTo(geom.left + geom.plotW, py); ctx.stroke();
        ctx.fillText((v * 100).toFixed(1) + '%', geom.left - 6, py + 3);
      }

      for (var i = 0; i < bars.length; i++) {
        var b = bars[i], cx = x(i);
        // Most minutes here are a quoted mid with no trade, so open == close is
        // the common case -- draw those as a flat tick rather than a body.
        var up = b[4] > b[1], flat = b[4] === b[1];
        ctx.strokeStyle = ctx.fillStyle = flat ? C('--faint') : (up ? '#1a9c5b' : '#cc3d3d');
        ctx.beginPath();
        ctx.moveTo(Math.round(cx) + 0.5, y(b[2]));
        ctx.lineTo(Math.round(cx) + 0.5, y(b[3]));
        ctx.stroke();
        var top = y(Math.max(b[1], b[4])), bot = y(Math.min(b[1], b[4]));
        if (bot - top < 1) ctx.fillRect(cx - body / 2, top, body, 1);
        else ctx.fillRect(cx - body / 2, top, body, bot - top);
      }

      if (vmax > 0) {
        ctx.fillStyle = C('--accent-line');
        for (var j = 0; j < bars.length; j++) {
          var vh = bars[j][5] / vmax * geom.volH;
          if (vh > 0) ctx.fillRect(x(j) - body / 2, geom.volTop + geom.volH - vh, body, vh);
        }
        ctx.strokeStyle = C('--line2'); ctx.beginPath();
        ctx.moveTo(geom.left, geom.volTop + geom.volH + 0.5);
        ctx.lineTo(geom.left + geom.plotW, geom.volTop + geom.volH + 0.5); ctx.stroke();
        ctx.fillStyle = C('--faint'); ctx.textAlign = 'right';
        ctx.fillText(fmt(vmax), geom.left - 6, geom.volTop + 10);
      }

      // Labels come from the bars themselves: the series is irregular, so evenly
      // spaced ticks would imply a regular spacing that is not there.
      ctx.fillStyle = C('--faint'); ctx.textAlign = 'center';
      var step = Math.max(1, Math.floor(bars.length / 5));
      for (var k = 0; k < bars.length; k += step) {
        ctx.fillText(stamp(bars[k][0], true), x(k), geom.h - 8);
      }
    }

    function fmt(n) {
      return n >= 1e6 ? (n / 1e6).toFixed(1) + 'M'
           : n >= 1e3 ? (n / 1e3).toFixed(1) + 'k' : String(Math.round(n));
    }
    function stamp(ms, short) {
      var d = new Date(ms);
      var p = function (n) { return (n < 10 ? '0' : '') + n; };
      var day = d.getFullYear() + '-' + p(d.getMonth() + 1) + '-' + p(d.getDate());
      return short ? day : day + ' ' + p(d.getHours()) + ':' + p(d.getMinutes());
    }

    cv.addEventListener('mousemove', function (e) {
      if (!bars.length || !geom) return;
      var r = cv.getBoundingClientRect();
      var i = Math.floor((e.clientX - r.left - geom.left) / (geom.plotW / bars.length));
      if (i < 0 || i >= bars.length) { tip.style.display = 'none'; draw(); return; }
      var b = bars[i];
      draw();
      ctx.strokeStyle = C('--line'); ctx.setLineDash([3, 3]); ctx.beginPath();
      ctx.moveTo(Math.round(x(i)) + 0.5, geom.priceTop);
      ctx.lineTo(Math.round(x(i)) + 0.5, geom.priceTop + geom.priceH);
      ctx.stroke(); ctx.setLineDash([]);
      tip.innerHTML = '<b>' + stamp(b[0]) + '</b>' +
        'O <span>' + (b[1] * 100).toFixed(1) + '%</span>  ' +
        'H <span>' + (b[2] * 100).toFixed(1) + '%</span><br>' +
        'L <span>' + (b[3] * 100).toFixed(1) + '%</span>  ' +
        'C <span>' + (b[4] * 100).toFixed(1) + '%</span><br>' +
        'vol <span>' + fmt(b[5]) + '</span>' +
        (b[5] ? '' : ' <span>(quote only)</span>');
      tip.style.display = 'block';
      var tx = e.clientX - r.left + 14;
      if (tx + tip.offsetWidth > geom.w) tx = e.clientX - r.left - tip.offsetWidth - 14;
      tip.style.left = tx + 'px';
      tip.style.top = Math.max(0, e.clientY - r.top - 10) + 'px';
    });
    cv.addEventListener('mouseleave', function () { tip.style.display = 'none'; draw(); });
    window.addEventListener('resize', draw);
    window.addEventListener('themechange', draw);

    fetch('/visualise/data?ticker={{ ticker|urlencode }}&interval={{ interval|urlencode }}')
      .then(function (r) { return r.json(); })
      .then(function (d) {
        if (d.error) { status.innerHTML = '<span class="err">' + d.error + '</span>'; return; }
        bars = d.bars;
        if (!bars.length) { status.textContent = 'no candles stored for this market'; return; }
        lo = Math.min.apply(null, bars.map(function (b) { return b[3]; }));
        hi = Math.max.apply(null, bars.map(function (b) { return b[2]; }));
        var pad = (hi - lo) * 0.08 || 0.01;
        lo = Math.max(0, lo - pad); hi = Math.min(1, hi + pad);
        vmax = Math.max.apply(null, bars.map(function (b) { return b[5]; }));
        draw();
        status.innerHTML = '<b>' + d.bars.length.toLocaleString() + '</b> bars at <b>' +
          d.interval_label + '</b> from <b>' + d.source.toLocaleString() +
          '</b> stored candles &middot; <b>' + d.traded.toLocaleString() +
          '</b> bars had a trade &middot; ' + stamp(bars[0][0]) + ' &rarr; ' +
          stamp(bars[bars.length - 1][0]);
      })
      .catch(function () { status.innerHTML = '<span class="err">failed to load</span>'; });
  })();
  </script>
{% endif %}

<div class="avail">
  <b>markets with the most candles stored</b>
  <table>
    <tr><th>market</th><th>candles</th><th>from</th><th>to</th></tr>
    {% for a in available %}
      <tr>
        <td><a href="/visualise?ticker={{ a.id|urlencode }}&interval={{ interval|urlencode }}"><span
            class="venue {{ 'v-p' if a.venue == 'poly' else 'v-k' }}">{{ 'P' if a.venue == 'poly' else 'K' }}</span>{{ a.id }}</a></td>
        <td>{{ '{:,}'.format(a.n) }}</td>
        <td>{{ a.first }}</td>
        <td>{{ a.last }}</td>
      </tr>
    {% else %}
      <tr><td colspan="4">nothing downloaded yet &mdash; use
        <a href="/manage">manage</a> to fetch a market</td></tr>
    {% endfor %}
  </table>
</div>
"""


# Bucket widths offered on the chart. "auto" picks one so the whole life of the
# market lands in a readable number of bars instead of 97k one-minute ticks.
INTERVALS = [("auto", "auto"), ("1 minute", "1 min"), ("5 minutes", "5 min"),
             ("15 minutes", "15 min"), ("1 hour", "1 hour"), ("4 hours", "4 hours"),
             ("1 day", "1 day")]
STEP_MINUTES = {"1 minute": 1, "5 minutes": 5, "15 minutes": 15,
                "1 hour": 60, "4 hours": 240, "1 day": 1440}
TARGET_BARS = 400


def candle_source(ident):
    """Which candle table holds this id, if any."""
    if db.query("SELECT 1 FROM kalshi_candles WHERE ticker = %s LIMIT 1", (ident,)):
        return "kalshi_candles", "ticker", "kalshi"
    if db.query("SELECT 1 FROM poly_candles WHERE condition_id = %s LIMIT 1", (ident,)):
        return "poly_candles", "condition_id", "poly"
    return None, None, None


COUNT_VIEWS = ("kalshi_candle_counts", "poly_candle_counts")


def refresh_counts(cur):
    """Both candle-count views brought up to date (filled the first time)."""
    for view in COUNT_VIEWS:
        cur.execute("SELECT ispopulated FROM pg_matviews WHERE matviewname = %s", (view,))
        populated = cur.fetchone()
        if populated is not None:
            cur.execute("REFRESH MATERIALIZED VIEW"
                        + (" CONCURRENTLY" if populated[0] else "") + f" {view}")


def counts_ready():
    """Fill the candle-count views if the viewer's background refresh has not
    yet (only right after it starts)."""
    if _cache.get(("counts ready",)):
        return
    for view in COUNT_VIEWS:
        ready = db.query("SELECT ispopulated FROM pg_matviews WHERE matviewname = %s", (view,))
        if ready and not ready[0]["ispopulated"]:
            db.execute(f"REFRESH MATERIALIZED VIEW {view}")
    _cache[("counts ready",)] = True


def _plottable():
    counts_ready()
    return db.query("""
        (SELECT c.ticker AS id, 'kalshi' AS venue, c.n,
                c.first_ts::date::text AS first, c.last_ts::date::text AS last, m.title
         FROM kalshi_candle_counts c LEFT JOIN kalshi_markets m ON m.ticker = c.ticker
         ORDER BY c.n DESC LIMIT 2000)
        UNION ALL
        (SELECT c.condition_id, 'poly', c.n, c.first_ts::date::text, c.last_ts::date::text,
                p.question
         FROM poly_candle_counts c LEFT JOIN poly_markets p ON p.condition_id = c.condition_id
         ORDER BY c.n DESC LIMIT 2000)
        ORDER BY n DESC
        LIMIT 2000""")      # the busiest; any other ticker can be typed in


def plottable():
    """The markets with the most candles, for the picker and the list."""
    return _cached(("plottable",), 900, _plottable)


@app.route("/visualise")
def visualise():
    ticker = (request.args.get("ticker") or "").strip()
    interval = request.args.get("interval", "auto")
    if interval not in STEP_MINUTES and interval != "auto":
        interval = "auto"
    error = head = None
    if ticker:
        table, _, venue = candle_source(ticker)
        if not table:
            error = (f"no candles stored for {ticker} \u2014 download them from the "
                     "market page first")
        else:
            label = {"kalshi": "Kalshi", "poly": "Polymarket"}[venue]
            found = db.query(
                "SELECT title FROM kalshi_markets WHERE ticker = %s" if venue == "kalshi"
                else "SELECT question AS title FROM poly_markets WHERE condition_id = %s",
                (ticker,))
            head = f"{label} \u00b7 {found[0]['title']}" if found else label
    return render_template_string(VISUALISE_PAGE, ticker=ticker, interval=interval,
                                  intervals=INTERVALS, error=error, head=head,
                                  available=plottable())


@app.route("/visualise/data")
def visualise_data():
    """OHLC buckets for the chart.

    Aggregated in the database rather than in the browser: a two-year market is
    ~100k one-minute rows, which is both a slow response and an unreadable chart.
    """
    ident = (request.args.get("ticker") or "").strip()
    table, key, _ = candle_source(ident)
    if not table:
        return jsonify({"error": "no candles stored for this market", "bars": []})

    interval = request.args.get("interval", "auto")
    span = db.query(f"SELECT count(*) n, min(ts) f, max(ts) l FROM {table}"
                    f" WHERE {key} = %s", (ident,))[0]
    if interval not in STEP_MINUTES:
        minutes = max((span["l"] - span["f"]).total_seconds() / 60, 1)
        interval = next((step for step in STEP_MINUTES
                         if minutes / STEP_MINUTES[step] <= TARGET_BARS), "1 day")

    rows = db.query(f"""
        SELECT date_bin(%s::interval, ts, TIMESTAMPTZ '2001-01-01') AS bucket,
               (array_agg(open ORDER BY ts))[1] AS open,
               max(high) AS high, min(low) AS low,
               (array_agg(close ORDER BY ts DESC))[1] AS close,
               coalesce(sum(volume), 0) AS volume
        FROM {table} WHERE {key} = %s
        GROUP BY bucket ORDER BY bucket""", (interval, ident))

    bars = [[int(r["bucket"].timestamp() * 1000), float(r["open"]), float(r["high"]),
             float(r["low"]), float(r["close"]), float(r["volume"])] for r in rows]
    return jsonify({"bars": bars, "interval": interval,
                    "interval_label": dict(INTERVALS).get(interval, interval),
                    "source": span["n"],
                    "traded": sum(1 for b in bars if b[5] > 0)})


@app.route("/query")
def query():
    sql = request.args.get("sql", "")
    # Keep the pickers on screen for whichever table the query came from.
    name = request.args.get("table") or None
    panels = (build_panels(name, request.args.get("asset"), None,
                           request.args.get("dim", "")) if name else [])

    try:
        rows = run(sql)
        return render(rows=rows, cols=list(rows[0].keys()) if rows else [], sql=sql,
                      panels=panels, table_name=name)
    except Exception as exc:
        return render(sql=sql, error=str(exc), panels=panels, table_name=name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--host", default="127.0.0.1",
                        help="use 0.0.0.0 to allow other machines on your LAN")
    args = parser.parse_args()
    # Downloads started from the page share the Kalshi budget below live's hot
    # markets and above backfill.
    ratelimit.DEFAULT = "warm"
    # The minute-by-minute record behind /api-usage's charts (monitor.py).
    threading.Thread(target=monitor.run, args=(threading.Event(),), daemon=True,
                     name="monitor").start()
    threading.Thread(target=keep_fresh, args=(threading.Event(),), daemon=True,
                     name="refresh").start()

    print(f"http://127.0.0.1:{args.port}")
    app.run(host=args.host, port=args.port, threaded=True)

"""Browse the database in a web browser.

    python viewer.py          ->  http://127.0.0.1:8080

Binds to localhost only. Read-only: anything that isn't a SELECT is refused.
"""

import argparse
import re
import sys
import bisect
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

from urllib.parse import quote, urlencode

import requests
from flask import (Flask, Response, jsonify, redirect, render_template_string,
                   request)
from psycopg2.extras import RealDictCursor, execute_values

import db
import paths
from db import connection
import fetch as candle_fetch
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


def key_col(name):
    # The grouping tables key on their own column, not a market id.
    grouping = {"kalshi_series": "series_ticker", "kalshi_events": "event_ticker",
                "poly_events": "event_id"}
    return grouping.get(name, "condition_id" if name in POLY else "ticker")


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
    if name == "kalshi_perp_candles":
        return "kalshi_perp_markets m", f"m.{dim}", "m.ticker"
    return f'"{name}" m', f"m.{dim}", f"m.{key_col(name)}"

# Nav order: general -> specific, one group per instrument type. Anything not
# listed here is appended in its own group.
TABLE_GROUPS = [
    ["kalshi_series", "kalshi_events", "kalshi_markets", "kalshi_candles"],
    ["kalshi_perp_markets", "kalshi_perp_candles"],
    ["poly_events", "poly_markets", "poly_candles"],
]

PAGE = """
<!doctype html>
<title>prediction_markets</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 1.5rem; }
 table { border-collapse: collapse; font-size: 13px; }
 th, td { border: 1px solid #ccc; padding: 4px 8px; text-align: left; }
 th { background: #eee; position: sticky; top: 0; }
 td { max-width: 320px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
 nav a { margin-right: 4px; }
 nav small { margin-right: 12px; color: #666; }
 nav a.on { font-weight: 700; }
 nav .sep { margin: 0 14px 0 2px; color: #bbb; }
 textarea { width: 100%; height: 60px; font-family: monospace; }
 .err { color: #b00; }
 .cols { display: flex; gap: 18px; align-items: flex-start; }
 .cols.no-filters .side { display: none; }
 .cols.no-nearby .nearby { display: none; }
 header .toggles { margin-left: auto; }
 header .toggles button { font-size: 12px; padding: 3px 9px; margin-left: 6px;
   border: 1px solid #ccc; background: #f6f6f6; border-radius: 3px; cursor: pointer; }
 header .toggles button.off { opacity: .45; }
 .side { width: 250px; flex: none; border-right: 1px solid #ddd; padding-right: 12px;
         max-height: 78vh; overflow-y: auto; }
 .side b { display: block; font-size: 12px; text-transform: uppercase;
           color: #555; margin-bottom: 4px; }
 .side small { color: #888; }
 .detail { background: #f6f6f6; border-left: 3px solid #0645ad; padding: 8px 12px;
           margin-bottom: 10px; }
 header { display: flex; align-items: baseline; gap: 18px; }
 header h2 { margin: 0 0 6px; }
 header .links a { font-size: 13px; margin-right: 12px; }
 .okc { color: #262; }
 .sentinel { padding: 10px 4px; font-size: 12px; color: #555; cursor: pointer;
             text-align: center; border: 1px solid #ddd; border-radius: 4px;
             margin-top: 8px; background: #fafafa; }
 .msg { background: #eef7ee; border-left: 3px solid #4a4; padding: 6px 10px;
        margin-bottom: 8px; font-size: 13px; }
 .msg.err { background: #fdeeee; border-left-color: #c44; }
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
 .raw summary { cursor: pointer; font-size: 12px; color: #555; }
 .raw table { margin-top: 6px; font-size: 12px; }
 .raw th { text-align: left; white-space: nowrap; vertical-align: top; }
 .raw td { max-width: 620px; white-space: normal; }
 .btn { display: inline-block; background: #0645ad; color: #fff; padding: 5px 11px;
        border-radius: 3px; font-size: 12px; text-decoration: none; }
 .btn:hover { background: #053a80; }
 .idx { color: #999; font-size: 11px; text-align: right; width: 1%;
        white-space: nowrap; }
 th.idx { text-align: right; }
 .main td a { text-decoration: none; }
 .side a { display: block; padding: 2px 4px; font-size: 12px; text-decoration: none;
           white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
 .side a.on { background: #0645ad; color: #fff; border-radius: 3px; }
 .side input { width: 100%; margin-bottom: 6px; }
 .dims { margin-bottom: 8px; padding-bottom: 6px; border-bottom: 1px solid #eee; }
 .dims a { display: inline-block; font-size: 11px; margin: 0 6px 2px 0;
           padding: 1px 5px; border-radius: 3px; background: #f0f0f0;
           color: #333; }
 .dims a.on { background: #0645ad; color: #fff; }
 .main { flex: 1; overflow-x: auto; min-width: 0; }
 .related { width: 290px; flex: none; border-left: 1px solid #ddd; padding-left: 12px;
            max-height: 78vh; overflow-y: auto; }
 .related b { display: block; font-size: 12px; text-transform: uppercase; color: #555; }
 .related a { display: block; font-size: 12px; text-decoration: none; margin-top: 6px; }
 .related .meta { font-size: 11px; color: #777; }
 .sliderow { display: flex; align-items: center; gap: 8px; margin: 6px 0 10px; }
 .sliderow input { flex: 1; }
 .sliderow span { font-size: 11px; color: #555; width: 34px; text-align: right; }
 .dot { display: inline-block; width: 6px; height: 6px; border-radius: 50%; }
 .dot.on { background: #4a4; }
 .cols.no-related .related { display: none; }
 .nearby { width: 300px; flex: none; border-left: 1px solid #ddd; padding-left: 12px;
           max-height: 78vh; overflow-y: auto; }
 .nearby b { display: block; font-size: 12px; text-transform: uppercase;
             color: #555; }
 .nearby .anchor { font-size: 11px; color: #888; margin-bottom: 8px; }
 .nearby a { display: block; font-size: 12px; text-decoration: none;
             margin-top: 6px; }
 .nearby .meta { font-size: 11px; color: #777; }
 .byrow { margin: 6px 0 10px; }
 a.nctl { cursor: pointer; }
 .byrow a { display: inline-block; font-size: 11px; margin: 0 4px 3px 0;
            padding: 2px 6px; border-radius: 3px; background: #f0f0f0; color: #333;
            text-decoration: none; }
 .byrow a.on { background: #0645ad; color: #fff; }
 .widen { margin-top: 12px; border-top: 1px solid #eee; padding-top: 6px; }
 .widen a { display: inline-block; margin-right: 8px; font-size: 11px; }
 .widen a.on { font-weight: 700; text-decoration: none; }
 .venue { display: inline-block; width: 13px; height: 13px; line-height: 13px;
          text-align: center; border-radius: 2px; font-size: 9px; font-weight: 700;
          color: #fff; margin-right: 5px; vertical-align: 1px; }
 .v-k { background: #2a6099; }
 .v-p { background: #99542a; }
 /* On a tablet or phone the columns stack, and a nested scrolling panel is
    unusable, so the panels grow with the page instead. */
 @media (max-width: 900px) {
   .cols { flex-direction: column; }
   .side, .nearby { width: auto; max-width: none; max-height: none;
                    overflow-y: visible; border: 0; padding: 0; }
   .nearby { border-top: 1px solid #ddd; padding-top: 10px; }
 }
</style>

<header>
  <h2><a href="/" style="text-decoration:none;color:inherit">prediction_markets</a></h2>
  <span class="links">
    <a href="/visualise">visualise</a>
    <a href="/stats">stats</a>
    <a href="/manage">manage</a>
  </span>
  <span class="toggles">
    <button type="button" onclick="panel('filters')">filters</button>
    <button type="button" onclick="panel('nearby')">nearby</button>
    <button type="button" onclick="panel('related')">related</button>
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


def table_list(ttl=10):
    """Table names with exact row counts, cached briefly.

    A real count(*) per table rather than pg_stat's n_live_tup estimate, which
    lags badly while a load is running -- but counting eight tables costs a
    quarter second, so the result is held for `ttl` seconds.
    """
    if _COUNTS["rows"] is not None and time.monotonic() - _COUNTS["at"] < ttl:
        return _COUNTS["rows"]
    names = [r["name"] for r in db.query("""
        SELECT c.relname AS name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind = 'r' AND c.relname <> 'api_keys'
        ORDER BY c.relname
    """)]
    rows = [(name, db.query(f'SELECT count(*) AS n FROM "{name}"')[0]["n"])
            for name in names]
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
<title>stats</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 1050px;
        padding: 0 1rem; color: #222; line-height: 1.5; }
 header { display: flex; align-items: baseline; gap: 18px; }
 header h2 { margin: 0 0 6px; } header h2 a { text-decoration: none; color: inherit; }
 header .links a { font-size: 13px; margin-right: 12px; }
 .two { display: grid; grid-template-columns: 1fr 1fr; gap: 18px; margin-top: 18px; }
 .col { border: 1px solid #ddd; border-radius: 6px; padding: 14px 16px; }
 .col.k { border-top: 3px solid #2a6099; } .col.p { border-top: 3px solid #99542a; }
 .col h3 { margin: 0 0 2px; font-size: 16px; }
 .col h3 a { font-size: 12px; font-weight: normal; margin-left: 8px; }
 .col .sub { color: #666; font-size: 12px; margin-bottom: 12px; }
 .big { font-size: 22px; font-weight: 700; }
 dl { display: grid; grid-template-columns: auto 1fr; gap: 3px 12px; margin: 0 0 14px; }
 dt { color: #666; font-size: 13px; }
 dd { margin: 0; text-align: right; font-variant-numeric: tabular-nums; }
 table { border-collapse: collapse; width: 100%; font-size: 13px; }
 th, td { border-bottom: 1px solid #eee; padding: 3px 4px; text-align: right; }
 th:first-child, td:first-child { text-align: left; }
 th { color: #666; font-weight: normal; font-size: 12px; }
 .bar { background: #dde6f2; display: inline-block; height: 8px; border-radius: 2px; }
 .bar.p { background: #f2e3dd; }
 .note { background: #f7f7f7; border-left: 3px solid #bbb; padding: 10px 14px;
         font-size: 13px; margin: 20px 0 0; }
 h4 { margin: 16px 0 4px; font-size: 13px; color: #555; }
 @media (max-width: 800px) { .two { grid-template-columns: 1fr; } }
</style>

<header>
  <h2><a href="/">prediction_markets</a></h2>
  <span class="links">
    <a href="/table/kalshi_markets?dim=category">tables</a>
  </span>
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
<title>manage</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 1.5rem; color: #222; }
 header { display: flex; align-items: baseline; gap: 18px; }
 header h2 { margin: 0 0 6px; } header h2 a { text-decoration: none; color: inherit; }
 header .links a { font-size: 13px; margin-right: 12px; }
 .cards { display: flex; flex-wrap: wrap; gap: 12px; margin: 14px 0 18px; }
 .card { border: 1px solid #ddd; border-radius: 6px; padding: 10px 14px; min-width: 150px; }
 .card .n { font-size: 20px; font-weight: 700; }
 .card .l { font-size: 11px; color: #666; text-transform: uppercase; letter-spacing: .03em; }
 .card.k { border-top: 3px solid #2a6099; } .card.p { border-top: 3px solid #99542a; }
 .card.on { border-top: 3px solid #4a7; }
 table { border-collapse: collapse; width: 100%; font-size: 13px; }
 th, td { border-bottom: 1px solid #eee; padding: 4px 7px; text-align: left; }
 th { background: #f4f4f4; position: sticky; top: 0; font-size: 12px; }
 td.n, th.n { text-align: right; font-variant-numeric: tabular-nums; }
 .controls { display: flex; flex-wrap: wrap; gap: 10px 14px; align-items: flex-end;
             margin-bottom: 14px; padding-bottom: 12px; border-bottom: 1px solid #eee; }
 .controls label { display: flex; flex-direction: column; font-size: 11px;
                   text-transform: uppercase; letter-spacing: .03em; color: #666; }
 .controls select, .controls input { margin-top: 3px; padding: 4px 6px; font-size: 13px;
                                     text-transform: none; }
 .controls button { padding: 6px 12px; font-size: 13px; }
 .pill { display: inline-block; padding: 1px 6px; border-radius: 8px; font-size: 11px; }
 .live { background: #e6f6e6; color: #262; }
 .done { background: #eee; color: #555; }
 .has { color: #262; font-weight: 700; }
 .idx { color: #999; font-size: 11px; }
 .pill.got { background: #e6f6e6; color: #262; font-weight: 700; }
 .pill.none { background: #f2f2f2; color: #888; }
 tr.empty td { background: #fcfcfc; color: #777; }
 tr.empty td a { color: #6a7fa8; }
 .no { color: #999; }
 .refresh { background: #f6f9ff; border: 1px solid #ccd8ee; border-radius: 6px;
            padding: 12px 14px; margin-top: 22px; }
 .refresh h3 { margin: 0 0 6px; font-size: 14px; color: #2a6099; }
 .refresh button { background: #2a6099; color: #fff; border: 0; padding: 6px 12px;
                   border-radius: 3px; cursor: pointer; font-size: 13px;
                   margin-right: 6px; }
 .refresh button:hover { background: #1d4570; }
 .msg.err { background: #fff2f2; border-left-color: #c33; }
 .danger { background: #fff6f6; border: 1px solid #f0c0c0; border-radius: 6px;
           padding: 12px 14px; margin-top: 22px; }
 .danger h3 { margin: 0 0 6px; font-size: 14px; color: #a33; }
 .danger button { background: #c33; color: #fff; border: 0; padding: 6px 12px;
                  border-radius: 3px; cursor: pointer; font-size: 13px; }
 .msg { background: #eef7ee; border-left: 3px solid #4a4; padding: 6px 10px;
        margin-bottom: 10px; font-size: 13px; }
 .venue { display: inline-block; width: 13px; height: 13px; line-height: 13px;
          text-align: center; border-radius: 2px; font-size: 9px; font-weight: 700;
          color: #fff; margin-right: 5px; }
 .v-k { background: #2a6099; } .v-p { background: #99542a; }
</style>

<header>
  <h2><a href="/">prediction_markets</a></h2>
  <span class="links">
    <a href="/table/kalshi_markets?dim=category">tables</a>
    <a href="/stats">stats</a>
  </span>
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
    <button name="which" value="perps" type="submit">Perpetuals</button>
  </form>
  <p style="font-size:12px;color:#666;margin:8px 0 0">
    Re-runs the scraper for the last 7 days plus anything still open, then loads it.
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
  <p style="font-size:12px;color:#666;margin:8px 0 0">
    Candles can always be re-downloaded on demand; market rows come back by re-running
    the scrapers. Nothing here touches the files in data/.
  </p>
</div>
"""


HOME_PAGE = """
<!doctype html>
<title>prediction_markets</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 1000px;
        padding: 0 1rem; color: #222; line-height: 1.5; }
 h1 { margin: 0 0 4px; font-size: 22px; }
 .sub { color: #666; margin-bottom: 22px; font-size: 14px; }
 .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
         gap: 14px; margin-bottom: 30px; }
 .card { border: 1px solid #ddd; border-radius: 6px; padding: 14px 16px;
         text-decoration: none; color: inherit; display: block; }
 .card:hover { border-color: #0645ad; background: #f8faff; }
 .card h3 { margin: 0 0 4px; font-size: 15px; }
 .card p { margin: 0 0 8px; font-size: 13px; color: #555; }
 .card .rows { font-size: 12px; color: #777; }
 .card .rows b { color: #222; }
 .k { border-left: 3px solid #2a6099; }
 .p { border-left: 3px solid #99542a; }
 h2 { font-size: 16px; margin: 26px 0 8px; }
 table.cmp { border-collapse: collapse; font-size: 13px; width: 100%; }
 table.cmp th, table.cmp td { border: 1px solid #ddd; padding: 6px 9px;
                              text-align: left; vertical-align: top; }
 table.cmp th { background: #f4f4f4; }
 .note { background: #f7f7f7; border-left: 3px solid #bbb; padding: 10px 14px;
         font-size: 13px; margin: 14px 0; }
 code { background: #f0f0f0; padding: 1px 4px; border-radius: 3px; font-size: 12px; }
</style>

<h1>prediction_markets</h1>
<div class="sub">Kalshi and Polymarket, same database, side by side.</div>

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
  <a class="card k" href="/table/kalshi_perp_markets?dim=status">
    <h3>Kalshi perpetuals</h3>
    <p>Leveraged up/down contracts. No expiry, funding every 8 hours.</p>
    <div class="rows"><b>{{ '{:,}'.format(counts.kalshi_perp_markets) }}</b> instruments &middot;
      <b>{{ '{:,}'.format(counts.kalshi_perp_candles) }}</b> candles</div>
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
  <a class="card" href="/api-usage">
    <h3>API usage</h3>
    <p>Who is calling the API, what they ask for, errors, backfill jobs, and
       whether collection is keeping up.</p>
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
      <td>Real OHLCV, with volume and open interest per bar.</td>
      <td>One forward-filled price point per minute. Open, high, low and close
          are the same number and volume is always empty.</td></tr>
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


def _manage_rows(venue, state, category, sort, q, limit, offset=0):
    """One row per market, with its candle coverage."""
    order = {"candles": "candles DESC NULLS LAST", "volume": "volume DESC NULLS LAST",
             "close": "closes DESC NULLS LAST", "open": "opened DESC NULLS LAST"}[sort]
    like = f"%{q}%" if q else None

    parts, args = [], []
    if venue in ("both", "kalshi"):
        cond, a = ["true"], []
        if category:
            cond.append("e.category = %s"); a.append(category)
        if q:
            cond.append("(m.ticker ILIKE %s OR m.title ILIKE %s)"); a += [like, like]
        if state == "active":
            cond.append("m.close_time > now()")
        elif state == "expired":
            cond.append("m.close_time <= now()")
        parts.append(("""
            SELECT 'kalshi' AS venue, m.ticker AS id,
                   COALESCE(m.raw->>'yes_sub_title', m.title) AS title, e.category,
                   m.close_time > now() AS active,
                   to_char(m.open_time, 'YYYY-MM-DD HH24:MI') AS opened,
                   to_char(m.close_time, 'YYYY-MM-DD HH24:MI') AS closes,
                   m.volume,
                   c.n AS candles, c.lo AS first, c.hi AS last
            FROM kalshi_markets m
            JOIN kalshi_events e USING (event_ticker)
            LEFT JOIN (SELECT ticker, count(*) n,
                              to_char(min(ts),'YYYY-MM-DD HH24:MI') lo,
                              to_char(max(ts),'YYYY-MM-DD HH24:MI') hi
                       FROM kalshi_candles GROUP BY ticker) c ON c.ticker = m.ticker
            WHERE """ + " AND ".join(cond), a))

    if venue in ("both", "poly"):
        cond, a = ["true"], []
        if category:
            cond.append("e.category = %s"); a.append(category)
        if q:
            cond.append("(p.condition_id ILIKE %s OR p.question ILIKE %s)"); a += [like, like]
        if state == "active":
            cond.append("p.end_date > now()")
        elif state == "expired":
            cond.append("p.end_date <= now()")
        parts.append(("""
            SELECT 'poly' AS venue, p.condition_id AS id, p.question AS title, e.category,
                   p.end_date > now() AS active,
                   NULL AS opened,
                   to_char(p.end_date, 'YYYY-MM-DD HH24:MI') AS closes,
                   p.volume,
                   c.n AS candles, c.lo AS first, c.hi AS last
            FROM poly_markets p
            LEFT JOIN poly_events e USING (event_id)
            LEFT JOIN (SELECT condition_id, count(*) n,
                              to_char(min(ts),'YYYY-MM-DD HH24:MI') lo,
                              to_char(max(ts),'YYYY-MM-DD HH24:MI') hi
                       FROM poly_candles GROUP BY condition_id) c
              ON c.condition_id = p.condition_id
            WHERE """ + " AND ".join(cond), a))

    sql = " UNION ALL ".join(f"({s})" for s, _ in parts)
    for _, a in parts:
        args += a
    having = ""
    if state == "withdata":
        having = " WHERE candles IS NOT NULL"
    elif state == "nodata":
        having = " WHERE candles IS NULL"
    # A stable tiebreak keeps paging from repeating or skipping rows when the
    # sort column has ties (it usually does -- volume 0, no candles).
    order += ", id"
    if limit is None:
        return db.query(f"SELECT * FROM ({sql}) x{having} ORDER BY {order}", tuple(args))
    args += [limit, offset]
    return db.query(f"SELECT * FROM ({sql}) x{having} ORDER BY {order}"
                    " LIMIT %s OFFSET %s", tuple(args))


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

    s = db.query("""
        SELECT (SELECT count(*) FROM kalshi_markets) k_markets,
               (SELECT count(DISTINCT ticker) FROM kalshi_candles) k_with,
               (SELECT count(*) FROM poly_markets) p_markets,
               (SELECT count(DISTINCT condition_id) FROM poly_candles) p_with,
               (SELECT count(*) FROM kalshi_markets WHERE close_time > now())
             + (SELECT count(*) FROM poly_markets WHERE end_date > now()) active,
               (SELECT count(*) FROM kalshi_markets WHERE close_time <= now())
             + (SELECT count(*) FROM poly_markets WHERE end_date <= now()) expired,
               pg_size_pretty(pg_database_size(current_database())) size""")[0]

    categories = [r["c"] for r in db.query("""
        SELECT DISTINCT category AS c FROM kalshi_events WHERE category IS NOT NULL
        UNION SELECT DISTINCT category FROM poly_events WHERE category IS NOT NULL
        ORDER BY c""")]

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
                "poly": "polymarket markets",
                "perps": "perpetuals"}


def _refresh_steps(which):
    import catalogue
    import subprocess

    def script(*argv):
        done = subprocess.run([sys.executable, *argv], cwd=str(paths.ROOT),
                              capture_output=True, text=True, timeout=7200)
        if done.returncode != 0:
            tail = (done.stderr or done.stdout or "").strip().splitlines()[-1:]
            raise RuntimeError(f"{argv[0]}: {tail[0] if tail else 'failed'}")

    return {"kalshi_crypto": [lambda: catalogue.sync_kalshi(categories=["Crypto"])],
            "kalshi_all": [catalogue.sync_series, catalogue.sync_kalshi],
            "poly": [catalogue.sync_poly],
            "perps": [lambda: script("kalshi_perps.py", "--days", "7"),
                      lambda: script("load_db.py")]}[which]


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


USAGE_PAGE = """
<!doctype html>
<title>API usage</title>
{% if refresh %}<meta http-equiv="refresh" content="10">{% endif %}
<style>
 body { font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 1150px;
        padding: 0 1rem; color: #222; line-height: 1.5; }
 header { display: flex; align-items: baseline; gap: 18px; flex-wrap: wrap; }
 header h2 { margin: 0 0 6px; } header h2 a { text-decoration: none; color: inherit; }
 header .links a { font-size: 13px; margin-right: 12px; }
 .bar-nav { font-size: 13px; margin: 4px 0 14px; color: #666; }
 .bar-nav a, .bar-nav b { margin-right: 10px; }
 .tiles { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 10px; }
 .tile { border: 1px solid #ddd; border-radius: 6px; padding: 10px 14px; }
 .tile b { display: block; font-size: 22px; font-variant-numeric: tabular-nums; }
 .tile span { color: #666; font-size: 12px; }
 .tile.bad b { color: #b3261e; }
 .col { border: 1px solid #ddd; border-radius: 6px; padding: 12px 16px; margin-top: 16px; }
 .col h3 { margin: 0 0 8px; font-size: 15px; }
 .col h3 small { color: #888; font-weight: normal; font-size: 12px; margin-left: 6px; }
 .two { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
 .two .col { margin-top: 16px; min-width: 0; }
 .scroll { overflow-x: auto; }
 table { border-collapse: collapse; width: 100%; font-size: 13px; }
 th, td { border-bottom: 1px solid #eee; padding: 3px 6px; text-align: right; white-space: nowrap; }
 th:first-child, td:first-child, td.l, th.l { text-align: left; }
 th { color: #666; font-weight: normal; font-size: 12px; }
 td.wrap { white-space: normal; word-break: break-all; text-align: left; }
 .s2 { color: #1e7b34; } .s4 { color: #b06000; } .s5 { color: #b3261e; font-weight: 600; }
 .muted { color: #888; }
 .k { border-top: 3px solid #2a6099; }
 dl { display: grid; grid-template-columns: auto 1fr; gap: 3px 12px; margin: 0; font-size: 13px; }
 dt { color: #666; } dd { margin: 0; font-variant-numeric: tabular-nums; }
 svg { display: block; width: 100%; height: 130px; }
 .note { background: #f7f7f7; border-left: 3px solid #bbb; padding: 10px 14px; font-size: 13px; margin: 16px 0 0; }
 @media (max-width: 800px) { .two { grid-template-columns: 1fr; } }
</style>

<header>
  <h2><a href="/">prediction_markets</a> &middot; API usage</h2>
  <span class="links">
    <a href="/stats">stats</a>
    <a href="/table/kalshi_markets?dim=category">tables</a>
  </span>
</header>
<div class="bar-nav">
  window:
  {% for w in windows %}{% if w == window %}<b>{{ w }}</b>{% else %}<a href="?window={{ w }}{% if not refresh %}&refresh=off{% endif %}">{{ w }}</a>{% endif %}{% endfor %}
  &middot;
  {% if refresh %}refreshing every 10 s &middot; <a href="?window={{ window }}&refresh=off">pause</a>
  {% else %}paused &middot; <a href="?window={{ window }}">resume</a>{% endif %}
  &middot; <span class="muted">as of {{ now }}</span>
</div>

{% if missing %}
<div class="note">Nothing recorded yet: the API starts writing to <code>api_requests</code>
the first time it runs with this version.</div>
{% else %}

<div class="tiles">
  <div class="tile"><b>{{ '{:,}'.format(t.n) }}</b><span>requests</span></div>
  <div class="tile{% if t.errors %} bad{% endif %}"><b>{{ '{:,}'.format(t.errors) }}</b>
    <span>errors{% if t.n %} &middot; {{ '%.1f'|format(100 * t.errors / t.n) }}%{% endif %}</span></div>
  <div class="tile"><b>{{ '{:,}'.format(t.limited) }}</b><span>rate-limited (429)</span></div>
  <div class="tile"><b>{{ ms(t.p50) }}</b><span>typical response</span></div>
  <div class="tile"><b>{{ ms(t.p95) }}</b><span>slowest 5% above</span></div>
  <div class="tile"><b>{{ size(t.bytes) }}</b><span>sent &middot; {{ '{:,}'.format(t.rows_sent) }} rows</span></div>
  <div class="tile"><b>{{ '{:,}'.format(t.backfills) }}</b><span>backfills pushed</span></div>
</div>

<div class="col">
  <h3>Requests over time <small>{{ bucket_label }} per bar &middot; red = errors &middot; busiest bar {{ '{:,}'.format(peak) }}</small></h3>
  <svg viewBox="0 0 1000 130" preserveAspectRatio="none" role="img" aria-label="requests per {{ bucket_label }}">
    <line x1="0" y1="120" x2="1000" y2="120" stroke="#ddd"/>
    {% for b in bars %}
    <rect x="{{ b.x }}" y="{{ 120 - b.h }}" width="{{ b.w }}" height="{{ b.h }}" fill="#9db6d6"><title>{{ b.label }}: {{ b.n }} requests, {{ b.e }} errors</title></rect>
    {% if b.eh %}<rect x="{{ b.x }}" y="{{ 120 - b.eh }}" width="{{ b.w }}" height="{{ b.eh }}" fill="#d9534f"/>{% endif %}
    {% endfor %}
  </svg>
  <div class="muted" style="font-size:12px;display:flex;justify-content:space-between"><span>{{ first_label }}</span><span>now</span></div>
</div>

<div class="two">
  <div class="col">
    <h3>Who is calling</h3>
    <div class="scroll"><table>
      <tr><th>key</th><th class="l">machine / client</th><th class="l">ip</th><th>requests</th><th>errors</th><th>last</th></tr>
      {% for r in callers %}
      <tr><td>{{ r.name }}</td><td class="l">{{ r.user_agent or '' }}</td><td class="l">{{ r.client_ip or '' }}</td>
          <td>{{ '{:,}'.format(r.n) }}</td><td>{{ r.e }}</td><td>{{ when(r.last) }}</td></tr>
      {% else %}<tr><td colspan="6" class="muted">no requests in this window</td></tr>{% endfor %}
    </table></div>
  </div>
  <div class="col">
    <h3>Most asked-for markets</h3>
    <div class="scroll"><table>
      <tr><th>ticker</th><th>requests</th><th>last</th></tr>
      {% for r in tickers %}
      <tr><td><a href="/table/kalshi_candles?ticker={{ r.t|urlencode }}">{{ r.t }}</a></td>
          <td>{{ '{:,}'.format(r.n) }}</td><td>{{ when(r.last) }}</td></tr>
      {% else %}<tr><td colspan="3" class="muted">none in this window</td></tr>{% endfor %}
    </table></div>
  </div>
</div>

<div class="col">
  <h3>Endpoints</h3>
  <div class="scroll"><table>
    <tr><th>endpoint</th><th>requests</th><th>errors</th><th>typical</th><th>slowest 5%</th><th>rows</th><th>sent</th></tr>
    {% for r in endpoints %}
    <tr><td>{{ r.method }} {{ r.ep }}</td><td>{{ '{:,}'.format(r.n) }}</td><td>{{ r.e }}</td>
        <td>{{ ms(r.p50) }}</td><td>{{ ms(r.p95) }}</td><td>{{ '{:,}'.format(r.rows_sent) }}</td><td>{{ size(r.bytes) }}</td></tr>
    {% else %}<tr><td colspan="7" class="muted">none in this window</td></tr>{% endfor %}
  </table></div>
</div>

<div class="col">
  <h3>Latest requests <small>the last 50, whatever the window</small></h3>
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

<div class="two">
  <div class="col">
    <h3>Recent errors <small>in this window</small></h3>
    <div class="scroll"><table>
      <tr><th>time</th><th class="l">request</th><th>status</th></tr>
      {% for r in errors %}
      <tr><td>{{ when(r.at) }}</td><td class="wrap">{{ r.method }} {{ r.path }}{% if r.query %}<span class="muted">?{{ r.query }}</span>{% endif %}</td>
          <td class="s{{ r.status // 100 }}">{{ r.status }} {{ r.error or '' }}</td></tr>
      {% else %}<tr><td colspan="3" class="muted">no errors</td></tr>{% endfor %}
    </table></div>
  </div>
  <div class="col">
    <h3>Backfill jobs <small>the last 15</small></h3>
    <div class="scroll"><table>
      <tr><th>job</th><th class="l">ticker</th><th class="l">status</th><th>progress</th><th>candles</th><th>asked</th></tr>
      {% for j in jobs %}
      <tr><td>{{ j.job_id }}</td><td class="l"><a href="/table/kalshi_candles?ticker={{ j.ticker|urlencode }}">{{ j.ticker }}</a></td>
          <td class="l {{ 's5' if j.status == 'failed' else 's2' if j.status == 'done' else '' }}" title="{{ j.error or '' }}">{{ j.status }}</td>
          <td>{{ j.requests_done }}/{{ j.requests_total if j.requests_total is not none else '?' }}</td>
          <td>{{ '{:,}'.format(j.candles) }}</td><td>{{ when(j.requested_at) }}</td></tr>
      {% else %}<tr><td colspan="6" class="muted">none yet</td></tr>{% endfor %}
    </table></div>
  </div>
</div>
{% endif %}

<div class="col k">
  <h3>Collection <small>what the API's data depends on</small></h3>
  <div class="two" style="gap:24px">
    <dl>
      <dt>Kalshi</dt><dd>{% if c.down %}<span class="s5">unreachable since {{ c.down }}</span>{% else %}<span class="s2">reachable</span>{% endif %}</dd>
      <dt>live candles</dt><dd>{% if c.live %}up to {{ c.live.upto }} &middot; {{ c.live.behind }} s behind &middot; last cycle {{ c.live.seconds }} s{% else %}<span class="muted">no heartbeat</span>{% endif %}</dd>
      <dt>hot list</dt><dd>{% if c.hot %}{{ '{:,}'.format(c.hot.n) }} markets (cap {{ '{:,}'.format(c.hot.cap) }}){% else %}<span class="muted">none published</span>{% endif %}</dd>
      <dt>budget</dt><dd>{{ c.budget }} requests/min{% if c.factor and c.factor < 1 %} &middot; <span class="s5">cut to {{ '%.0f'|format(100 * c.factor) }}% after a 429</span>{% endif %}</dd>
    </dl>
    <table>
      <tr><th>Kalshi requests by</th><th>last minute</th><th>last hour</th></tr>
      {% for name, one, hour in c.used %}
      <tr><td>{{ name }}</td><td>{{ one if one is not none else '?' }}</td><td>{{ '{:,}'.format(hour) if hour is not none else '?' }}</td></tr>
      {% endfor %}
    </table>
  </div>
</div>
"""

# window: (length in seconds, seconds per bar, what a bar is called)
USAGE_WINDOWS = {"1h": (3600, 60, "1 min"), "24h": (86400, 600, "10 min"),
                 "7d": (7 * 86400, 3600, "1 hour"), "30d": (30 * 86400, 6 * 3600, "6 hours")}
USAGE_TZ = ZoneInfo(os.environ.get("PGTIMEZONE", "Asia/Kolkata"))


def _ms(value):
    if value is None:
        return "—"
    return f"{value:.0f} ms" if value < 1000 else f"{value / 1000:.1f} s"


def _size(n):
    n = float(n or 0)
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


def _when(dt):
    if dt is None:
        return ""
    local = dt.astimezone(USAGE_TZ)
    recent = datetime.now(USAGE_TZ) - local < timedelta(hours=20)
    return local.strftime("%H:%M:%S" if recent else "%d %b %H:%M")


def _query(params):
    return "&".join(f"{k}={v}" for k, v in (params or {}).items())


def _collection():
    """The same signals the API's /v1/status reads, from Redis."""
    import json as _json
    out = {"budget": settings.KALSHI_REQUESTS_PER_MIN, "down": None, "live": None, "hot": None,
           "factor": None, "used": []}
    try:
        r = ratelimit.client()
        host = settings.KALSHI_API_URL.split("/")[2]
        down = r.get(f"{settings.REDIS_PREFIX}down:{host}")
        if down:
            out["down"] = _when(datetime.fromtimestamp(float(down), timezone.utc))
        live = r.get(ratelimit.KEY + "live")
        if live:
            live = _json.loads(live)
            out["live"] = {"upto": _when(datetime.fromtimestamp(live["minute"], timezone.utc)),
                           "behind": round(time.time() - live["minute"]),
                           "seconds": live["seconds"]}
        hot = r.get(ratelimit.KEY + "hot")
        if hot:
            hot = _json.loads(hot)
            out["hot"] = {"n": len(hot["tickers"]), "cap": hot["cap"]}
        out["factor"] = ratelimit.factor()
        out["used"] = [(s, ratelimit.used(s, 1), ratelimit.used(s, 60)) for s in ratelimit.SHARE]
    except Exception:
        pass
    return out


@app.route("/api-usage")
def api_usage():
    window = request.args.get("window", "24h")
    if window not in USAGE_WINDOWS:
        window = "24h"
    length, step, bucket_label = USAGE_WINDOWS[window]
    refresh = request.args.get("refresh") != "off"
    page = dict(windows=list(USAGE_WINDOWS), window=window, refresh=refresh,
                bucket_label=bucket_label, now=datetime.now(USAGE_TZ).strftime("%H:%M:%S"),
                ms=_ms, size=_size, when=_when, c=_collection(), missing=False)
    if not db.query("SELECT to_regclass('api_requests') IS NOT NULL AS ok")[0]["ok"]:
        return render_template_string(USAGE_PAGE, **page, missing=True)

    since = {"length": length}
    where = "r.at >= now() - make_interval(secs => %(length)s)"
    t = db.query(f"""
        SELECT count(*) AS n, count(*) FILTER (WHERE status >= 400) AS errors,
               count(*) FILTER (WHERE status = 429) AS limited,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY ms) AS p50,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95,
               COALESCE(sum(bytes), 0) AS bytes, COALESCE(sum(row_count), 0) AS rows_sent,
               count(*) FILTER (WHERE endpoint = '/v1/backfill' AND method = 'POST'
                                AND status = 202) AS backfills
        FROM api_requests r WHERE {where}""", since)[0]

    counts = {int(r["b"]): r for r in db.query(f"""
        SELECT floor(EXTRACT(EPOCH FROM r.at) / %(step)s) * %(step)s AS b, count(*) AS n,
               count(*) FILTER (WHERE status >= 400) AS e
        FROM api_requests r WHERE {where} GROUP BY 1""", {**since, "step": step})}
    end = int(time.time() // step * step)
    starts = list(range(end - (length // step - 1) * step, end + step, step))
    peak = max([counts[s]["n"] for s in starts if s in counts] or [0])
    width = 1000 / len(starts)
    bars = []
    for i, s in enumerate(starts):
        c = counts.get(s)
        n, e = (c["n"], c["e"]) if c else (0, 0)
        bars.append({"x": round(i * width, 2), "w": round(max(width - 1, 0.5), 2),
                     "h": round(110 * n / peak, 1) if peak else 0,
                     "eh": round(110 * e / peak, 1) if peak else 0, "n": n, "e": e,
                     "label": _when(datetime.fromtimestamp(s, timezone.utc))})

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
    tickers = db.query(f"""
        SELECT tk AS t, count(*) AS n, max(r.at) AS last
        FROM api_requests r, unnest(r.tickers) AS tk
        WHERE {where} GROUP BY tk ORDER BY n DESC LIMIT 20""", since)
    recent = db.query("""
        SELECT r.*, k.name FROM api_requests r LEFT JOIN api_keys k USING (key_id)
        ORDER BY r.at DESC LIMIT 50""")
    errors = db.query(f"""
        SELECT r.* FROM api_requests r WHERE {where} AND r.status >= 400
        ORDER BY r.at DESC LIMIT 20""", since)
    for r in recent + errors:
        r["query"] = _query(r["params"])
    jobs = db.query("SELECT * FROM api_jobs ORDER BY job_id DESC LIMIT 15")
    return render_template_string(
        USAGE_PAGE, **page, t=t, bars=bars, peak=peak, callers=callers, endpoints=endpoints,
        tickers=tickers, recent=recent, errors=errors, jobs=jobs,
        first_label=_when(datetime.fromtimestamp(starts[0], timezone.utc)))


@app.route("/stats")
def stats():
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
    return render_template_string(
        STATS_PAGE,
        k=dict(k, **kc, minutes=km, fill=fill, estimate=int(km * fill),
               sectors=sectors("kalshi_markets m JOIN kalshi_events e USING (event_ticker)")),
        p=dict(p, **pc, minutes=pm, estimate=pm,
               sectors=sectors("poly_markets m LEFT JOIN poly_events e USING (event_id)")))


@app.route("/")
def index():
    counts = dict(table_list())
    for name in ("kalshi_markets", "kalshi_series", "kalshi_events", "kalshi_candles", "poly_markets",
                 "poly_events", "poly_candles", "kalshi_perp_markets", "kalshi_perp_candles"):
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
    "kalshi_perp_markets": ["status"],
    "kalshi_perp_candles": ["status"],
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

    rows = db.query(f"SELECT {col}::text AS v, count(*) n FROM {source}"
                    f" WHERE {col} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC LIMIT 300")
    panels = [dict(title=dim.replace("_", " "), dims=dims, dim=dim, table=name,
                   options=[(r["v"], f"{r['n']:,}") for r in rows],
                   selected=value, url=f"{base}&asset=", all_url=base)]
    if not value or name in ("kalshi_series", "poly_events"):
        return panels

    # Second level: the markets carrying that value, busiest first.
    return panels


NEAREST_TITLES = 60        # distinct titles fetched before the threshold is applied


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
    args = {"t": text, "id": ident, "thr": threshold, "pool": NEAREST_TITLES, "per": per}
    rows = []
    with connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if same_venue or name in POLY:
                cur.execute("""
                    WITH near AS (
                        SELECT title, similarity(title, %(t)s) AS score FROM kalshi_titles
                        ORDER BY title <-> %(t)s LIMIT %(pool)s)
                    SELECT DISTINCT ON (m.title) 'kalshi' AS venue, m.ticker AS id,
                           m.title, e.category, m.volume, n.score,
                           to_char(m.close_time, 'YYYY-MM-DD HH24:MI') AS closes,
                           EXISTS (SELECT 1 FROM kalshi_candles c
                                   WHERE c.ticker = m.ticker) AS got
                    FROM near n
                    JOIN kalshi_markets m ON m.title = n.title
                    JOIN kalshi_events e USING (event_ticker)
                    WHERE n.score >= %(thr)s AND m.ticker <> %(id)s
                    ORDER BY m.title, m.volume DESC NULLS LAST""", args)
                found = sorted(cur.fetchall(),
                               key=lambda r: (-r["score"], -float(r["volume"] or 0)))
                rows += found[:per]
            if same_venue or name not in POLY:
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
                rows += cur.fetchall()

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
        # The listing's price narrows the field; the exact test is below,
        # on the newer price where there is one.
        where_k += ["m.close_time > now()",
                    "abs(NULLIF(m.raw->>'last_price_dollars', '')::numeric - %s) <= %s"]
        args_k += [a["last"], (points + 5) / 100]
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
    """Download one market's candles now and store them.

    Runs from the market's own start to its close, or now if it is still open.
    """
    session = requests.Session()
    now = int(datetime.now(timezone.utc).timestamp())

    if name in POLY:
        found = db.query("SELECT yes_token_id, end_date,"
                         " raw->>'startDate' AS start_date"
                         " FROM poly_markets WHERE condition_id = %s", (ident,))
        if not found or not found[0]["yes_token_id"]:
            return 0, "no token id stored for this market"
        row = found[0]
        hi = min(int(row["end_date"].timestamp()), now) if row["end_date"] else now
        lo = hi - 30 * 86400
        if row["start_date"]:
            try:
                lo = int(datetime.fromisoformat(
                    row["start_date"].replace("Z", "+00:00")).timestamp())
            except ValueError:
                pass
        if hi <= lo:
            return 0, "market has not started trading yet"

        data = session.get("https://clob.polymarket.com/prices-history",
                           params={"market": row["yes_token_id"],
                                   "startTs": lo, "endTs": hi, "fidelity": 1},
                           timeout=60).json()
        buckets = {}
        for point in data.get("history") or []:
            buckets.setdefault(point["t"] - point["t"] % 60, []).append(float(point["p"]))
        rows = [(ident, datetime.fromtimestamp(m, timezone.utc),
                 pts[0], max(pts), min(pts), pts[-1], None)
                for m, pts in sorted(buckets.items())]
        table = "poly_candles"
        cols = ("condition_id", "ts", "open", "high", "low", "close", "volume")
    else:
        # The same trades-only fetch live.py and backfill.py use, which also
        # moves the market's bookkeeping forward.
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

    if not rows:
        return 0, "the venue returned no candles for this market"
    with connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur,
                           f"INSERT INTO {table} ({', '.join(cols)}) VALUES %s"
                           " ON CONFLICT DO NOTHING", rows, page_size=1000)
    return len(rows), None


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
                 ("done", f"downloaded {count:,} candles in {elapsed:.0f}s"))
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
                      "poly_events": ["event_id", "title", "slug"],
                      "kalshi_perp_markets": ["ticker"]}
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
    candle_table = {"kalshi_markets": "kalshi_candles", "kalshi_perp_markets": "kalshi_perp_candles",
                    "poly_markets": "poly_candles"}.get(name)
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
    candle_table = {"kalshi_markets": "kalshi_candles",
                    "kalshi_perp_markets": "kalshi_perp_candles",
                    "poly_markets": "poly_candles"}.get(name)

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
<title>visualise</title>
<style>
 body { font-family: system-ui, sans-serif; margin: 1.5rem; color: #222; }
 header { display: flex; align-items: baseline; gap: 18px; }
 header h2 { margin: 0 0 10px; }
 header .links a { font-size: 13px; margin-right: 12px; }
 form.pick { margin-bottom: 12px; display: flex; gap: 8px; align-items: center;
             flex-wrap: wrap; }
 form.pick input[name=ticker] { width: 340px; padding: 5px 8px; font-size: 13px; }
 form.pick select, form.pick button { padding: 5px 8px; font-size: 13px; }
 .hint { font-size: 12px; color: #777; margin-bottom: 14px; }
 .wrap { position: relative; border: 1px solid #ddd; border-radius: 4px;
         padding: 8px; background: #fff; }
 canvas { display: block; width: 100%; height: 520px; }
 #tip { position: absolute; pointer-events: none; display: none; background: #fff;
        border: 1px solid #bbb; border-radius: 4px; padding: 6px 9px; font-size: 12px;
        box-shadow: 0 1px 4px rgba(0,0,0,.15); white-space: nowrap; z-index: 2; }
 #tip b { display: block; margin-bottom: 3px; }
 #tip span { color: #666; }
 .bar { font-size: 12px; color: #666; margin-top: 8px; }
 .bar b { color: #222; }
 .err { color: #b00; }
 .avail { margin-top: 26px; font-size: 13px; }
 .avail table { border-collapse: collapse; font-size: 13px; }
 .avail th, .avail td { border: 1px solid #ddd; padding: 4px 9px; text-align: left; }
 .avail th { background: #f2f2f2; }
 .venue { display: inline-block; width: 15px; height: 15px; line-height: 15px;
          text-align: center; border-radius: 3px; font-size: 10px; color: #fff;
          margin-right: 5px; }
 .v-k { background: #2a6099; } .v-p { background: #99542a; }
</style>

<header>
  <h2><a href="/" style="text-decoration:none;color:inherit">prediction_markets</a></h2>
  <span class="links">
    <a href="/table/kalshi_markets?dim=category">tables</a>
    <a href="/stats">stats</a>
    <a href="/manage">manage</a>
  </span>
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

      ctx.strokeStyle = '#eee'; ctx.fillStyle = '#888';
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
        ctx.strokeStyle = ctx.fillStyle = flat ? '#999' : (up ? '#1a9c5b' : '#cc3d3d');
        ctx.beginPath();
        ctx.moveTo(Math.round(cx) + 0.5, y(b[2]));
        ctx.lineTo(Math.round(cx) + 0.5, y(b[3]));
        ctx.stroke();
        var top = y(Math.max(b[1], b[4])), bot = y(Math.min(b[1], b[4]));
        if (bot - top < 1) ctx.fillRect(cx - body / 2, top, body, 1);
        else ctx.fillRect(cx - body / 2, top, body, bot - top);
      }

      if (vmax > 0) {
        ctx.fillStyle = '#c9d4e4';
        for (var j = 0; j < bars.length; j++) {
          var vh = bars[j][5] / vmax * geom.volH;
          if (vh > 0) ctx.fillRect(x(j) - body / 2, geom.volTop + geom.volH - vh, body, vh);
        }
        ctx.strokeStyle = '#eee'; ctx.beginPath();
        ctx.moveTo(geom.left, geom.volTop + geom.volH + 0.5);
        ctx.lineTo(geom.left + geom.plotW, geom.volTop + geom.volH + 0.5); ctx.stroke();
        ctx.fillStyle = '#888'; ctx.textAlign = 'right';
        ctx.fillText(fmt(vmax), geom.left - 6, geom.volTop + 10);
      }

      // Labels come from the bars themselves: the series is irregular, so evenly
      // spaced ticks would imply a regular spacing that is not there.
      ctx.fillStyle = '#888'; ctx.textAlign = 'center';
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
      ctx.strokeStyle = '#bbb'; ctx.setLineDash([3, 3]); ctx.beginPath();
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
  <b>markets with candles stored</b>
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


def plottable():
    """Every market that actually has candles, for the picker and the list."""
    return db.query("""
        SELECT c.ticker AS id, 'kalshi' AS venue, count(*) AS n,
               min(c.ts)::date::text AS first, max(c.ts)::date::text AS last,
               max(m.title) AS title
        FROM kalshi_candles c LEFT JOIN kalshi_markets m ON m.ticker = c.ticker
        GROUP BY c.ticker
        UNION ALL
        SELECT c.condition_id, 'poly', count(*),
               min(c.ts)::date::text, max(c.ts)::date::text, max(p.question)
        FROM poly_candles c LEFT JOIN poly_markets p ON p.condition_id = c.condition_id
        GROUP BY c.condition_id
        ORDER BY n DESC""")


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

    print(f"http://127.0.0.1:{args.port}")
    app.run(host=args.host, port=args.port, threaded=True)

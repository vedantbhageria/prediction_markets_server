"""The daily data check, and a watchdog between checks.

    python check.py            # run forever (the check service): the watchdog
                               # every 5 minutes, the full check once a day
    python check.py --now      # one full check now, then exit
    python check.py --show     # print the newest check

Once a day, at the quietest hour on both venues (09:00 UTC by default), every
check below runs over the day before (UTC) and each gives ok, warn or fail:

    freshness      Kalshi live, the Polymarket feed and the aggregator are current
    programs       every program is running
    kalshi_sample  a sample of yesterday's Kalshi markets downloaded again: every
                   traded minute stored, each with Kalshi's own numbers
    poly_sample    a sample of yesterday's Polymarket markets read again: every
                   trade stored, under the same id
    poly_candles   a sample of Polymarket candles made again from the stored
                   trades: the same open, high, low, close, volume, notional, count
    sanity         impossible values: prices outside 0-1, high below low, empty
                   volume, candles in the future or after a market closed
    venue_api      what the venues answered in the logs since the last run: HTTP
                   4xx (410 = an endpoint retired), deprecation notices, trades
                   missing fields the code needs
    gaps           minutes Kalshi was unreachable or live fell behind; Polymarket
                   feed gaps
    progress       history still to fetch on each venue, against the last run
    matching       open markets without a record, templates without a vector
    api            API errors and response times
    disk           free space, growth per day and days until full

Results go into the `checks` table (one row per check per run), show on the
viewer's /status page and at GET /v1/checks, and are pushed to a phone through
ntfy (https://ntfy.sh): set NTFY_TOPIC in .env and subscribe to the same topic
in the ntfy app. The push is retried while the network is blocked.

The watchdog, every WATCH_SECONDS, looks only at what needs action now and
pushes the moment it starts (again every 6 hours while it lasts) and when it
clears:

    programs     a program is not running
    kalshi_live  Kalshi live more than 15 minutes behind
    poly_feed    the Polymarket feed more than 30 minutes behind
    aggregate    over 50,000 minutes waiting for their candle and not going down
    disk         less than 10% free
    venue_api    a venue answered 410 (an endpoint retired), sent a
                 deprecation notice, or left out a field the code needs
                 (pushed once, as it happens)
"""

import argparse
import json
import logging
import os
import re
import shutil
import signal
import subprocess
import threading
import time
from datetime import datetime, timezone

import requests
from psycopg2.extras import Json

import db
import fetch
import monitor
import paths
import poly
import ratelimit
import store
import venues

log = logging.getLogger("check")

db.load_env()
CHECK_HOUR = int(os.environ.get("CHECK_HOUR_UTC", "9"))
NTFY_URL = os.environ.get("NTFY_URL", "https://ntfy.sh").rstrip("/")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
PUSH_TRIES = 72                 # every 5 minutes for 6 hours
STATE = paths.state("check.json")
WATCH_STATE = paths.state("watch.json")
WATCH_SECONDS = 300
REMIND_SECONDS = 6 * 3600
LOGS = os.path.expanduser("~/logs")
PROGRAMS = {"discover": "discover.py", "activity": "activity.py", "live": "live.py",
            "trades": "trades.py", "aggregate": "aggregate.py", "backfill": "backfill.py",
            "match": "match.py", "viewer": "viewer.py", "api": "uvicorn api.main"}
SAMPLE = 12                     # markets re-read per venue
OK, WARN, FAIL = "ok", "warn", "fail"
ORDER = {FAIL: 0, WARN: 1, OK: 2}


def yesterday():
    """[start, end) of yesterday, UTC, as unix seconds."""
    end = int(time.time()) // 86400 * 86400
    return end - 86400, end


def _level(value, warn, fail):
    return FAIL if value >= fail else WARN if value >= warn else OK


# ---------------------------------------------------------------------------
# The checks. Each returns (status, value, detail).
# ---------------------------------------------------------------------------

def freshness():
    """How far behind Kalshi live, the Polymarket feed and the aggregator are."""
    out = {}
    live = ratelimit.client().get(ratelimit.KEY + "live")
    behind = time.time() - json.loads(live)["minute"] if live else None
    if behind is None:
        newest = db.query("SELECT EXTRACT(EPOCH FROM now() - max(ts)) AS s FROM kalshi_candles"
                          " WHERE ts > now() - interval '1 day'")[0]["s"]
        behind = float(newest) if newest is not None else 86400
    out["kalshi_live_s"] = round(behind)
    feed = db.query("SELECT EXTRACT(EPOCH FROM now() - covered_to) AS s FROM poly_feed")
    out["poly_feed_s"] = round(float(feed[0]["s"])) if feed else None
    dirty = db.query("SELECT count(*) AS n, EXTRACT(EPOCH FROM now() - min(minute)) AS s"
                     " FROM poly_dirty")[0]
    out["candles_waiting"], out["oldest_waiting_s"] = dirty["n"], round(float(dirty["s"] or 0))
    return out


def check_freshness(ctx):
    out, worst = freshness(), OK
    # Minutes waiting for their candle are counted, not aged: a history
    # backfill marks old minutes, and the aggregator makes ~20,000 a minute.
    for value, warn, fail in ((out["kalshi_live_s"], 300, 1800),
                              (out["poly_feed_s"] if out["poly_feed_s"] is not None else 1e9, 600, 3600),
                              (out["candles_waiting"], 50_000, 500_000)):
        level = _level(value, warn, fail)
        worst = min(worst, level, key=ORDER.get)
    return worst, (f"Kalshi live {out['kalshi_live_s']} s behind, Polymarket feed"
                   f" {out['poly_feed_s']} s, {out['candles_waiting']:,} minutes waiting for their candle"), out


def not_running():
    return [name for name, pattern in PROGRAMS.items()
            if subprocess.run(["pgrep", "-f", pattern], capture_output=True).returncode != 0]


def check_programs(ctx):
    missing = not_running()
    return (FAIL if missing else OK,
            f"not running: {', '.join(missing)}" if missing else f"all {len(PROGRAMS)} running",
            {"missing": missing})


def check_kalshi_sample(ctx):
    """Yesterday's traded minutes of a few covered markets, against Kalshi's."""
    a, b = yesterday()
    tickers = [r["ticker"] for r in db.query("""
        SELECT c.ticker FROM kalshi_candles c JOIN kalshi_tracking t USING (ticker)
        WHERE c.ts >= to_timestamp(%s) AND c.ts < to_timestamp(%s)
          AND t.candles_fetched_from <= to_timestamp(%s) AND t.candles_fetched_to >= to_timestamp(%s)
        GROUP BY c.ticker ORDER BY random() LIMIT %s""", (a, b, a, b, SAMPLE))]
    if not tickers:
        return WARN, "no Kalshi market covered all of yesterday to compare", {}
    missing = extra = wrong = 0
    bad = []
    per = max(1, venues.KALSHI_MAX_CANDLES // 1440)
    with ratelimit.using("warm"):
        for i in range(0, len(tickers), per):
            batch = tickers[i:i + per]
            got = venues.kalshi_candles(batch, a + 60, b, attempts=4)
            for t in batch:
                if t not in got:
                    raise RuntimeError(f"Kalshi did not answer for {t}")
                theirs = {r[1]: tuple(float(x) for x in r[2:7]) for r in fetch._rows(t, got[t], a, b)}
                ours = {r["ts"]: tuple(float(r[k]) for k in ("open", "high", "low", "close", "volume"))
                        for r in db.query("SELECT ts, open, high, low, close, volume FROM kalshi_candles"
                                          " WHERE ticker = %s AND ts >= to_timestamp(%s)"
                                          " AND ts < to_timestamp(%s)", (t, a, b))}
                m = len(set(theirs) - set(ours))
                e = len(set(ours) - set(theirs))
                w = sum(1 for k in set(ours) & set(theirs)
                        if any(abs(x - y) > 1e-6 for x, y in zip(ours[k], theirs[k])))
                missing, extra, wrong = missing + m, extra + e, wrong + w
                if m or e or w:
                    bad.append({"ticker": t, "missing": m, "extra": e, "wrong": w})
    status = FAIL if (missing or extra or wrong) else OK
    return status, (f"{len(tickers)} markets: {missing} minutes missing, {extra} extra,"
                    f" {wrong} different"), {"tickers": tickers, "bad": bad}


def check_poly_sample(ctx):
    """Yesterday's trades of a few covered markets, read again from Polymarket."""
    a, b = yesterday()
    picks = db.query(f"""
        SELECT x.condition_id FROM (
            SELECT condition_id, count(*) AS n FROM poly_trades
            WHERE ts >= to_timestamp(%s) AND ts < to_timestamp(%s) GROUP BY 1) x
        JOIN poly_tracking t USING (condition_id)
        WHERE x.n <= 20000 AND {poly.HISTORY_DONE}
          AND t.trades_from <= to_timestamp(%s) AND poly_covered_to(t.trades_to) >= to_timestamp(%s)
        ORDER BY random() LIMIT %s""", (a, b, a, b, SAMPLE))
    if not picks:
        return WARN, "no Polymarket market covered all of yesterday to compare", {}
    missing = extra = 0
    bad = []
    with ratelimit.using("warm"):
        for r in picks:
            cid = r["condition_id"]
            listed = {x[2] for page in poly.pages(cid, a) for x in map(poly.row, page)
                      if x is not None and a <= x[1].timestamp() < b}
            stored = {x["trade_id"] for x in db.query(
                "SELECT trade_id FROM poly_trades WHERE condition_id = %s AND ts >= to_timestamp(%s)"
                " AND ts < to_timestamp(%s)", (cid, a, b))}
            m, e = len(listed - stored), len(stored - listed)
            missing, extra = missing + m, extra + e
            if m or e:
                bad.append({"market": cid, "missing": m, "extra": e, "listed": len(listed)})
    return (FAIL if missing or extra else OK,
            f"{len(picks)} markets: {missing} trades missing, {extra} extra", {"bad": bad})


CANDLES_AGAIN = """
WITH s AS (SELECT unnest(%(c)s::text[]) AS condition_id),
x AS (
    SELECT t.condition_id, date_trunc('minute', t.ts) AS minute, t.ts, t.trade_id, t.size,
           t.size * t.price AS paid,
           CASE WHEN m.yes_token_id IS NULL OR t.token_id = m.yes_token_id
                THEN t.price ELSE 1 - t.price END AS p
    FROM s JOIN poly_markets m USING (condition_id)
    JOIN poly_trades t ON t.condition_id = s.condition_id
    WHERE t.ts >= to_timestamp(%(a)s) AND t.ts < to_timestamp(%(b)s)),
c AS (
    SELECT condition_id, minute,
           (array_agg(p ORDER BY ts, trade_id))[1] AS o, max(p) AS h, min(p) AS l,
           (array_agg(p ORDER BY ts DESC, trade_id DESC))[1] AS cl,
           sum(size) AS v, sum(paid) AS n, count(*) AS k
    FROM x GROUP BY condition_id, minute),
pc AS (SELECT pc.* FROM poly_candles pc JOIN s USING (condition_id)
       WHERE pc.ts >= to_timestamp(%(a)s) AND pc.ts < to_timestamp(%(b)s))
SELECT count(*) FILTER (WHERE pc.ts IS NULL) AS missing,
       count(*) FILTER (WHERE c.minute IS NULL) AS extra,
       count(*) FILTER (WHERE c.minute IS NOT NULL AND pc.ts IS NOT NULL AND (
           abs(pc.open - c.o) > 1e-6 OR abs(pc.high - c.h) > 1e-6 OR abs(pc.low - c.l) > 1e-6
           OR abs(pc.close - c.cl) > 1e-6 OR abs(pc.volume - c.v) > 1e-4
           OR abs(pc.notional - c.n) > 1e-3 OR pc.trades <> c.k)) AS wrong,
       count(*) AS minutes
FROM c FULL JOIN pc ON pc.condition_id = c.condition_id AND pc.ts = c.minute"""


def check_poly_candles(ctx):
    a, b = yesterday()
    ids = [r["condition_id"] for r in db.query(
        "SELECT condition_id FROM (SELECT DISTINCT condition_id FROM poly_candles"
        " WHERE ts >= to_timestamp(%s) AND ts < to_timestamp(%s)) x ORDER BY random() LIMIT 50",
        (a, b))]
    if not ids:
        return WARN, "no Polymarket candles yesterday", {}
    r = db.query(CANDLES_AGAIN, {"c": ids, "a": a, "b": b})[0]
    bad = r["missing"] + r["extra"] + r["wrong"]
    return (FAIL if bad else OK,
            f"{r['minutes']:,} minutes of {len(ids)} markets: {r['missing']} missing,"
            f" {r['extra']} extra, {r['wrong']} different", dict(r))


SANITY = [
    ("kalshi prices or volumes impossible", """
        SELECT count(*) AS n FROM kalshi_candles WHERE ts >= to_timestamp(%(a)s) AND ts < to_timestamp(%(b)s)
          AND (low > high OR open < low OR open > high OR close < low OR close > high
               OR low < 0 OR high > 1 OR volume <= 0)""", FAIL),
    ("poly prices or volumes impossible", """
        SELECT count(*) AS n FROM poly_candles WHERE ts >= to_timestamp(%(a)s) AND ts < to_timestamp(%(b)s)
          AND (low > high OR open < low OR open > high OR close < low OR close > high
               OR low < 0 OR high > 1 OR volume <= 0 OR trades <= 0)""", FAIL),
    ("poly trades impossible", """
        SELECT count(*) AS n FROM poly_trades WHERE ts >= to_timestamp(%(a)s) AND ts < to_timestamp(%(b)s)
          AND (price <= 0 OR price > 1 OR size <= 0)""", FAIL),
    ("candles in the future", """
        SELECT (SELECT count(*) FROM kalshi_candles WHERE ts > now() + interval '2 minutes')
             + (SELECT count(*) FROM poly_candles WHERE ts > now() + interval '2 minutes') AS n""", FAIL),
    ("kalshi candles after the market closed", """
        SELECT count(*) AS n FROM kalshi_candles c JOIN kalshi_markets m USING (ticker)
        WHERE c.ts >= to_timestamp(%(a)s) AND c.ts < to_timestamp(%(b)s)
          AND c.ts >= m.close_time + interval '2 minutes'""", WARN),
]


def check_sanity(ctx):
    a, b = yesterday()
    found = {}
    status = OK
    for label, sql, level in SANITY:
        n = db.query(sql, {"a": a, "b": b})[0]["n"]
        if n:
            found[label] = n
            status = min(status, level, key=ORDER.get)
    return status, ("; ".join(f"{k}: {v:,}" for k, v in found.items()) or "nothing impossible"), found


def _new_log_lines(ctx):
    """Log lines written since the last run (the last 2 MB of each log the
    first time). The check and the watchdog each keep their own place."""
    if "lines" in ctx:
        return ctx["lines"]
    seen = ctx["state"].setdefault("log_offsets", {})
    lines = {}
    for name in PROGRAMS:
        path = os.path.join(LOGS, f"{name}.log")
        if not os.path.exists(path):
            continue
        size = os.path.getsize(path)
        start = seen.get(name, max(0, size - 2_000_000))
        if start > size:            # the log was rotated
            start = 0
        with open(path, "rb") as f:
            f.seek(start)
            lines[name] = f.read().decode("utf-8", "replace").splitlines()
        seen[name] = size
    ctx["lines"] = lines
    return lines


def venue_answers(ctx):
    """(HTTP 4xx counts by venue, deprecation lines, trades missing fields)
    in the log lines since the last look."""
    counts, deprecated, dropped = {}, [], 0
    for name, lines in _new_log_lines(ctx).items():
        for line in lines:
            m = re.search(r"(kalshi\.com|polymarket\.com)\S*: HTTP (4\d\d)", line)
            if m:
                key = f"{m.group(1).split('.')[0]} {m.group(2)}"
                counts[key] = counts.get(key, 0) + 1
            if "deprecat" in line.lower() or "sunset" in line.lower():
                deprecated.append(line[:200])
            m = re.search(r"trades without the fields needed: (\d+)", line)
            if m:
                dropped += int(m.group(1))
    return counts, deprecated, dropped


def check_venue_api(ctx):
    counts, deprecated, dropped = venue_answers(ctx)
    gone = sum(v for k, v in counts.items() if k.endswith("410"))
    status = (FAIL if gone or dropped else
              WARN if deprecated or sum(counts.values()) > 100 else OK)
    parts = [f"{k}: {v:,}" for k, v in sorted(counts.items())]
    if deprecated:
        parts.append(f"{len(deprecated)} deprecation notices")
    if dropped:
        parts.append(f"{dropped:,} Polymarket trades missing needed fields")
    return status, "; ".join(parts) or "no refusals", {
        "http": counts, "deprecation": deprecated[:10], "trades_missing_fields": dropped}


def check_gaps(ctx):
    a, b = yesterday()
    lines = [x for x in monitor.read(a) if x["t"] < b]
    down = sum(1 for x in lines if x.get("up") is False)
    slow = sum(1 for x in lines if x.get("behind", 0) > 600)
    feed_gaps = sum(1 for x in _new_log_lines(ctx).get("trades", []) if "feed gap" in x)
    detail = {"minutes_recorded": len(lines), "kalshi_down_minutes": down,
              "live_over_10min_behind_minutes": slow, "poly_feed_gaps": feed_gaps}
    status = WARN if down > 60 or slow > 60 or feed_gaps else OK
    if len(lines) < 1200:
        status = WARN
    return status, (f"Kalshi unreachable {down} min, live >10 min behind {slow} min,"
                    f" {feed_gaps} Polymarket feed gaps ({len(lines)} of 1,440 minutes recorded)"), detail


def check_progress(ctx):
    kalshi = monitor.backfill_left()
    p = db.query(f"""SELECT count(*) AS n FROM poly_tracking t JOIN poly_markets m USING (condition_id)
                     WHERE NOT {poly.HISTORY_DONE} AND {poly.TRADED}""")[0]["n"]
    last = ctx["state"].get("progress") or {}
    ctx["state"]["progress"] = {"kalshi": kalshi, "poly": p}
    stuck = [v for v, now in (("kalshi", kalshi), ("poly", p))
             if now and last.get(v) is not None and now >= last[v]]
    done = {v: (last[v] - now) if last.get(v) is not None else None
            for v, now in (("kalshi", kalshi), ("poly", p))}
    return (WARN if stuck else OK,
            f"history left: Kalshi {kalshi:,} markets ({_delta(done['kalshi'])}),"
            f" Polymarket {p:,} ({_delta(done['poly'])})" + (f"; not moving: {', '.join(stuck)}" if stuck else ""),
            {"kalshi_left": kalshi, "poly_left": p, "done_since_last": done})


def _delta(n):
    return "first run" if n is None else f"{n:,} done since the last run"


def check_matching(ctx):
    r = db.query("""
        SELECT (SELECT count(*) FROM kalshi_markets m WHERE m.close_time > now() AND NOT EXISTS
                  (SELECT 1 FROM propositions p WHERE p.venue = 'kalshi' AND p.market_id = m.ticker)) +
               (SELECT count(*) FROM poly_markets m WHERE m.closed IS NOT TRUE AND NOT EXISTS
                  (SELECT 1 FROM propositions p WHERE p.venue = 'poly' AND p.market_id = m.condition_id))
               AS no_record""")[0]
    no_vector = None
    if store.has_vector():
        no_vector = db.query("""SELECT count(DISTINCT p.template) AS n FROM propositions p
                                WHERE NOT p.placeholder AND p.template IS NOT NULL AND NOT EXISTS
                                  (SELECT 1 FROM templates v WHERE v.template = p.template)""")[0]["n"]
    worst = max(r["no_record"], no_vector or 0)
    return (_level(worst, 10_000, 100_000),
            f"open markets without a record: {r['no_record']:,}; templates without a vector:"
            f" {no_vector if no_vector is not None else 'pgvector off'}",
            {"no_record": r["no_record"], "no_vector": no_vector})


def check_api(ctx):
    r = db.query("""SELECT count(*) AS n, count(*) FILTER (WHERE status >= 500) AS e5,
                           percentile_cont(0.95) WITHIN GROUP (ORDER BY ms) AS p95
                    FROM api_requests WHERE at > now() - interval '1 day'""")[0]
    status = WARN if r["e5"] or (r["p95"] or 0) > 5000 else OK
    return status, (f"{r['n']:,} requests, {r['e5']} server errors, slowest 5% at"
                    f" {round(r['p95'] or 0):,} ms"), {k: float(v or 0) for k, v in r.items()}


def check_disk(ctx):
    total, used, free = shutil.disk_usage("/")
    a, b = yesterday()
    sizes = [(x["t"], x["db"]) for x in monitor.read(a - 86400) if "db" in x]
    growth = None
    if len(sizes) > 1 and sizes[-1][0] > sizes[0][0]:
        growth = (sizes[-1][1] - sizes[0][1]) / ((sizes[-1][0] - sizes[0][0]) / 86400)
    days = free / growth if growth and growth > 0 else None
    status = (FAIL if free / total < 0.05 or (days is not None and days < 7) else
              WARN if free / total < 0.15 or (days is not None and days < 30) else OK)
    gb = 1024 ** 3
    value = (f"{free / gb:,.0f} GB free of {total / gb:,.0f} GB;"
             + (f" database grows {growth / gb:.1f} GB a day, full in about {days:,.0f} days"
                if days else " growth not measured yet"))
    return status, value, {"free_gb": round(free / gb, 1), "total_gb": round(total / gb, 1),
                           "growth_gb_day": round(growth / gb, 2) if growth else None,
                           "days_left": round(days) if days else None}


CHECKS = [("freshness", check_freshness), ("programs", check_programs),
          ("kalshi_sample", check_kalshi_sample), ("poly_sample", check_poly_sample),
          ("poly_candles", check_poly_candles), ("sanity", check_sanity),
          ("venue_api", check_venue_api), ("gaps", check_gaps), ("progress", check_progress),
          ("matching", check_matching), ("api", check_api), ("disk", check_disk)]


# ---------------------------------------------------------------------------
# A run, its record and the push
# ---------------------------------------------------------------------------

def _load_state():
    try:
        return json.loads(STATE.read_text())
    except (OSError, ValueError):
        return {}


def run_once():
    ctx = {"state": _load_state()}
    run_at = datetime.now(timezone.utc).replace(microsecond=0)
    results = []
    for name, fn in CHECKS:
        started = time.time()
        try:
            status, value, detail = fn(ctx)
        except Exception as exc:
            status, value, detail = FAIL, f"the check itself failed: {type(exc).__name__}: {exc}", {}
        detail = dict(detail or {}, seconds=round(time.time() - started, 1))
        results.append((name, status, value, detail))
        log.info("%-14s %-4s %s", name, status, value)
    with db.cursor(dict_rows=False) as cur:
        for name, status, value, detail in results:
            cur.execute("INSERT INTO checks (run_at, name, status, value, detail)"
                        " VALUES (%s, %s, %s, %s, %s)", (run_at, name, status, value, Json(detail)))
    STATE.write_text(json.dumps(ctx["state"]))
    return run_at, results


def message(results):
    """(title, body, priority) for the push: failures first."""
    bad = [r for r in results if r[1] != OK]
    fails = sum(1 for r in results if r[1] == FAIL)
    if not bad:
        title = f"prediction_markets: all {len(results)} checks OK"
    else:
        title = f"prediction_markets: {fails} failed, {len(bad) - fails} to look at"
    lines = [f"{'FAIL' if s == FAIL else 'warn'}  {n}: {v}" for n, s, v, _ in
             sorted(bad, key=lambda r: ORDER[r[1]])]
    lines += [f"ok  {n}: {v}" for n, s, v, _ in results if s == OK]
    return title, "\n".join(lines), 5 if fails else 4 if bad else 3


def push(results, stop=None):
    if not NTFY_TOPIC:
        log.warning("NTFY_TOPIC is not set in .env: nothing pushed")
        return False
    title, body, priority = message(results)
    for attempt in range(PUSH_TRIES):
        try:
            r = requests.post(f"{NTFY_URL}/{NTFY_TOPIC}", data=body.encode("utf-8"), timeout=30,
                              headers={"Title": title, "Priority": str(priority),
                                       "Tags": "rotating_light" if priority == 5 else
                                       "warning" if priority == 4 else "white_check_mark"})
            if r.status_code < 300:
                log.info("pushed to ntfy: %s", title)
                return True
            log.warning("ntfy answered %s: %s", r.status_code, r.text[:120])
        except requests.RequestException as exc:
            log.warning("ntfy unreachable (%s); trying again in 5 minutes", type(exc).__name__)
        if stop is not None and stop.wait(300):
            return False
        if stop is None:
            time.sleep(300)
    return False


def _load(path):
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError):
        return {}


def watch_problems(ctx):
    """({condition: problem or None}, [one-off events]) right now."""
    f = freshness()
    missing = not_running()
    total, _, free = shutil.disk_usage("/")
    problems = {
        "programs": f"not running: {', '.join(missing)}" if missing else None,
        "kalshi_live": (f"Kalshi live is {f['kalshi_live_s'] // 60} min behind"
                        if f["kalshi_live_s"] > 900 else None),
        "poly_feed": (f"the Polymarket feed is {f['poly_feed_s'] // 60} min behind"
                      if f["poly_feed_s"] is not None and f["poly_feed_s"] > 1800 else None),
        # A backlog that is large and not shrinking since the last round:
        # the aggregator has stopped or cannot keep up.
        "aggregate": (f"{f['candles_waiting']:,} minutes waiting for their candle, not going down"
                      if f["candles_waiting"] > 50_000
                      and f["candles_waiting"] >= ctx["state"].get("waiting", 0) else None),
        "disk": f"only {100 * free / total:.0f}% of the disk free" if free / total < 0.10 else None,
    }
    ctx["state"]["waiting"] = f["candles_waiting"]
    counts, deprecated, dropped = venue_answers(ctx)
    events = []
    gone = {k: v for k, v in counts.items() if k.endswith("410")}
    if gone:
        events.append("an endpoint is retired (HTTP 410): " + ", ".join(f"{k} x{v}" for k, v in gone.items()))
    if deprecated:
        events.append("deprecation notice: " + deprecated[0])
    if dropped:
        events.append(f"{dropped:,} Polymarket trades arrived without a field the code needs")
    return problems, events


def _push_once(title, body, priority):
    if not NTFY_TOPIC:
        return True                     # nowhere to push: nothing to keep
    try:
        r = requests.post(f"{NTFY_URL}/{NTFY_TOPIC}", data=body.encode("utf-8"), timeout=30,
                          headers={"Title": title, "Priority": str(priority),
                                   "Tags": {5: "rotating_light", 4: "warning"}.get(priority, "white_check_mark")})
        return r.status_code < 300
    except requests.RequestException:
        return False


def watch(stop):
    """The watchdog loop (a thread of the service). Pushes that fail -- the
    network blocked -- are kept and sent on a later round."""
    state = _load(WATCH_STATE)
    pending = []
    while not stop.wait(WATCH_SECONDS):
        try:
            now = time.time()
            ctx = {"state": state}
            problems, events = watch_problems(ctx)
            active = state.setdefault("active", {})
            for name, problem in problems.items():
                if problem and name not in active:
                    active[name] = {"since": now, "pushed": now}
                    pending.append((f"prediction_markets: {name}", problem, 5))
                elif problem and now - active[name]["pushed"] > REMIND_SECONDS:
                    active[name]["pushed"] = now
                    hours = (now - active[name]["since"]) / 3600
                    pending.append((f"prediction_markets: still {name}",
                                    f"{problem} (for {hours:.0f} h)", 4))
                elif not problem and name in active:
                    took = (now - active.pop(name)["since"]) / 60
                    pending.append((f"prediction_markets: {name} fine again",
                                    f"recovered after {took:.0f} min", 3))
            pending += [("prediction_markets: venue_api", e, 5) for e in events]
            for name, problem in problems.items():
                if problem:
                    log.warning("watch: %s: %s", name, problem)
            WATCH_STATE.write_text(json.dumps(state))
            pending = [p for p in pending if not _push_once(*p)][-50:]
        except Exception as exc:
            log.warning("watch round failed: %s: %s", type(exc).__name__, exc)


def latest():
    return db.query("SELECT * FROM checks WHERE run_at = (SELECT max(run_at) FROM checks)"
                    " ORDER BY CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, name")


def next_run(now=None):
    now = now or time.time()
    at = now // 86400 * 86400 + CHECK_HOUR * 3600
    return at if at > now else at + 86400


def main():
    parser = argparse.ArgumentParser(description="The daily data check.")
    parser.add_argument("--now", action="store_true", help="one run now, then exit")
    parser.add_argument("--show", action="store_true", help="print the newest run")
    parser.add_argument("--no-push", action="store_true", help="do not push to ntfy")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s",
                        datefmt="%H:%M:%S")
    if args.show:
        for r in latest():
            print(f"{r['run_at']:%Y-%m-%d %H:%M}  {r['status']:<4}  {r['name']:<14} {r['value']}")
        return
    if args.now:
        store.ensure_schema()
        _, results = run_once()
        if not args.no_push:
            push(results)
        return
    store.ensure_schema()
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    threading.Thread(target=watch, args=(stop,), name="watch", daemon=True).start()
    while not stop.is_set():
        at = next_run()
        log.info("next check at %s UTC", time.strftime("%Y-%m-%d %H:%M", time.gmtime(at)))
        if stop.wait(max(0, at - time.time())):
            break
        try:
            _, results = run_once()
            if not args.no_push:
                push(results, stop)
        except Exception as exc:
            log.warning("check run failed: %s: %s", type(exc).__name__, exc)


if __name__ == "__main__":
    main()

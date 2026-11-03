"""Stress test of the whole system, on the server, away from the real data.

    .venv/bin/python tests/stress_test.py                 # every phase, ~30 min
    .venv/bin/python tests/stress_test.py reads load      # just these phases
    .venv/bin/python tests/stress_test.py --keep          # leave its schemas behind

Everything runs in its own schema (pm_stress, pm_stress_collect), its own
Redis (a redis-server started here, on port 6391) and against the simulated
Kalshi, so the real data and the real Kalshi are never touched. The real live
collection is watched throughout, to show whether the load reached it.

Phases (each records its numbers in /tmp/pmstress/results.json):

  data      ~8 million candles over 2,000 markets and 30 days, older chunks
            compressed, as the store will look once the backfill is done
  reads     every kind of API read against them, one at a time
  load      the API under 1, 5, 20, 50 and 100 concurrent clients
  limits    a key's limit stays exact under 50 threads; a flood of bad keys
            does not lock out a good one
  pushes    ~1,000 backfill pushes at once, against the simulated Kalshi
  viewer    market pages, both panels and the usage page, under load
  faults    the database connections cut, then Redis killed, under load
  collect   discover, activity, live and backfill against a simulated Kalshi
            ten times the size, at five times today's budget
  records   every API request of the run is in api_requests
  store     the writers production showed failing: candle writes and scoring
            racing on the same bookkeeping rows (deadlocks), a backfill into
            compressed days, and a prune of markets in compressed chunks
"""

import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
import urllib.request
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORK = Path("/tmp/pmstress")
REDIS_PORT, MOCK_PORT, API_PORT, VIEWER_PORT, BIG_MOCK_PORT = 6391, 18300, 18200, 18210, 18320
API = f"http://127.0.0.1:{API_PORT}"
VIEWER = f"http://127.0.0.1:{VIEWER_PORT}"
SCHEMA = "pm_stress"
BUDGET = 1200                   # Kalshi's published 20/s, five times ours

ENV = dict(os.environ, PGSCHEMA=SCHEMA, REDIS_URL=f"redis://127.0.0.1:{REDIS_PORT}/0",
           REDIS_PREFIX="pmstress:", PM_STATE_DIR=str(WORK / "state"),
           KALSHI_API_URL=f"http://127.0.0.1:{MOCK_PORT}/trade-api/v2", LIVE_VENUES="kalshi",
           KALSHI_REQUESTS_PER_MIN=str(BUDGET), BACKFILL_DAYS="2",
           API_RATE_PER_MIN="100000000", API_BACKFILL_PER_MIN="1000000",
           VIEWER_USER="", VIEWER_PASSWORD_HASH="", PYTHONUNBUFFERED="1")
os.environ.update({k: ENV[k] for k in ("PGSCHEMA", "REDIS_URL", "REDIS_PREFIX", "PM_STATE_DIR",
                                       "KALSHI_API_URL", "KALSHI_REQUESTS_PER_MIN",
                                       "BACKFILL_DAYS")}, PGAPPNAME="pmstress-test")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import redis                  # noqa: E402
import requests               # noqa: E402

import db                     # noqa: E402
import mock_kalshi            # noqa: E402
import store                  # noqa: E402

START = time.time()
RESULTS = WORK / "results.json"
procs = {}
results = {}
sent = {"api": 0}               # API requests made, for the records phase
sent_lock = threading.Lock()


def say(text):
    print(f"[{(time.time() - START) / 60:5.1f} min] {text}", flush=True)


def save():
    RESULTS.write_text(json.dumps(results, indent=1, default=str))


def pct(values, q):
    if not values:
        return None
    values = sorted(values)
    return values[min(len(values) - 1, int(q * len(values)))]


def summary(lat, statuses, seconds):
    return {"requests": len(lat), "per_second": round(len(lat) / seconds, 1),
            "p50_ms": round(pct(lat, 0.5) * 1000, 1) if lat else None,
            "p95_ms": round(pct(lat, 0.95) * 1000, 1) if lat else None,
            "p99_ms": round(pct(lat, 0.99) * 1000, 1) if lat else None,
            "max_ms": round(max(lat) * 1000, 1) if lat else None,
            "statuses": {str(k): statuses.count(k) for k in sorted(set(statuses))}}


# ---------------------------------------------------------------------------
# Processes
# ---------------------------------------------------------------------------

def start(name, args, env=None, app=None):
    log = open(WORK / f"{name}.log", "a")
    e = dict(env or ENV, PGAPPNAME=app or f"pmstress-{name}")
    procs[name] = subprocess.Popen(args, cwd=ROOT, env=e, stdout=log, stderr=subprocess.STDOUT,
                                   start_new_session=True)


def _signal(p, sig):
    """Signal a process's whole group if it leads one, else just the process."""
    try:
        os.killpg(p.pid, sig)
    except ProcessLookupError:
        try:
            p.send_signal(sig)
        except ProcessLookupError:
            pass


def stop(name, sig=signal.SIGTERM):
    """Stop a process and everything it started (the API's worker processes)."""
    p = procs.pop(name, None)
    if p and p.poll() is None:
        _signal(p, sig)
        try:
            p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            _signal(p, signal.SIGKILL)
            p.wait(timeout=10)


def wait_http(url, seconds=90):
    for _ in range(seconds * 2):
        try:
            if requests.get(url, timeout=3).status_code < 500:
                return True
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise SystemExit(f"{url} did not come up")


def start_redis():
    start("redis", ["redis-server", "--port", str(REDIS_PORT), "--save", "", "--appendonly", "no"])
    for _ in range(50):
        try:
            redis.Redis(port=REDIS_PORT).ping()
            return
        except redis.RedisError:
            time.sleep(0.1)


API_WORKERS = int(os.environ.get("STRESS_API_WORKERS", "4"))


def start_api():
    start("api", [sys.executable, "-m", "uvicorn", "api.main:app", "--host", "127.0.0.1",
                  "--port", str(API_PORT), "--workers", str(API_WORKERS)])
    wait_http(API + "/health")


def start_viewer():
    start("viewer", [sys.executable, "viewer.py", "--host", "127.0.0.1", "--port", str(VIEWER_PORT)])
    wait_http(VIEWER + "/")


def run(name, args, env=None, timeout=3600):
    a = time.time()
    with open(WORK / f"{name}.log", "a") as log:
        out = subprocess.run(args, cwd=ROOT, env=dict(env or ENV, PGAPPNAME=f"pmstress-{name}"),
                             timeout=timeout, stdout=log, stderr=subprocess.STDOUT)
    return out.returncode, time.time() - a


# ---------------------------------------------------------------------------
# Watching: the real live collection, the API's memory, connections in use
# ---------------------------------------------------------------------------

class Watch:
    """Samples every two seconds while a phase runs."""

    def __init__(self):
        self.samples, self.stop = [], threading.Event()
        self.prod = redis.Redis(port=6379, socket_timeout=2)
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc):
        self.stop.set()
        self.thread.join()

    def _loop(self):
        while not self.stop.wait(2):
            s = {"t": time.time()}
            try:
                live = json.loads(self.prod.get("pm:kalshi:live") or "null")
                s["prod_behind"] = round(time.time() - live["minute"]) if live else None
                s["prod_cycle"] = live["seconds"] if live else None
            except Exception:
                pass
            try:
                rows = db.query("SELECT application_name AS app, count(*) AS n FROM pg_stat_activity"
                                " WHERE datname = current_database() GROUP BY 1")
                s["connections"] = {r["app"] or "?": r["n"] for r in rows}
            except Exception:
                pass
            for name in ("api", "viewer"):
                p = procs.get(name)
                if p and p.poll() is None:
                    try:
                        rss = int(Path(f"/proc/{p.pid}/status").read_text().split("VmRSS:")[1]
                                  .split()[0])
                        s[f"{name}_mb"] = round(rss / 1024)
                    except Exception:
                        pass
            s["load"] = os.getloadavg()[0]
            self.samples.append(s)

    def report(self):
        def col(k):
            return [s[k] for s in self.samples if s.get(k) is not None]
        conns = [sum(s.get("connections", {}).values()) for s in self.samples]
        stress = [sum(v for k, v in s.get("connections", {}).items() if k.startswith("pmstress"))
                  for s in self.samples]
        return {"prod_behind_max_s": max(col("prod_behind"), default=None),
                "prod_cycle_max_s": max(col("prod_cycle"), default=None),
                "api_mb_max": max(col("api_mb"), default=None),
                "viewer_mb_max": max(col("viewer_mb"), default=None),
                "db_connections_max": max(conns, default=None),
                "stress_connections_max": max(stress, default=None),
                "load_max": round(max(col("load"), default=0), 1)}


# ---------------------------------------------------------------------------
# Load generation: several processes, each with threads
# ---------------------------------------------------------------------------

def _thread_loop(base, key, picker, deadline, seed):
    rng = random.Random(seed)
    s = requests.Session()
    if key:
        s.headers["X-API-Key"] = key
    out = []
    while time.time() < deadline:
        method, path, params, body = picker(rng)
        a = time.time()
        try:
            r = s.request(method, base + path, params=params, json=body, timeout=120)
            status = r.status_code
            r.content
        except requests.RequestException:
            status = 0
        out.append((time.time() - a, status, a))
    return out


def _process(base, key, picker_name, clients, deadline, seed):
    picker = PICKERS[picker_name]
    with ThreadPoolExecutor(clients) as pool:
        futures = [pool.submit(_thread_loop, base, key, picker, deadline, seed * 1000 + i)
                   for i in range(clients)]
        return [x for f in futures for x in f.result()]


def hammer(base, key, picker_name, clients, seconds, api=True):
    """`clients` concurrent clients for `seconds`. Returns [(latency, status, at)]."""
    procs_n = min(4, clients)
    per = [clients // procs_n + (1 if i < clients % procs_n else 0) for i in range(procs_n)]
    deadline = time.time() + seconds
    with ProcessPoolExecutor(procs_n) as pool:
        futures = [pool.submit(_process, base, key, picker_name, n, deadline, i)
                   for i, n in enumerate(per) if n]
        out = [x for f in futures for x in f.result()]
    if api:
        with sent_lock:
            sent["api"] += len(out)
    return out


BUSY, MEDIUM, LIGHT = "SM00000", "SM00100", "SM01000"


def _ticker(rng):
    r = rng.random()
    return f"SM{rng.randrange(0, 50) if r < 0.5 else rng.randrange(50, 2000):05d}"


def pick_api(rng):
    r, now = rng.random(), int(time.time())
    t = _ticker(rng)
    if r < 0.40:
        start = now - rng.randrange(1, 29) * 86400
        return "GET", f"/v1/markets/{t}/candles", {"interval": "1m", "start": start,
                                                   "end": start + 86400}, None
    if r < 0.55:
        return "GET", f"/v1/markets/{t}/candles", {"interval": "1h", "start": now - 7 * 86400}, None
    if r < 0.65:
        tickers = ",".join(_ticker(rng) for _ in range(10))
        return "GET", "/v1/candles", {"tickers": tickers, "interval": "5m",
                                      "start": now - 86400}, None
    if r < 0.75:
        return "GET", "/v1/markets", {"q": rng.choice(["Bitcoin", "Fed", "Lakers", "gold"]),
                                      "status": "open", "limit": 100}, None
    if r < 0.85:
        return "GET", f"/v1/markets/{t}", None, None
    if r < 0.95:
        return "GET", "/v1/latest", {"tickers": ",".join(_ticker(rng) for _ in range(50))}, None
    return "GET", "/v1/status", None, None


def pick_cheap(rng):
    return "GET", "/v1", None, None


def pick_viewer(rng):
    r, t = rng.random(), _ticker(rng)
    if r < 0.35:
        return "GET", "/table/kalshi_candles", {"ticker": t}, None
    if r < 0.55:
        return "GET", "/related", {"table": "kalshi_candles", "ticker": t,
                                   "t": rng.choice([0.15, 0.25, 0.5])}, None
    if r < 0.75:
        return "GET", "/nearby", {"table": "kalshi_candles", "ticker": t,
                                  "by": rng.choice(["time", "price", "price,time"]),
                                  "near": 6, "pts": 5, "cat": 1}, None
    if r < 0.9:
        return "GET", "/api-usage", {"window": rng.choice(["1h", "24h", "30d"]),
                                     "refresh": "off"}, None
    return "GET", "/", None, None


PICKERS = {"api": pick_api, "cheap": pick_cheap, "viewer": pick_viewer}


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

WORDS = ["Bitcoin", "Ethereum", "the Fed", "inflation", "Tesla", "Nvidia", "the Lakers",
         "Chelsea", "gold", "oil", "the Senate", "Texas", "OpenAI", "SpaceX", "Tokyo"]


def phase_data():
    """~8 million candles over 2,000 markets and 30 days."""
    say("data: making 2,000 markets and their candles")
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            cur.execute(f"CREATE SCHEMA {SCHEMA}")
    store.ensure_schema()
    words = "ARRAY[" + ",".join(f"'{w}'" for w in WORDS) + "]"
    timings = {}
    a = time.time()
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO kalshi_series (series_ticker, category, title, frequency)
                SELECT 'SS' || i, (ARRAY['Crypto','Politics','Sports','Economics'])[1 + i % 4],
                       'Stress series ' || i, 'daily' FROM generate_series(0, 39) i""")
            cur.execute("""INSERT INTO kalshi_events (event_ticker, series_ticker, title, category, raw)
                SELECT 'SE' || lpad(i::text, 4, '0'), 'SS' || (i % 40), 'Stress event ' || i,
                       (ARRAY['Crypto','Politics','Sports','Economics'])[1 + (i % 40) % 4], '{}'
                FROM generate_series(0, 399) i""")
            cur.execute(f"""INSERT INTO kalshi_markets (ticker, event_ticker, title, status,
                       open_time, close_time, strike_type, volume, open_interest, raw)
                SELECT 'SM' || lpad(i::text, 5, '0'), 'SE' || lpad((i % 400)::text, 4, '0'),
                       'Will ' || ({words})[1 + i % 15] || ' beat ' || ({words})[1 + (i / 15) % 15]
                           || ' by day ' || (i % 28 + 1) || '?',
                       'active', now() - interval '31 days', now() + ((i % 72) + 1) * interval '1 hour',
                       'greater', 1000000 / (i + 1), 100,
                       jsonb_build_object('last_price_dollars', to_char(0.05 + random() * 0.9, 'FM0.0000'),
                                          'yes_sub_title', 'Stress ' || i)
                FROM generate_series(0, 1999) i""")
            cur.execute("INSERT INTO kalshi_titles SELECT DISTINCT title FROM kalshi_markets"
                        " ON CONFLICT DO NOTHING")
    for name, lo, hi, density in (("busy", 0, 50, 0.9), ("medium", 50, 500, 0.2),
                                  ("light", 500, 2000, 0.03)):
        b = time.time()
        db.execute("""
            INSERT INTO kalshi_candles (ticker, ts, open, high, low, close, volume, open_interest)
            SELECT m.ticker, t, p, p + 0.01, p - 0.01, p, 1 + floor(random() * 50), 100
            FROM kalshi_markets m
            CROSS JOIN LATERAL generate_series(date_trunc('minute', now() - interval '30 days'),
                                               date_trunc('minute', now() - interval '3 minutes'),
                                               interval '1 minute') t
            CROSS JOIN LATERAL (SELECT round((0.2 + 0.6 * random())::numeric, 2) AS p) x
            WHERE m.ticker BETWEEN %s AND %s AND random() < %s""",
                   (f"SM{lo:05d}", f"SM{hi - 1:05d}", density))
        timings[name] = round(time.time() - b)
        say(f"data: {name} markets written in {timings[name]}s")
    db.execute("""INSERT INTO kalshi_tracking (ticker, volume, volume_measured_at, activity_score,
                      candles_fetched_from, candles_fetched_to)
                  SELECT ticker, volume, now(), 1000.0 / (row_number() OVER (ORDER BY ticker)),
                         date_trunc('minute', now() - interval '30 days'),
                         date_trunc('minute', now() - interval '2 minutes')
                  FROM kalshi_markets""")
    b = time.time()
    compressed = db.query("SELECT count(compress_chunk(c, if_not_compressed => true)) AS n"
                          " FROM show_chunks('kalshi_candles', older_than => interval '2 days') c")
    db.execute("ANALYZE")
    rows = db.query("SELECT count(*) AS n FROM kalshi_candles")[0]["n"]
    size = db.query("SELECT pg_size_pretty(hypertable_size('kalshi_candles')) AS s")[0]["s"]
    results["data"] = {"markets": 2000, "candles": rows, "size": size,
                       "chunks_compressed": compressed[0]["n"], "seconds": round(time.time() - a),
                       "write_seconds_by_tier": timings, "compress_seconds": round(time.time() - b)}
    say(f"data: {rows:,} candles, {size}, {compressed[0]['n']} chunks compressed")
    save()


def make_keys():
    from api import keys
    out = {}
    for name, scope, rate in (("stress", "full", None), ("limited", "full", 100),
                              ("reader", "read", None)):
        db.execute("DELETE FROM api_keys WHERE name = %s", (name,))
        out[name] = keys.create(name, scope, rate)
    return out


def timed_get(s, path, params=None, pages=False):
    """One request, or every page of it. Returns (seconds, rows, bytes, status)."""
    a, rows, size, token = time.time(), 0, 0, None
    while True:
        p = dict(params or {})
        if token:
            p["page_token"] = token
        r = s.get(API + path, params=p, timeout=300)
        size += len(r.content)
        with sent_lock:
            sent["api"] += 1
        if r.status_code != 200:
            return time.time() - a, rows, size, r.status_code
        rows += int(r.headers.get("X-Row-Count", 0))
        token = (r.headers.get("X-Next-Page-Token") if p.get("format") in ("csv", "parquet")
                 else (r.json().get("next_page_token") if pages else None))
        if not (pages and token):
            return time.time() - a, rows, size, 200


def phase_reads(keys):
    say("reads: every kind of API read, three times each")
    s = requests.Session()
    s.headers["X-API-Key"] = keys["stress"]
    now = int(time.time())
    month = {"start": now - 30 * 86400, "end": now}
    many = ",".join(f"SM{i:05d}" for i in range(0, 2000, 20))         # 100 markets
    cases = [
        ("1m, 1 day, busy market", f"/v1/markets/{BUSY}/candles", {"start": now - 86400}, False),
        ("1m, 1 day, raw", f"/v1/markets/{BUSY}/candles", {"start": now - 86400, "fill": "raw"}, False),
        ("1m, 1 day, quiet market", f"/v1/markets/{LIGHT}/candles", {"start": now - 86400}, False),
        ("1m, 1 day, 10 days ago (compressed)", f"/v1/markets/{BUSY}/candles",
         {"start": now - 10 * 86400, "end": now - 9 * 86400}, False),
        ("1m, 30 days, json, all pages", f"/v1/markets/{BUSY}/candles", month, True),
        ("1m, 30 days, parquet, all pages", f"/v1/markets/{BUSY}/candles",
         dict(month, format="parquet"), True),
        ("1m, 30 days, csv, all pages", f"/v1/markets/{BUSY}/candles",
         dict(month, format="csv"), True),
        ("5m, 30 days", f"/v1/markets/{BUSY}/candles", dict(month, interval="5m"), True),
        ("1h, 30 days", f"/v1/markets/{BUSY}/candles", dict(month, interval="1h"), True),
        ("1d, 30 days", f"/v1/markets/{BUSY}/candles", dict(month, interval="1d"), True),
        ("100 markets, 1h, 7 days, all pages", "/v1/candles",
         {"tickers": many, "interval": "1h", "start": now - 7 * 86400}, True),
        ("100 markets, 1m, 1 day, all pages", "/v1/candles",
         {"tickers": many, "interval": "1m", "start": now - 86400}, True),
        ("latest, 1,000 markets", "/v1/latest",
         {"tickers": ",".join(f"SM{i:05d}" for i in range(1000))}, False),
        ("markets: search 'Bitcoin', open", "/v1/markets", {"q": "Bitcoin", "status": "open"}, False),
        ("markets: all, by activity, 1,000", "/v1/markets", {"sort": "-activity", "limit": 1000}, False),
        ("market detail", f"/v1/markets/{BUSY}", None, False),
        ("events", "/v1/events", {"limit": 1000}, False),
        ("categories", "/v1/categories", None, False),
        ("status", "/v1/status", None, False),
    ]
    out = []
    for name, path, params, pages in cases:
        runs = [timed_get(s, path, params, pages) for _ in range(3)]
        secs = sorted(r[0] for r in runs)[1]
        out.append({"case": name, "seconds": round(secs, 3), "rows": runs[0][1],
                    "bytes": runs[0][2], "status": runs[0][3]})
        say(f"reads: {name}: {secs:.2f}s, {runs[0][1]:,} rows, {runs[0][2] / 1e6:.1f} MB")
    results["reads"] = out
    save()


def phase_load(keys):
    out = []
    for clients in (1, 5, 20, 50, 100):
        with Watch() as w:
            got = hammer(API, keys["stress"], "api", clients, 25)
        row = dict(summary([g[0] for g in got], [g[1] for g in got], 25), clients=clients,
                   api_workers=API_WORKERS, **w.report())
        out.append(row)
        say(f"load: {clients} clients: {row['per_second']}/s, p50 {row['p50_ms']} ms, p95"
            f" {row['p95_ms']} ms, p99 {row['p99_ms']} ms, {row['statuses']}, api"
            f" {row['api_mb_max']} MB, db conns {row['stress_connections_max']},"
            f" prod live behind max {row['prod_behind_max_s']} s")
        results["load"] = out
        save()


def phase_limits(keys):
    say("limits: 50 threads against a key allowed 100 a minute")
    time.sleep(61 - time.time() % 60)
    got = hammer(API, keys["limited"], "cheap", 50, 15)
    ok = sum(1 for g in got if g[1] == 200)
    limited = sum(1 for g in got if g[1] == 429)
    res = {"limited_key": {"requests": len(got), "allowed": ok, "refused_429": limited,
                           "exact": ok == 100}}
    say(f"limits: {ok} allowed of {len(got)}, {limited} refused")

    say("limits: a flood of bad keys while a good key keeps working")
    time.sleep(61 - time.time() % 60)
    flood = threading.Thread(target=lambda: res.__setitem__(
        "flood", hammer(API, "pm_not_a_real_key", "cheap", 20, 15)))
    flood.start()
    time.sleep(3)
    good = hammer(API, keys["stress"], "cheap", 2, 8)
    flood.join()
    bad = res.pop("flood")
    res["bad_key_flood"] = {"requests": len(bad),
                            "refused_401": sum(1 for g in bad if g[1] == 401),
                            "refused_429": sum(1 for g in bad if g[1] == 429),
                            "good_key_requests": len(good),
                            "good_key_ok": sum(1 for g in good if g[1] == 200)}
    say(f"limits: flood {res['bad_key_flood']}")
    results["limits"] = res
    save()


def phase_pushes(keys):
    say("pushes: the simulated Kalshi, its markets discovered, then a push for every one")
    start("mock", [sys.executable, "tests/mock_kalshi.py", "--port", str(MOCK_PORT),
                   "--state", str(WORK / "mock.json")])
    for _ in range(50):
        if (WORK / "mock.json").exists():
            break
        time.sleep(0.2)
    t0 = json.loads((WORK / "mock.json").read_text())["t0"]
    run("discover", [sys.executable, "discover.py", "--once", "--no-prune"])
    run("activity", [sys.executable, "activity.py", "--once"])
    markets, _ = mock_kalshi.build(t0)
    tickers = [m["ticker"] for m in markets]
    s = requests.Session()
    s.headers["X-API-Key"] = keys["stress"]
    a = time.time()
    with ThreadPoolExecutor(10) as pool:
        posted = list(pool.map(lambda t: s.post(API + "/v1/backfill", json={"ticker": t},
                                                timeout=60), tickers))
    with sent_lock:
        sent["api"] += len(posted)
    accepted = {t: r.json()["job_id"] for t, r in zip(tickers, posted) if r.status_code == 202}
    post_seconds = time.time() - a
    say(f"pushes: {len(accepted)} of {len(tickers)} accepted in {post_seconds:.1f}s; waiting")
    with Watch() as w:
        while time.time() - a < 1800:
            left = db.query("SELECT count(*) AS n FROM api_jobs WHERE status IN ('queued', 'running')")[0]["n"]
            if not left:
                break
            time.sleep(5)
    done_at = time.time()
    states = {r["status"]: r["n"] for r in db.query(
        "SELECT status, count(*) AS n FROM api_jobs GROUP BY 1")}
    # The Kalshi budget, from the simulated Kalshi's own log.
    log = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{MOCK_PORT + 1}/_stats",
                                            timeout=60).read())["requests"]
    times = sorted(e[0] for e in log if e[0] >= a)
    worst60 = max((sum(1 for x in times[i:i + 5000] if x < t + 60) for i, t in enumerate(times)),
                  default=0)
    worst1 = max((sum(1 for x in times[i:i + 200] if x < t + 1) for i, t in enumerate(times)),
                 default=0)
    # Spot check: 25 markets' stored candles against the true trades.
    wrong = 0
    by = {m["ticker"]: m for m in markets}
    for t in random.Random(1).sample(list(accepted), min(25, len(accepted))):
        j = db.query("SELECT * FROM api_jobs WHERE job_id = %s", (accepted[t],))[0]
        lo, hi = int(j["from_ts"].timestamp()), int(j["to_ts"].timestamp())
        truth = sum(1 for x in range(lo, hi, 60) if mock_kalshi.candle(by[t], x))
        if truth != j["candles"]:
            wrong += 1
    results["pushes"] = {"markets": len(tickers), "accepted": len(accepted),
                         "post_seconds": round(post_seconds, 1),
                         "all_done_seconds": round(done_at - a, 1), "jobs": states,
                         "kalshi_requests": len(times),
                         "kalshi_429": sum(1 for e in log if e[0] >= a and e[2] == 429),
                         "busiest_60s": worst60, "busiest_1s": worst1, "budget_per_min": BUDGET,
                         "spot_checked": 25, "spot_wrong": wrong, **w.report()}
    say(f"pushes: {results['pushes']}")
    save()
    stop("mock")


def phase_viewer(keys):
    start_viewer()
    s = requests.Session()
    single = []
    for name, path, params in (
            ("home", "/", None),
            ("market page, busy", "/table/kalshi_candles", {"ticker": BUSY}),
            ("market page, quiet", "/table/kalshi_candles", {"ticker": LIGHT}),
            ("related wording", "/related", {"table": "kalshi_candles", "ticker": BUSY, "t": 0.25}),
            ("nearby by expiry", "/nearby", {"table": "kalshi_candles", "ticker": BUSY, "by": "time",
                                             "near": 6, "pts": 5, "cat": 1}),
            ("nearby by odds", "/nearby", {"table": "kalshi_candles", "ticker": BUSY, "by": "price",
                                           "near": 1, "pts": 5, "cat": 0}),
            ("usage page, 1h", "/api-usage", {"window": "1h", "refresh": "off"}),
            ("usage page, 30d", "/api-usage", {"window": "30d", "refresh": "off"})):
        times, status = [], None
        for _ in range(3):
            a = time.time()
            r = s.get(VIEWER + path, params=params, timeout=300)
            times.append(time.time() - a)
            status = r.status_code
        single.append({"page": name, "seconds": round(sorted(times)[1], 3), "status": status,
                       "bytes": len(r.content)})
        say(f"viewer: {name}: {sorted(times)[1]:.2f}s ({status})")
    requests_logged = db.query("SELECT count(*) AS n FROM api_requests")[0]["n"]
    out = []
    for clients in (1, 10, 30):
        with Watch() as w:
            got = hammer(VIEWER, None, "viewer", clients, 20, api=False)
        row = dict(summary([g[0] for g in got], [g[1] for g in got], 20), clients=clients,
                   **w.report())
        out.append(row)
        say(f"viewer: {clients} clients: {row['per_second']}/s, p50 {row['p50_ms']} ms,"
            f" p95 {row['p95_ms']} ms, {row['statuses']}")
    results["viewer"] = {"pages": single, "api_requests_logged": requests_logged, "load": out}
    save()


def _timeline(got, t0):
    """Errors per second, relative to t0."""
    errs = {}
    for lat, status, at in got:
        if status != 200:
            sec = int(at - t0)
            errs[sec] = errs.get(sec, 0) + 1
    return errs


def phase_faults(keys):
    if "viewer" not in procs:
        start_viewer()
    res = {}
    # 1. Every database connection of the API and the viewer cut, mid-load.
    say("faults: cutting every database connection of the API and viewer under load")
    box = {}
    t0 = time.time()
    th_api = threading.Thread(target=lambda: box.__setitem__(
        "api", hammer(API, keys["stress"], "api", 20, 30)))
    th_v = threading.Thread(target=lambda: box.__setitem__(
        "viewer", hammer(VIEWER, None, "viewer", 5, 30, api=False)))
    th_api.start(), th_v.start()
    time.sleep(10)
    cut = db.query("SELECT count(pg_terminate_backend(pid)) AS n FROM pg_stat_activity"
                   " WHERE application_name IN ('pmstress-api', 'pmstress-viewer')")[0]["n"]
    cut_at = time.time() - t0
    th_api.join(), th_v.join()
    for name in ("api", "viewer"):
        got = box[name]
        after = [g for g in got if g[2] - t0 >= cut_at]
        errors = [g for g in after if g[1] != 200]
        res[f"db_cut_{name}"] = {
            "connections_cut": cut, "requests_after_cut": len(after), "errors_after_cut": len(errors),
            "error_statuses": {str(k): [g[1] for g in errors].count(k) for k in {g[1] for g in errors}},
            "last_error_after_s": round(max((g[2] - t0 - cut_at for g in errors), default=0), 1),
            "errors_by_second": _timeline(got, t0)}
        say(f"faults: db cut, {name}: {res[f'db_cut_{name}']}")

    # 2. Redis killed for ten seconds, mid-load.
    say("faults: Redis killed for ten seconds under load")
    t0 = time.time()
    th = threading.Thread(target=lambda: box.__setitem__(
        "redis", hammer(API, keys["stress"], "api", 20, 30)))
    th.start()
    time.sleep(10)
    stop("redis", signal.SIGKILL)
    time.sleep(10)
    start_redis()
    th.join()
    got = box["redis"]
    during = [g for g in got if 10 <= g[2] - t0 < 20]
    res["redis_down"] = {"requests_while_down": len(during),
                         "errors_while_down": sum(1 for g in during if g[1] != 200),
                         "statuses": {str(k): [g[1] for g in got].count(k) for k in {g[1] for g in got}},
                         "p95_ms_while_down": round(pct([g[0] for g in during], 0.95) * 1000, 1)
                         if during else None}
    say(f"faults: redis down: {res['redis_down']}")
    results["faults"] = res
    save()


def phase_collect():
    """The collectors against a simulated Kalshi ten times the size."""
    say("collect: a simulated Kalshi ten times the size, budget 1,200/min")
    schema = "pm_stress_collect"
    env = dict(ENV, PGSCHEMA=schema, REDIS_PREFIX="pmstresscol:",
               PM_STATE_DIR=str(WORK / "collect_state"),
               KALSHI_API_URL=f"http://127.0.0.1:{BIG_MOCK_PORT}/trade-api/v2",
               MARKET_SYNC_MINUTES="5")
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
            cur.execute(f"CREATE SCHEMA {schema}")
    start("bigmock", [sys.executable, "tests/mock_kalshi.py", "--port", str(BIG_MOCK_PORT),
                      "--state", str(WORK / "bigmock.json"), "--scale", "10"])
    for _ in range(100):
        if (WORK / "bigmock.json").exists():
            break
        time.sleep(0.2)
    res = {}
    code, secs = run("collect_discover", [sys.executable, "discover.py", "--once", "--no-prune"],
                     env=env)
    res["discover_once_s"] = round(secs, 1)
    code, secs = run("collect_activity", [sys.executable, "activity.py", "--once"], env=env)
    res["activity_once_s"] = round(secs, 1)
    os.environ["PGSCHEMA"] = schema
    db.close_pool()
    res["markets"] = db.query("SELECT count(*) AS n FROM kalshi_markets")[0]["n"]
    t_live = time.time()
    code, secs = run("collect_live", [sys.executable, "live.py", "--cycles", "3"], env=env,
                     timeout=900)
    res["live_3_cycles_s"] = round(secs, 1)
    cycles = [line for line in (WORK / "collect_live.log").read_text().splitlines()
              if " hot " in line and "requests" in line]
    res["live_cycles"] = cycles[-3:]
    say(f"collect: {res}")
    t_bf = time.time()
    start("collect_backfill", [sys.executable, "backfill.py"], env=env)
    with Watch() as w:
        for _ in range(48):                           # up to four minutes
            time.sleep(5)
            if procs["collect_backfill"].poll() is not None:
                break
    res["backfill_finished"] = procs["collect_backfill"].poll() is not None
    stop("collect_backfill")
    bf_secs = time.time() - t_bf
    res["backfill_seconds"] = round(bf_secs)
    left = db.query("""
        SELECT count(*) AS n FROM kalshi_tracking t JOIN kalshi_markets m USING (ticker)
        WHERE t.candles_fetched_from > to_timestamp(GREATEST(%s,
              EXTRACT(EPOCH FROM date_trunc('minute', COALESCE(m.open_time, t.candles_fetched_from)))))""",
                    (int(time.time()) - 2 * 86400,))[0]["n"]
    res["backfill_markets_left"] = left
    res["candles_stored"] = db.query("SELECT count(*) AS n FROM kalshi_candles")[0]["n"]
    log = json.loads(urllib.request.urlopen(f"http://127.0.0.1:{BIG_MOCK_PORT + 1}/_stats",
                                            timeout=120).read())["requests"]
    times = sorted(e[0] for e in log if e[0] >= t_live)
    res["kalshi_requests"] = len(times)
    res["kalshi_429"] = sum(1 for e in log if e[0] >= t_live and e[2] == 429)
    res["busiest_60s"] = max((sum(1 for x in times[i:i + 5000] if x < t + 60)
                              for i, t in enumerate(times)), default=0)
    res["busiest_1s"] = max((sum(1 for x in times[i:i + 200] if x < t + 1)
                             for i, t in enumerate(times)), default=0)
    bf_times = [x for x in times if x >= t_bf]
    res["backfill_requests_per_min"] = round(len(bf_times) / (bf_secs / 60), 1)
    res.update(w.report())
    say(f"collect: {res}")
    results["collect"] = res
    save()
    stop("bigmock")
    os.environ["PGSCHEMA"] = SCHEMA
    db.close_pool()


def phase_records():
    time.sleep(8)                     # the recorder writes every three seconds
    logged = db.query("SELECT count(*) AS n FROM api_requests WHERE path <> '/health'")[0]["n"]
    results["records"] = {"api_requests_sent": sent["api"], "api_requests_logged": logged,
                          "missing": sent["api"] - logged}
    say(f"records: {results['records']}")
    save()


def phase_store():
    import activity
    from datetime import datetime, timedelta, timezone
    res = {}
    say("store: 8 writers and the scorer racing on the same bookkeeping rows")
    tickers = [f"SM{i:05d}" for i in range(2000)]
    scan = db.query("INSERT INTO kalshi_scans (finished_at) VALUES (now()) RETURNING scan_id")[0]["scan_id"]
    stop_at = time.time() + 60
    errors, writes, scores = [], [0], [0]

    def writer(seed):
        rng = random.Random(seed)
        minute = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        while time.time() < stop_at:
            some = rng.sample(tickers, 100)
            t = minute - timedelta(minutes=rng.randrange(1, 600))
            try:
                store.write_kalshi_fetch([(x, t, 0.5, 0.5, 0.5, 0.5, 1, 1) for x in some],
                                         [(x, t, t + timedelta(minutes=1)) for x in some])
                writes[0] += 1
            except Exception as exc:
                errors.append(type(exc).__name__)

    def scorer():
        nonlocal scan
        while time.time() < stop_at:
            db.execute("INSERT INTO kalshi_observations (scan_id, ticker, volume, asked_at,"
                       " answered_at) SELECT %s, ticker, volume + 1, now(), now()"
                       " FROM kalshi_markets WHERE ticker LIKE 'SM%%'", (scan,))
            try:
                activity.score_scan(scan)
                scores[0] += 1
            except Exception as exc:
                errors.append(type(exc).__name__)
            scan = db.query("INSERT INTO kalshi_scans (finished_at) VALUES (now())"
                            " RETURNING scan_id")[0]["scan_id"]

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(8)]
    threads.append(threading.Thread(target=scorer))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    res["race"] = {"writes": writes[0], "scorings": scores[0],
                   "errors": {e: errors.count(e) for e in set(errors)}}
    say(f"store: {res['race']}")

    say("store: a 100-market backfill write into compressed days")
    day = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(days=12)
    rows = [(f"SM{i:05d}", day + timedelta(minutes=k), 0.5, 0.51, 0.49, 0.5, 3, 100)
            for i in range(100) for k in range(1440)]
    a = time.time()
    try:
        store.write_kalshi_fetch(rows, [(f"SM{i:05d}", day, day + timedelta(days=1))
                                        for i in range(100)])
        res["compressed_write"] = {"rows": len(rows), "seconds": round(time.time() - a, 1),
                                   "error": None}
    except Exception as exc:
        res["compressed_write"] = {"rows": len(rows), "error": f"{type(exc).__name__}: {exc}"[:200]}
    say(f"store: {res['compressed_write']}")

    say("store: prune 150 markets whose candles sit in compressed chunks")
    db.execute("UPDATE kalshi_markets SET close_time = now() - interval '40 days'"
               " WHERE ticker BETWEEN 'SM01850' AND 'SM01999'")
    before = db.query("SELECT count(*) AS n FROM kalshi_candles WHERE ticker BETWEEN"
                      " 'SM01850' AND 'SM01999'")[0]["n"]
    a = time.time()
    try:
        res["prune"] = {"candles_to_delete": before, "deleted": store.prune(30),
                        "seconds": round(time.time() - a, 1), "error": None}
    except Exception as exc:
        res["prune"] = {"candles_to_delete": before, "error": f"{type(exc).__name__}: {exc}"[:200]}
    say(f"store: {res['prune']}")
    results["store"] = res
    save()


PHASES = ["data", "reads", "load", "limits", "pushes", "viewer", "faults", "collect", "records",
          "store"]


def main():
    wanted = [a for a in sys.argv[1:] if a in PHASES] or PHASES
    WORK.mkdir(parents=True, exist_ok=True)
    if RESULTS.exists():
        results.update(json.loads(RESULTS.read_text()))
    start_redis()
    try:
        if "data" in wanted:
            phase_data()
        keys = make_keys()
        for name in ("api_requests",):
            db.execute(f"TRUNCATE {name}")
        need_api = any(p in wanted for p in ("reads", "load", "limits", "pushes", "viewer", "faults"))
        if need_api:
            start_api()
        for name in wanted:
            if name in ("data", "collect", "records", "store"):
                continue
            globals()[f"phase_{name}"](keys)
        if "records" in wanted:
            phase_records()
        if "store" in wanted:
            phase_store()
        stop("viewer")
        stop("api")
        if "collect" in wanted:
            phase_collect()
    finally:
        for name in list(procs):
            stop(name)
    if "--keep" not in sys.argv:
        # Its data is millions of candles with background jobs of their own.
        for schema in (SCHEMA, "pm_stress_collect"):
            db.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    results["finished"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save()
    say("done; results in " + str(RESULTS))


if __name__ == "__main__":
    main()

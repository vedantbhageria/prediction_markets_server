"""Polymarket collection end to end, against a simulated Polymarket, on the server.

    .venv/bin/python tests/poly_test.py              # every phase, ~35 min
    .venv/bin/python tests/poly_test.py api load     # just these (after discover and live)
    .venv/bin/python tests/poly_test.py --keep       # leave its schema behind

Runs in its own schema (pm_polytest), its own Redis (port 6392) and against
tests/mock_poly.py, so neither the real data nor the real Polymarket is
touched. The simulated venue trades about 900 times a minute -- as fast as the
real one -- and a feed read goes back at most 2,000 trades (POLY_FEED_MOST) so a
gap is only two minutes away; a market's trades are stored 5,000 at a time
(POLY_MARKET_BATCH), so the busiest are read in many batches. Its trades are
known in advance, so every stored
trade and every candle is checked against them.

Phases (numbers in /tmp/pmpoly/results.json):

  discover   one discovery pass: every market listed and tracked
  live       trades.py, aggregate.py, discover.py and backfill.py --venue poly
             together; then every trade and candle checked
  gap        the venue unreachable for 4 minutes (twice the feed's reach):
             the gap is caught up market by market, then everything checked
  restart    trades.py and aggregate.py killed (-9) for 3 minutes, restarted
  throttle   most requests refused (429) for a minute: the budget halves,
             nothing is lost
  api        every API read and the push command, for Polymarket; pmapi; the
             viewer's status page
  load       requests a minute at rest, candle lag, 20 API clients at once
  prune      markets closed past retention go, with their trades and candles
  kalshi     Kalshi's own tests, to show it is unchanged
"""

import json
import os
import random
import shutil
import signal
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORK = Path("/tmp/pmpoly")
SCHEMA = "pm_polytest"
REDIS_PORT, MOCK_PORT, API_PORT, VIEWER_PORT = 6392, 18400, 18410, 18411
MOCK = f"http://127.0.0.1:{MOCK_PORT}"
API = f"http://127.0.0.1:{API_PORT}"
T0 = int(time.time())

ENV = dict(os.environ, PGSCHEMA=SCHEMA, REDIS_URL=f"redis://127.0.0.1:{REDIS_PORT}/0",
           REDIS_PREFIX="pmpoly:", PM_STATE_DIR=str(WORK / "state"), LIVE_VENUES="poly",
           KALSHI_API_URL="http://127.0.0.1:9/trade-api/v2",
           POLY_GAMMA_URL=MOCK, POLY_DATA_URL=MOCK,
           POLY_FEED_MOST="2000", POLY_MARKET_BATCH="5000", POLY_LIST_LAG_SECONDS="60",
           POLY_FEED_LAG_SECONDS="25", POLY_FEED_SECONDS="5",
           POLY_REQUESTS_PER_MIN="600", POLY_MAX_PER_SECOND="15",
           BACKFILL_DAYS="1", MARKET_SYNC_MINUTES="3", NET_PROBE_SECONDS="5",
           API_RATE_PER_MIN="1000000", API_BACKFILL_PER_MIN="100000",
           VIEWER_USER="", VIEWER_PASSWORD_HASH="", VPN_PROXY="", PYTHONUNBUFFERED="1")
os.environ.update({k: ENV[k] for k in ENV if k.startswith(("PG", "REDIS", "PM_", "POLY",
                                                           "BACKFILL", "LIVE_", "KALSHI_"))})
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import redis                  # noqa: E402
import requests               # noqa: E402

import db                     # noqa: E402
import mock_poly              # noqa: E402
import poly                   # noqa: E402
import store                  # noqa: E402

RESULTS = WORK / "results.json"
procs, results = {}, {}
universe = mock_poly.Universe(T0)


def say(text):
    print(f"[{(time.time() - T0) / 60:5.1f} min] {text}", flush=True)


def save():
    RESULTS.write_text(json.dumps(results, indent=1, default=str))


def start(name, args):
    log = open(WORK / f"{name}.log", "a")
    procs[name] = subprocess.Popen(args, cwd=ROOT, env=dict(ENV, PGAPPNAME=f"pmpoly-{name}"),
                                   stdout=log, stderr=subprocess.STDOUT, start_new_session=True)


def stop(name, sig=signal.SIGTERM):
    p = procs.pop(name, None)
    if p and p.poll() is None:
        try:
            os.killpg(p.pid, sig)
        except ProcessLookupError:
            p.send_signal(sig)
        try:
            p.wait(timeout=90)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL)
            p.wait(timeout=10)


def run(name, args, timeout=3600):
    a = time.time()
    with open(WORK / f"{name}.log", "a") as log:
        code = subprocess.run(args, cwd=ROOT, env=dict(ENV, PGAPPNAME=f"pmpoly-{name}"),
                              stdout=log, stderr=subprocess.STDOUT, timeout=timeout).returncode
    return code, round(time.time() - a, 1)


def _redis_up():
    try:
        return redis.Redis(port=REDIS_PORT).ping()
    except redis.RedisError:
        return False


def wait_http(url, seconds=120):
    for _ in range(seconds * 2):
        try:
            if requests.get(url, timeout=3).status_code < 500:
                return
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise SystemExit(f"{url} did not come up")


def mode(m):
    requests.get(f"{MOCK}/_control", params={"mode": m}, timeout=10)


def log_lines(name, needle):
    path = WORK / f"{name}.log"
    return [l for l in path.read_text().splitlines() if needle in l] if path.exists() else []


# ---------------------------------------------------------------------------
# Checking against the simulated venue
# ---------------------------------------------------------------------------

def expected_candles(trades, yes):
    """{minute: (o, h, l, c, volume, notional, n)} as aggregate.py should make them."""
    by = {}
    for t in trades:
        r = poly.row(t)
        p = r[5] if r[3] == yes else 1 - r[5]
        by.setdefault(int(r[1].timestamp()) // 60 * 60, []).append((r[1].timestamp(), r[2], round(p, 6), r[6], r[6] * r[5]))
    out = {}
    for m, xs in by.items():
        xs.sort()
        ps = [x[2] for x in xs]
        out[m] = (ps[0], max(ps), min(ps), ps[-1], sum(x[3] for x in xs),
                  sum(x[4] for x in xs), len(xs))
    return out


def check(label, sample=None):
    """Every tracked market whose history is complete: stored trades against
    the venue's, over the stretch its coverage claims; candles against the
    trades."""
    drain_candles()
    rows = db.query(f"""
        SELECT t.condition_id, m.yes_token_id,
               EXTRACT(EPOCH FROM t.history_from) AS h, EXTRACT(EPOCH FROM t.trades_from) AS f,
               EXTRACT(EPOCH FROM poly_covered_to(t.trades_to)) AS c
        FROM poly_tracking t JOIN poly_markets m USING (condition_id)
        WHERE {poly.HISTORY_DONE}""")
    if sample:
        rows = random.Random(1).sample(rows, min(sample, len(rows)))
    res = {"markets": len(rows), "trades_checked": 0, "missing": 0, "extra": 0,
           "candles_checked": 0, "candles_wrong": 0, "bad_markets": []}

    def one(r):
        cid, lo, hi = r["condition_id"], float(r["f"]), float(r["c"])
        if float(r["f"]) <= float(r["h"]):
            lo = float(r["h"])
        m = universe.by_cid[cid]
        truth = universe.truth(m, int(lo) + 1, int(hi))         # whole seconds inside
        want = {poly.row(t)[2]: t for t in truth}
        got = {x["trade_id"] for x in db.query(
            "SELECT trade_id FROM poly_trades WHERE condition_id = %s AND ts >= to_timestamp(%s)"
            " AND ts < to_timestamp(%s)", (cid, int(lo) + 1, int(hi)))}
        missing, extra = len(set(want) - got), len(got - set(want))
        # Candles for minutes wholly inside the stretch.
        exp = expected_candles(truth, r["yes_token_id"])
        a, b = (int(lo) // 60 + 1) * 60, int(hi) // 60 * 60
        stored = {int(x["t"]): x for x in db.query(
            "SELECT EXTRACT(EPOCH FROM ts)::bigint AS t, open, high, low, close, volume, notional,"
            " trades FROM poly_candles WHERE condition_id = %s"
            " AND ts >= to_timestamp(%s) AND ts < to_timestamp(%s)", (cid, a, b))}
        wrong = 0
        for minute in set(k for k in exp if a <= k < b) | set(stored):
            e, s = exp.get(minute), stored.get(minute)
            if not e or not s or any(abs(float(s[k]) - v) > 1e-4 * max(1, abs(v)) for k, v in
                                     zip(("open", "high", "low", "close", "volume", "notional",
                                          "trades"), e)):
                wrong += 1
        checked = len(set(k for k in exp if a <= k < b) | set(stored))
        return cid, len(want), missing, extra, checked, wrong

    with ThreadPoolExecutor(8) as pool:
        for cid, n, missing, extra, checked, wrong in pool.map(one, rows):
            res["trades_checked"] += n
            res["missing"] += missing
            res["extra"] += extra
            res["candles_checked"] += checked
            res["candles_wrong"] += wrong
            if missing or extra or wrong:
                res["bad_markets"].append((cid[:12], universe.by_cid[cid]["kind"], missing, extra,
                                           wrong))
    res["bad_markets"] = res["bad_markets"][:10]
    res["ok"] = not (res["missing"] or res["extra"] or res["candles_wrong"])
    say(f"check {label}: {res}")
    return res


def drain_candles(seconds=180):
    """Wait for aggregate.py to make every marked minute of a listed market."""
    for _ in range(seconds):
        n = db.query("SELECT count(*) AS n FROM poly_dirty d WHERE EXISTS"
                     " (SELECT 1 FROM poly_markets m WHERE m.condition_id = d.condition_id)")[0]["n"]
        if n == 0:
            return
        time.sleep(1)
    say(f"warning: {n} minutes still waiting for candles")


def behind():
    return db.query(f"SELECT count(*) AS n FROM poly_tracking t WHERE {poly.HISTORY_DONE}"
                    f" AND {poly.BEHIND}")[0]["n"]


def history_left():
    return db.query(f"SELECT count(*) AS n FROM poly_tracking t WHERE NOT {poly.HISTORY_DONE}")[0]["n"]


def wait_until(test, seconds, every=5):
    a = time.time()
    while time.time() - a < seconds:
        if test():
            return round(time.time() - a)
        time.sleep(every)
    return None


# ---------------------------------------------------------------------------
# Phases
# ---------------------------------------------------------------------------

def phase_discover():
    code, secs = run("discover_once", [sys.executable, "discover.py", "--once", "--no-prune"])
    listed = [m for m in universe.markets if m["start"] <= time.time()]
    r = db.query("SELECT (SELECT count(*) FROM poly_markets) AS markets,"
                 " (SELECT count(*) FROM poly_tracking) AS tracked,"
                 f" (SELECT count(*) FROM poly_tracking t WHERE {poly.HISTORY_DONE}) AS done")[0]
    never = sum(1 for m in listed if m["kind"] == "never")
    res = {"exit": code, "seconds": secs, "venue_markets": len(listed), **r,
           "never_traded": never, "ok": code == 0 and r["markets"] == len(listed)
           and r["tracked"] == len(listed)}
    say(f"discover: {res}")
    results["discover"] = res


def phase_live():
    start("trades", [sys.executable, "trades.py"])
    start("aggregate", [sys.executable, "aggregate.py"])
    start("discover", [sys.executable, "discover.py", "--no-prune"])
    a = time.time()
    code, secs = run("backfill", [sys.executable, "backfill.py", "--venue", "poly"], timeout=1800)
    res = {"backfill_exit": code, "backfill_seconds": secs, "history_left": history_left()}
    stats = requests.get(f"{MOCK}/_stats", timeout=60).json()
    res["backfill_requests"] = sum(1 for s in stats if s["at"] >= a and s["path"] == "/v2/trades")
    # Every market read whole: the busiest ("huge") ones in several batches.
    res["huge_markets_whole"] = db.query(
        "SELECT count(*) FILTER (WHERE trades_from <= history_from) AS whole, count(*) AS n"
        " FROM poly_tracking WHERE condition_id = ANY(%s)",
        ([m["cid"] for m in universe.markets if m["kind"] == "huge"],))[0]
    say(f"live: backfill {res}; letting the feed run 4 minutes with new markets opening")
    time.sleep(240)
    res["behind"] = behind()
    res["check"] = check("live")
    res["feed_gaps_logged"] = len(log_lines("trades", "feed gap"))
    results["live"] = res
    save()


def phase_gap():
    say("gap: the venue drops every connection for 4 minutes")
    mode("down")
    time.sleep(240)
    mode("normal")
    a = time.time()
    gaps_before = len(log_lines("trades", "feed gap"))
    took = wait_until(lambda: len(log_lines("trades", "feed gap")) > gaps_before, 120, 2)
    # The catch-up waits for discovery to see which markets moved (a pass is
    # three minutes here), then reads them.
    recovered = wait_until(lambda: behind() == 0 and len(log_lines("trades", "caught up")) > 0,
                           900, 10)
    time.sleep(60)
    res = {"gap_noticed_after_s": took, "caught_up_after_s": round(time.time() - a) if recovered is not None else None,
           "catch_up_rounds": log_lines("trades", "caught up")[-3:], "check": check("gap")}
    # The check compares what each market's coverage claims; a market never
    # caught up claims less, so it is checked here: every market caught up.
    res["behind_after"] = behind()
    res["ok"] = recovered is not None and res["behind_after"] == 0 and res["check"]["ok"]
    if not res["ok"]:
        say(f"FAIL gap: caught up {recovered is not None}, {res['behind_after']} markets still behind")
    results["gap"] = res
    save()


def phase_restart():
    say("restart: trades.py and aggregate.py killed -9 for 3 minutes")
    stop("trades", signal.SIGKILL)
    stop("aggregate", signal.SIGKILL)
    time.sleep(180)
    start("trades", [sys.executable, "trades.py"])
    start("aggregate", [sys.executable, "aggregate.py"])
    a = time.time()
    time.sleep(30)
    recovered = wait_until(lambda: behind() == 0, 900, 10)
    time.sleep(60)
    res = {"recovered_after_s": round(time.time() - a), "check": check("restart"),
           "behind_after": behind()}
    res["ok"] = recovered is not None and res["behind_after"] == 0 and res["check"]["ok"]
    if not res["ok"]:
        say(f"FAIL restart: {res['behind_after']} markets still behind")
    results["restart"] = res
    save()


def phase_throttle():
    say("throttle: 80% of requests refused (429) for a minute")
    r = redis.Redis(port=REDIS_PORT, decode_responses=True)
    mode("429")
    time.sleep(60)
    factor = float(r.get("pmpoly:poly:factor") or 1)
    mode("normal")
    wait_until(lambda: behind() == 0, 600, 10)
    time.sleep(90)
    results["throttle"] = {"budget_factor_during": factor, "check": check("throttle")}
    save()


def make_key():
    from api import keys
    db.execute("DELETE FROM api_keys WHERE name = 'polytest'")
    return keys.create("polytest", "full", None)


def phase_api():
    key = make_key()
    start("api", [sys.executable, "-m", "uvicorn", "api.main:app", "--host", "127.0.0.1",
                  "--port", str(API_PORT), "--workers", "2"])
    start("viewer", [sys.executable, "viewer.py", "--host", "127.0.0.1", "--port", str(VIEWER_PORT)])
    wait_http(API + "/health")
    wait_http(f"http://127.0.0.1:{VIEWER_PORT}/")
    s = requests.Session()
    s.headers["X-API-Key"] = key
    res, ok = {}, True

    def get(path, **params):
        r = s.get(API + path, params=params, timeout=120)
        return r

    hot = [m for m in universe.markets if m["kind"] == "hot"][0]["cid"]
    warm = next(m["cid"] for m in universe.markets if m["kind"] == "warm"
                and db.query(f"SELECT 1 FROM poly_tracking t WHERE condition_id = %s AND {poly.HISTORY_DONE}",
                             (m["cid"],)))
    r = get("/v1/markets", venue="poly", limit=10000)
    res["markets_poly"] = len(r.json()["markets"]) if r.ok else r.status_code
    r = get("/v1/markets", venue="all", limit=50, sort="-activity")
    res["markets_all_status"] = r.status_code
    r = get("/v1/markets", venue="poly", tag="group-3", status="open", limit=10000)
    res["markets_tag_filter"] = len(r.json()["markets"]) if r.ok else r.status_code
    r = get(f"/v1/markets/{hot}")
    res["market_detail_fields"] = sorted(r.json()) if r.ok else r.status_code
    a = int(time.time()) - 3 * 3600
    raw = get(f"/v1/markets/{hot}/candles", interval="1m", start=a, fill="raw").json()
    stored = db.query("SELECT count(*) AS n FROM poly_candles WHERE condition_id = %s"
                      " AND ts >= to_timestamp(%s) AND ts < now()", (hot, a))[0]["n"]
    res["candles_raw"] = {"rows": len(raw["candles"]), "stored": stored}
    ok &= abs(len(raw["candles"]) - stored) <= 1
    fwd = get(f"/v1/markets/{warm}/candles", interval="1m", start=a, fill="forward").json()
    ts = [c["ts"] for c in fwd["candles"]]
    res["candles_forward"] = {"rows": len(ts), "filled": sum(c["filled"] for c in fwd["candles"]),
                              "coverage": fwd["coverage"]}
    r = get(f"/v1/markets/{hot}/candles", interval="1h", start=int(time.time()) - 30 * 3600)
    res["candles_1h_rows"] = len(r.json()["candles"]) if r.ok else r.status_code
    r = get("/v1/candles", tickers=f"{hot},{warm}", interval="5m", start=a)
    res["candles_mixed_status"] = r.status_code
    # Trades, page by page, against what is stored.
    n, token = 0, None
    while True:
        r = get(f"/v1/markets/{hot}/trades", start=a, limit=500, page_token=token).json()
        n += len(r["trades"])
        token = r.get("next_page_token")
        if not token:
            break
    want = db.query("SELECT count(*) AS n FROM poly_trades WHERE condition_id = %s"
                    " AND ts >= to_timestamp(%s)", (hot, a))[0]["n"]
    res["trades_paged"] = {"returned": n, "stored": want}
    ok &= n == want
    r = get("/v1/latest", tickers=f"{hot},{warm}")
    res["latest_rows"] = len(r.json()["latest"]) if r.ok else r.status_code
    r = s.post(API + "/v1/backfill", json={"ticker": warm}, timeout=60)
    job = r.json()
    for _ in range(120):
        j = get(f"/v1/backfill/{job['job_id']}").json()
        if j["status"] in ("done", "failed"):
            break
        time.sleep(1)
    res["push"] = {"status": j["status"], "new_trades": j["candles"], "error": j["error"]}
    ok &= j["status"] == "done"
    st = get("/v1/status").json()
    res["status_poly"] = st.get("poly")
    ok &= bool(st.get("poly", {}).get("feed"))
    # The client, as a notebook would use it.
    import pmapi
    pm = pmapi.Client(API, key=key)
    df = pm.markets(venue="poly", max_rows=100)
    c = pm.candles(hot, "5m", start=a)
    t = pm.trades(hot, start=a)
    res["pmapi"] = {"markets": df.shape, "candles": c.shape, "trades": t.shape,
                    "trade_columns": list(t.columns)}
    v = requests.get(f"http://127.0.0.1:{VIEWER_PORT}/status?refresh=off", timeout=120)
    res["viewer_status"] = {"code": v.status_code, "has_polymarket": "Polymarket" in v.text}
    ok &= v.status_code == 200 and "Polymarket" in v.text
    res["ok"] = bool(ok)
    say(f"api: {res}")
    results["api"] = res
    save()


def phase_load():
    a = time.time()
    time.sleep(120)
    stats = requests.get(f"{MOCK}/_stats", timeout=60).json()
    recent = [x for x in stats if x["at"] >= a]
    res = {"requests_per_min_at_rest": round(len(recent) / 2, 1),
           "by_path": {p: sum(1 for x in recent if x["path"] == p) for p in {x["path"] for x in recent}}}
    lag = db.query("SELECT EXTRACT(EPOCH FROM now() - max(ts)) AS s FROM poly_candles")[0]["s"]
    feed = db.query("SELECT EXTRACT(EPOCH FROM now() - covered_to) AS s FROM poly_feed")[0]["s"]
    res["newest_candle_age_s"] = round(lag)
    res["feed_behind_s"] = round(feed)
    key = make_key()
    cids = [m["cid"] for m in universe.markets if m["kind"] in ("hot", "warm")][:200]
    lat, codes = [], []
    lock = threading.Lock()

    def client(seed):
        rng, s = random.Random(seed), requests.Session()
        s.headers["X-API-Key"] = key
        end = time.time() + 30
        while time.time() < end:
            t = time.time()
            r = s.get(f"{API}/v1/markets/{rng.choice(cids)}/candles",
                      params={"interval": rng.choice(["1m", "5m", "1h"]), "start": int(t) - 6 * 3600},
                      timeout=60)
            with lock:
                lat.append(time.time() - t)
                codes.append(r.status_code)

    with ThreadPoolExecutor(20) as pool:
        list(pool.map(client, range(20)))
    lat.sort()
    res["api_20_clients"] = {"requests": len(lat), "per_s": round(len(lat) / 30, 1),
                             "p50_ms": round(lat[len(lat) // 2] * 1000),
                             "p95_ms": round(lat[int(len(lat) * 0.95)] * 1000),
                             "errors": sum(1 for c in codes if c >= 400)}
    res["trades_stored"] = db.query("SELECT count(*) AS n FROM poly_trades")[0]["n"]
    res["candles_stored"] = db.query("SELECT count(*) AS n FROM poly_candles")[0]["n"]
    say(f"load: {res}")
    results["load"] = res
    save()


def phase_prune():
    victims = [x["condition_id"] for x in db.query(
        "SELECT condition_id FROM poly_markets WHERE closed IS TRUE ORDER BY condition_id LIMIT 20")]
    db.execute("UPDATE poly_markets SET end_date = now() - interval '40 days'"
               " WHERE condition_id = ANY(%s)", (victims,))
    before = db.query("SELECT count(*) AS n FROM poly_trades WHERE condition_id = ANY(%s)", (victims,))[0]["n"]
    counts = store.prune(30)
    left = db.query("""SELECT (SELECT count(*) FROM poly_trades WHERE condition_id = ANY(%(v)s)) AS trades,
                              (SELECT count(*) FROM poly_candles WHERE condition_id = ANY(%(v)s)) AS candles,
                              (SELECT count(*) FROM poly_tracking WHERE condition_id = ANY(%(v)s)) AS tracking,
                              (SELECT count(*) FROM poly_markets WHERE condition_id = ANY(%(v)s)) AS markets""",
                    {"v": victims})[0]
    res = {"markets": len(victims), "trades_before": before, "pruned": counts, "left": left,
           "ok": not any(left.values())}
    say(f"prune: {res}")
    results["prune"] = res
    save()


def phase_kalshi():
    res = {}
    env = {k: v for k, v in os.environ.items() if not k.startswith(("PGSCHEMA", "REDIS", "PM_", "POLY",
                                                                     "BACKFILL", "LIVE_", "KALSHI_"))}
    for test in ("activity_test.py", "ratelimit_test.py", "api_test.py"):
        a = time.time()
        with open(WORK / f"kalshi_{test}.log", "w") as log:
            code = subprocess.run([sys.executable, f"tests/{test}"], cwd=ROOT, env=env,
                                  stdout=log, stderr=subprocess.STDOUT, timeout=1800).returncode
        res[test] = {"exit": code, "seconds": round(time.time() - a)}
        say(f"kalshi: {test} {res[test]}")
    results["kalshi"] = res
    save()


PHASES = ["discover", "live", "gap", "restart", "throttle", "api", "load", "prune", "kalshi"]


def main():
    wanted = [a for a in sys.argv[1:] if a in PHASES] or PHASES
    WORK.mkdir(parents=True, exist_ok=True)
    (WORK / "state").mkdir(exist_ok=True)
    start("redis", ["redis-server", "--port", str(REDIS_PORT), "--save", "", "--appendonly", "no"])
    start("mock", [sys.executable, "tests/mock_poly.py", "--port", str(MOCK_PORT), "--start", str(T0)])
    wait_http(f"{MOCK}/_stats")
    if "discover" in wanted:
        # A fresh store needs fresh discovery state: a saved "closed markets
        # imported up to" from an earlier run would skip them.
        shutil.rmtree(WORK / "state", ignore_errors=True)
        (WORK / "state").mkdir()
        # The same goes for Redis: a test Redis left running by an earlier run
        # still holds its budget, cut to a tenth by that run's throttle phase.
        wait_until(lambda: _redis_up(), 30, 1)
        redis.Redis(port=REDIS_PORT).flushdb()
        with db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
                cur.execute(f"CREATE SCHEMA {SCHEMA}")
        db.close_pool()
        store.ensure_schema()
    try:
        for name in wanted:
            say(f"--- {name}")
            globals()[f"phase_{name}"]()
    finally:
        for name in list(procs):
            stop(name)
    results["finished"] = time.strftime("%Y-%m-%d %H:%M:%S")
    save()
    if "--keep" not in sys.argv:
        db.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    say("done; results in " + str(RESULTS))


if __name__ == "__main__":
    main()

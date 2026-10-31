"""The HTTP API end to end, against the mock Kalshi. Run on the server:

    .venv/bin/python tests/api_test.py          # about five minutes

Builds its own schema (pm_api_test) and Redis prefix, lets discover.py and
activity.py read the mock once, starts the API on port 18100 and checks:

  1  keys: none, wrong, revoked, read-only, and the per-key request limit
  2  errors: every refusal has the documented status and code
  3  the push command: backfills of busy, quiet, closed and never-traded
     markets store exactly the mock's true trades, and move coverage
  4  candles: raw = the true traded minutes; forward fill = a fill of the
     truth worked out here, at 1m, 5m, 1h and 1d, whole-life and mid-life
  5  paging: small pages joined = one big page; json = csv = parquet
  6  many tickers in one request = each one asked alone
  7  markets, events, series, categories, latest, hot and status agree
     with the database
  8  a duplicate push is refused while one is running, and a job cut short
     by the API dying is picked up again when it restarts
  9  pmapi.py returns the same data as DataFrames
"""

import io
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORK = Path("/tmp/pmapitest")
MOCK_PORT, API_PORT = 18090, 18100
API = f"http://127.0.0.1:{API_PORT}"

ENV = dict(os.environ,
           PGSCHEMA="pm_api_test", REDIS_PREFIX="pmapitest:", PM_STATE_DIR=str(WORK / "state"),
           KALSHI_API_URL=f"http://127.0.0.1:{MOCK_PORT}/trade-api/v2", LIVE_VENUES="kalshi",
           KALSHI_REQUESTS_PER_MIN="600", BACKFILL_DAYS="2", API_RATE_PER_MIN="100000",
           PYTHONUNBUFFERED="1")
os.environ.update({k: ENV[k] for k in ("PGSCHEMA", "REDIS_PREFIX", "PM_STATE_DIR",
                                       "KALSHI_API_URL", "BACKFILL_DAYS")})
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import pandas as pd           # noqa: E402
import requests               # noqa: E402

import db                     # noqa: E402
import mock_kalshi            # noqa: E402
import ratelimit              # noqa: E402

START = time.time()
procs = {}
failures = []


def say(text):
    print(f"[{(time.time() - START) / 60:5.1f} min] {text}", flush=True)


def check(ok, what):
    if not ok:
        failures.append(what)
        say(f"FAIL {what}")
    return ok


def start(name, *args):
    log = open(WORK / f"{name}.log", "a")
    log.write(f"\n===== start {time.strftime('%H:%M:%S')}\n")
    log.flush()
    procs[name] = subprocess.Popen([sys.executable, *args], cwd=ROOT, env=ENV,
                                   stdout=log, stderr=subprocess.STDOUT)


def stop(name, sig=signal.SIGTERM):
    p = procs.pop(name, None)
    if p and p.poll() is None:
        p.send_signal(sig)
        try:
            p.wait(timeout=60)
        except subprocess.TimeoutExpired:
            p.kill()


def run(*args):
    out = subprocess.run([sys.executable, *args], cwd=ROOT, env=ENV, timeout=900,
                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    (WORK / "run.log").open("a").write(out.stdout)
    return out.stdout


def mock_mode(m):
    urllib.request.urlopen(f"http://127.0.0.1:{MOCK_PORT + 1}/_control?mode={m}", timeout=30)


def start_api():
    start("api", "-m", "uvicorn", "api.main:app", "--host", "127.0.0.1", "--port",
          str(API_PORT))
    for _ in range(120):
        try:
            if requests.get(API + "/health", timeout=2).status_code == 200:
                return
        except requests.ConnectionError:
            pass
        time.sleep(0.5)
    raise SystemExit("the API did not come up; see " + str(WORK / "api.log"))


class Api:
    def __init__(self, key):
        self.s = requests.Session()
        if key:
            self.s.headers["X-API-Key"] = key

    def get(self, path, **params):
        return self.s.get(API + path, params=params, timeout=120)

    def json(self, path, **params):
        r = self.get(path, **params)
        assert r.status_code == 200, (path, params, r.status_code, r.text[:300])
        return r.json()

    def post(self, path, body):
        return self.s.post(API + path, json=body, timeout=60)


def error_code(r):
    try:
        return r.status_code, r.json()["error"]["code"]
    except Exception:
        return r.status_code, None


# ---------------------------------------------------------------------------
# The truth, from the mock's own trade generator
# ---------------------------------------------------------------------------

def floor(t, step):
    return int(t // step * step)


def ceil(t, step):
    return int(-(-t // step) * step)


def epoch(text):
    return pd.Timestamp(text).timestamp() if text else None


def expected(m, step, a, b, cov, stored):
    """What the API should give for [a, b) with forward fill, from the true
    trades -- only those in `stored` = [lo, hi), what the backfill wrote."""
    now = time.time()
    lo, hi = stored
    truth = {}
    for minute in range(lo, hi, 60):
        c = mock_kalshi.candle(m, minute)
        if c:
            truth[minute] = c
    cf, ct = epoch(cov["from"]), epoch(cov["to"])
    fill_lo, fill_hi = floor(cf, step), min(ct, m["close"])
    a0, b0 = floor(max(a, m["open"]), step), ceil(min(b, m["close"], now), step)
    last = None
    for minute in sorted(truth):
        if cf <= minute < a0:
            last = truth[minute][3]
    rows = []
    for t in range(a0, b0, step):
        got = [truth[x] for x in range(t, t + step, 60) if x in truth]
        inside = fill_lo <= t < fill_hi
        if got:
            rows.append((t, got[0][0], max(g[1] for g in got), min(g[2] for g in got),
                         got[-1][3], sum(g[4] for g in got), False))
            last = got[-1][3] if inside else None
        elif last is not None and inside:
            rows.append((t, last, last, last, last, 0, True))
    return rows


def as_tuples(candles):
    return [(int(epoch(c["ts"])), round(c["open"], 6), round(c["high"], 6), round(c["low"], 6),
             round(c["close"], 6), round(c["volume"], 4), c["filled"]) for c in candles]


def rounded(rows):
    return [(t, round(o, 6), round(h, 6), round(lw, 6), round(c, 6), round(v, 4), f)
            for t, o, h, lw, c, v, f in rows]


def all_pages(api, path, key="candles", **params):
    out, token = [], None
    while True:
        body = api.json(path, page_token=token, **params) if token else api.json(path, **params)
        out += body[key]
        token = body["next_page_token"]
        if not token:
            return out


# ---------------------------------------------------------------------------

def setup():
    subprocess.run(["rm", "-rf", str(WORK)])
    (WORK / "state").mkdir(parents=True)
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS pm_api_test CASCADE")
            cur.execute("CREATE SCHEMA pm_api_test")
    r = ratelimit.client()
    stale = list(r.scan_iter("pmapitest:*"))
    if stale:
        r.delete(*stale)
    start("mock", "tests/mock_kalshi.py", "--port", str(MOCK_PORT), "--state",
          str(WORK / "mock.json"))
    for _ in range(50):
        if (WORK / "mock.json").exists():
            break
        time.sleep(0.2)
    t0 = json.loads((WORK / "mock.json").read_text())["t0"]
    say("mock Kalshi up; one discovery pass and one scoring")
    run("discover.py", "--once", "--no-prune")
    run("activity.py", "--once")
    start_api()
    keys = {}
    for name, extra in (("full", []), ("reader", ["--read-only"]), ("slow", ["--rate", "5"]),
                        ("gone", [])):
        keys[name] = run("-m", "api.keys", "create", name, *extra).strip().splitlines()[-1]
    run("-m", "api.keys", "revoke", "gone")
    say("API up with four keys")
    return t0, keys


def test_keys(keys):
    anon, full = Api(None), Api(keys["full"])
    check(requests.get(API + "/health").status_code == 200, "1 /health answers without a key")
    check(error_code(anon.get("/v1/markets")) == (401, "missing_key"), "1 no key: 401")
    check(error_code(Api("pm_wrong").get("/v1/markets")) == (401, "invalid_key"), "1 wrong key")
    check(error_code(Api(keys["gone"]).get("/v1/markets")) == (401, "invalid_key"),
          "1 revoked key")
    bearer = requests.get(API + "/v1/markets", timeout=30,
                          headers={"Authorization": f"Bearer {keys['full']}"})
    check(bearer.status_code == 200, "1 Authorization: Bearer works too")
    reader = Api(keys["reader"])
    check(reader.get("/v1/markets").status_code == 200, "1 read-only key reads")
    t = full.json("/v1/markets", limit=1)["markets"][0]["ticker"]
    check(error_code(reader.post("/v1/backfill", {"ticker": t})) == (403, "read_only_key"),
          "1 read-only key cannot backfill")
    slow = Api(keys["slow"])
    # Start at the top of a minute, so all six land in one window.
    time.sleep(61 - time.time() % 60)
    got = [slow.get("/v1/markets", limit=1) for _ in range(6)]
    check([r.status_code for r in got] == [200] * 5 + [429], "1 the sixth request in a minute"
          f" is refused: {[r.status_code for r in got]}")
    check(got[4].headers.get("X-RateLimit-Remaining") == "0" and
          got[0].headers.get("X-RateLimit-Limit") == "5", "1 rate-limit headers")
    check("Retry-After" in got[5].headers, "1 429 says when to retry")
    say("keys checked")


def test_errors(api, ticker):
    cases = [
        (api.get("/v1/markets/NO-SUCH-MARKET"), (404, "not_found"), "unknown market"),
        (api.get(f"/v1/markets/{ticker}/candles", interval="7m"), (400, "invalid_parameter"),
         "bad interval"),
        (api.get(f"/v1/markets/{ticker}/candles", start="yesterday"), (400, "invalid_parameter"),
         "bad time"),
        (api.get(f"/v1/markets/{ticker}/candles", start="2026-10-02", end="2026-10-01"),
         (400, "invalid_parameter"), "start after end"),
        (api.get(f"/v1/markets/{ticker}/candles", page_token="garbage"),
         (400, "invalid_page_token"), "bad page token"),
        (api.get("/v1/markets", sort="colour"), (400, "invalid_parameter"), "bad sort"),
        (api.get("/v1/markets", limit=0), (400, "invalid_parameter"), "limit 0"),
        (api.get("/v1/candles", tickers=",".join(f"T{i}" for i in range(101))),
         (400, "invalid_parameter"), "101 tickers"),
        (api.get("/v1/candles", tickers=f"{ticker},NOPE"), (404, "not_found"),
         "one unknown ticker of two"),
        (api.post("/v1/backfill", {"ticker": "NO-SUCH-MARKET"}), (404, "not_found"),
         "backfill of an unknown market"),
        (api.post("/v1/backfill", {"tick": ticker}), (400, "invalid_parameter"),
         "backfill without a ticker"),
        (api.get("/v1/backfill/999999"), (404, "not_found"), "unknown job"),
        (api.get("/v1/nothing-here"), (404, "not_found"), "unknown path"),
    ]
    for r, want, what in cases:
        check(error_code(r) == want, f"2 {what}: wanted {want}, got {error_code(r)}")
    say("errors checked")


def push(api, tickers):
    started = {}
    for t in tickers:
        r = api.post("/v1/backfill", {"ticker": t})
        if check(r.status_code == 202, f"3 backfill of {t} accepted: {r.status_code} {r.text[:200]}"):
            started[t] = r.json()["job_id"]
    deadline = time.time() + 600
    done = {}
    while len(done) < len(started) and time.time() < deadline:
        for t, job_id in started.items():
            if t not in done:
                j = api.json(f"/v1/backfill/{job_id}")
                if j["status"] in ("done", "failed"):
                    done[t] = j
        time.sleep(1)
    return done


def test_push_and_candles(api, by_ticker, picks):
    jobs_done = push(api, picks)
    for t in picks:
        j = jobs_done.get(t)
        if not check(j is not None and j["status"] == "done", f"3 job for {t} finished: {j}"):
            continue
        m = by_ticker[t]
        lo, hi = int(epoch(j["from"])), int(epoch(j["to"]))
        true_minutes = sum(1 for x in range(lo, hi, 60) if mock_kalshi.candle(m, x))
        check(j["candles"] == true_minutes, f"3 {t} ({m['kind']}): job wrote {j['candles']}"
              f" candles, the mock has {true_minutes} traded minutes")
        info = api.json(f"/v1/markets/{t}")
        cov = info["candles"]
        check(epoch(cov["from"]) <= lo and epoch(cov["to"]) >= hi,
              f"3 {t}: coverage {cov} takes in the job's [{j['from']}, {j['to']})")

        # Raw over the whole job window: exactly the true traded minutes.
        raw = api.json(f"/v1/markets/{t}/candles", start=lo, end=hi, fill="raw")
        want = [r for r in expected(m, 60, lo, hi, raw["coverage"], (lo, hi)) if not r[6]]
        check(as_tuples(raw["candles"]) == rounded(want),
              f"3 {t}: raw candles = the true trades ({len(raw['candles'])} vs {len(want)})")
        check(raw["coverage"]["complete"], f"3 {t}: coverage complete for the job window")

        # Forward fill, every interval, whole job window and a mid-life one.
        for name, step in (("1m", 60), ("5m", 300), ("1h", 3600), ("1d", 86400)):
            for a, b in ((lo, hi), (lo + (hi - lo) // 3, lo + 2 * (hi - lo) // 3)):
                got = all_pages(api, f"/v1/markets/{t}/candles", interval=name, start=a, end=b)
                body = api.json(f"/v1/markets/{t}/candles", interval=name, start=a, end=b,
                                limit=1)
                want = expected(m, step, a, b, body["coverage"], (lo, hi))
                check(as_tuples(got) == rounded(want),
                      f"4 {t} {name} [{a}, {b}) forward: {len(got)} rows vs {len(want)} expected")
    say(f"pushed {len(picks)} backfills and checked their candles")
    return jobs_done


def test_paging_and_formats(api, tickers):
    t = tickers[0]
    # Fixed windows: two calls a second apart must not straddle a minute.
    end = int(time.time()) - 300
    day = dict(start=end - 86400, end=end)
    for fill in ("forward", "raw"):
        one = api.json(f"/v1/markets/{t}/candles", fill=fill, limit=10000, **day)["candles"]
        paged = all_pages(api, f"/v1/markets/{t}/candles", fill=fill, limit=137, **day)
        check(one == paged and len(one) > 137,
              f"5 {fill}: 137-row pages joined = one page ({len(paged)} rows)")

    params = dict(interval="5m", **day)
    j = pd.DataFrame(api.json(f"/v1/markets/{t}/candles", **params)["candles"])
    j["ts"] = pd.to_datetime(j["ts"], utc=True)
    c = pd.read_csv(io.StringIO(api.get(f"/v1/markets/{t}/candles", format="csv",
                                        **params).text), parse_dates=["ts"])
    pq = api.get(f"/v1/markets/{t}/candles", format="parquet", **params)
    p = pd.read_parquet(io.BytesIO(pq.content))
    for name, df in (("csv", c), ("parquet", p)):
        same = (list(df.columns) == list(j.columns) and len(df) == len(j)
                and (df["ts"].astype("int64") // 10**9 == j["ts"].astype("int64") // 10**9).all()
                and all((df[k].fillna(-1) - j[k].fillna(-1)).abs().max() < 1e-9
                        for k in ("open", "high", "low", "close", "volume")))
        check(same, f"5 {name} = json ({len(df)} rows)")
    check(str(p["ts"].dtype).startswith("datetime64") and p["filled"].dtype == bool,
          f"5 parquet keeps types: {dict(p.dtypes.astype(str))}")
    check(pq.headers.get("X-Coverage-Complete") in ("true", "false"), "5 coverage headers")

    # Many tickers at once = each alone, in order, across small pages.
    names = tickers[:5]
    two = dict(start=end - 2 * 86400, end=end)
    together = all_pages(api, "/v1/candles", tickers=",".join(names), interval="15m", limit=211,
                         **two)
    alone = []
    for n in names:
        alone += [{"ticker": n, **r} for r in all_pages(
            api, f"/v1/markets/{n}/candles", interval="15m", **two)]
    check(together == alone, f"6 five tickers together = each alone ({len(together)} rows)")
    say("paging, formats and many-ticker requests checked")


def test_lists(api):
    rows = db.query("SELECT count(*) AS n, count(*) FILTER (WHERE close_time > now()) AS open"
                    " FROM kalshi_markets")[0]
    every = all_pages(api, "/v1/markets", key="markets", limit=333, sort="ticker")
    check(len(every) == rows["n"] and len({m["ticker"] for m in every}) == rows["n"],
          f"7 all markets, paged: {len(every)} of {rows['n']}")
    check([m["ticker"] for m in every] == sorted(m["ticker"] for m in every), "7 sort=ticker")
    opened = all_pages(api, "/v1/markets", key="markets", status="open", limit=1000)
    check(len(opened) == rows["open"], f"7 status=open: {len(opened)} of {rows['open']}")
    vols = [m["volume"] for m in api.json("/v1/markets", limit=50)["markets"]]
    check(vols == sorted(vols, reverse=True), "7 default sort is busiest first")
    some = every[7]
    q = api.json("/v1/markets", q=some["title"])["markets"]
    check([m["ticker"] for m in q] == [some["ticker"]], "7 q finds a market by its title")
    check(api.json("/v1/markets", q=some["ticker"][:7], limit=10000)["markets"],
          "7 q finds markets by a ticker prefix")
    ev = api.json(f"/v1/events/{some['event_ticker']}")
    check(some["ticker"] in [m["ticker"] for m in ev["markets"]]
          and ev["market_count"] == len(ev["markets"]), "7 an event lists all its markets")
    ser = api.json(f"/v1/series/{some['series_ticker']}")
    check(ser["market_count"] > 0 and len(ser["events"]) == min(100, ser["event_count"]),
          "7 a series has counts and its latest events")
    in_series = all_pages(api, "/v1/events", key="events", series=some["series_ticker"])
    check(len(in_series) == ser["event_count"], "7 /events?series= lists every event in a series")
    cats = api.json("/v1/categories")["categories"]
    check(sum(c["market_count"] for c in cats) == rows["n"],
          "7 categories add up to every market")
    lat = api.json("/v1/latest", tickers=",".join(m["ticker"] for m in every[:20]))["latest"]
    check([r["ticker"] for r in lat] == [m["ticker"] for m in every[:20]],
          "7 /latest keeps the order asked for")
    hot = api.json("/v1/hot")
    published = json.loads(ratelimit.client().get(ratelimit.KEY + "hot"))
    check(hot["count"] == len(published["tickers"]) and len(hot["markets"]) == hot["count"],
          f"7 /hot = the published hot list ({hot['count']})")
    st = api.json("/v1/status")
    check(st["kalshi"]["reachable"] and st["scans"] and "budget" in st, "7 /status answers")
    say("lists and collection state checked")


def test_duplicate_and_restart(api, by_ticker, ticker):
    m = by_ticker[ticker]
    mock_mode("slow")                       # every Kalshi answer takes 20 s
    first = api.post("/v1/backfill", {"ticker": ticker})
    second = api.post("/v1/backfill", {"ticker": ticker})
    check(first.status_code == 202, "8 the first push is accepted")
    check(error_code(second) == (409, "already_queued"), f"8 a second push while the first"
          f" runs is refused: {error_code(second)}")
    job_id = first.json()["job_id"]
    for _ in range(60):
        if api.json(f"/v1/backfill/{job_id}")["status"] == "running":
            break
        time.sleep(0.5)
    stop("api", signal.SIGKILL)             # dies mid-job
    say("API killed in the middle of a job")
    mock_mode("normal")
    start_api()
    deadline = time.time() + 300
    while time.time() < deadline:
        j = api.json(f"/v1/backfill/{job_id}")
        if j["status"] in ("done", "failed"):
            break
        time.sleep(1)
    check(j["status"] == "done", f"8 the job finished after the restart: {j}")
    lo, hi = int(epoch(j["from"])), int(epoch(j["to"]))
    want = sum(1 for x in range(lo, hi, 60) if mock_kalshi.candle(m, x))
    check(j["candles"] == want, f"8 ...with every candle: {j['candles']} of {want}")
    say("duplicate refused; a job cut short was finished after the restart")


def test_client(key, ticker, many):
    import pmapi
    pm = pmapi.Client(API, key)
    end = int(time.time()) - 300
    df = pm.candles(ticker, "5m", start=end - 86400, end=end)
    raw = Api(key).json(f"/v1/markets/{ticker}/candles", interval="5m", start=end - 86400,
                        end=end)
    check(len(df) == len(raw["candles"]) and df.index.name == "ts"
          and abs(df["close"].sum() - sum(c["close"] for c in raw["candles"])) < 1e-6
          and str(df.index.dtype).startswith("datetime64"), f"9 pm.candles: {len(df)} rows")
    check(df.attrs["coverage"]["complete"] == raw["coverage"]["complete"],
          "9 pm.candles carries coverage")
    long = pm.candles(many, "1h", fill="raw")
    check(set(long["ticker"]) <= set(many) and not long["filled"].any(), "9 pm.candles(list)")
    wide = pm.closes(many, "1h")
    check(set(wide.columns) <= set(many), f"9 pm.closes: {wide.shape}")
    mk = pm.markets(status="open")
    n = db.query("SELECT count(*) AS n FROM kalshi_markets WHERE close_time > now()")[0]["n"]
    check(len(mk) == n, f"9 pm.markets(status='open'): {len(mk)} of {n}")
    check(pm.market(ticker)["ticker"] == ticker, "9 pm.market")
    j = pm.wait(pm.backfill(ticker, start=time.time() - 3 * 3600))
    check(j["status"] == "done", "9 pm.backfill + pm.wait")
    check(len(pm.hot()) > 0 and "kalshi" in pm.status(), "9 pm.hot, pm.status")
    say("pmapi client checked")


def main():
    t0, keys = setup()
    markets, _ = mock_kalshi.build(t0)
    by_ticker = {m["ticker"]: m for m in markets}
    kinds = {}
    for m in markets:
        kinds.setdefault(m["kind"], []).append(m["ticker"])
    picks = (kinds["hot"][:3] + kinds["warm"][:3] + kinds["cold"][:2] + kinds["never"][:1]
             + kinds["closing"][:1] + kinds["closed"][:2])
    api = Api(keys["full"])
    try:
        test_keys(keys)
        test_errors(api, picks[0])
        test_push_and_candles(api, by_ticker, picks)
        test_paging_and_formats(api, picks)
        test_lists(api)
        test_duplicate_and_restart(api, by_ticker, kinds["hot"][5])
        test_client(keys["full"], picks[0], picks[:4])
    finally:
        stop("api")
        stop("mock")
    print()
    if failures:
        print(f"FAILED: {len(failures)} checks")
        for f in failures:
            print("  -", f)
        sys.exit(1)
    print("PASSED: every check")


if __name__ == "__main__":
    main()

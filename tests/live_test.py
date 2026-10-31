"""End-to-end test: discover + activity + live + backfill against mock Kalshi.

    .venv/bin/python tests/live_test.py            # about 35 minutes

Everything runs in its own database schema (pm_live_test), Redis prefix and
state folder, so it can run on the server beside the real collectors.

For half an hour it lets the programs collect while things go wrong on a
schedule -- programs killed mid-flight, the network dropping out, Kalshi
throttling and crawling, Redis vanishing -- then it stops them, lets them
catch up once, and checks:

  1  every market is tracked and nothing is left behind
  2  every market's history reaches its open (or the backfill window)
  3  every stored candle is exactly the true one, and no traded minute
     inside a market's covered stretch is missing
  4  only traded minutes were stored
  5  the request budget held over every 60 seconds, faults included
  6  no request broke Kalshi's size limits
  7  the busiest markets ended up on the hot list, inside its cap
  8  backfill.py --verify agrees
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORK = Path("/tmp/pmtest")
LOGS = WORK / "logs"
PORT = 18080
BUDGET = 300
SYNC_MINUTES = 2
DEAD_REDIS = "redis://127.0.0.1:6390/0"

ENV = dict(os.environ,
           PGSCHEMA="pm_live_test", REDIS_PREFIX="pmtest:", PM_STATE_DIR=str(WORK / "state"),
           KALSHI_API_URL=f"http://127.0.0.1:{PORT}/trade-api/v2", LIVE_VENUES="kalshi",
           MARKET_SYNC_MINUTES=str(SYNC_MINUTES), KALSHI_REQUESTS_PER_MIN=str(BUDGET),
           BACKFILL_DAYS="2", PYTHONUNBUFFERED="1")
os.environ.update({k: ENV[k] for k in ("PGSCHEMA", "REDIS_PREFIX", "PM_STATE_DIR",
                                       "KALSHI_API_URL", "MARKET_SYNC_MINUTES",
                                       "KALSHI_REQUESTS_PER_MIN", "BACKFILL_DAYS")})
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

import db                     # noqa: E402
import mock_kalshi            # noqa: E402
import ratelimit              # noqa: E402
import settings               # noqa: E402

START = time.time()
procs = {}


def say(text):
    print(f"[{(time.time() - START) / 60:5.1f} min] {text}", flush=True)


def start(name, *args, redis=None):
    env = dict(ENV)
    if redis:
        env["REDIS_URL"] = redis
    log = open(LOGS / f"{name}.log", "a")
    log.write(f"\n===== start {time.strftime('%H:%M:%S')} {' '.join(args)} redis={redis or 'normal'}\n")
    log.flush()
    procs[name] = subprocess.Popen([sys.executable, *args], cwd=ROOT, env=env,
                                   stdout=log, stderr=subprocess.STDOUT)
    say(f"started {name}" + (" (Redis unreachable)" if redis else ""))


def kill(name, sig=signal.SIGKILL):
    p = procs.pop(name, None)
    if p and p.poll() is None:
        p.send_signal(sig)
        try:
            p.wait(timeout=90)
        except subprocess.TimeoutExpired:
            p.kill()
    say(f"{'killed' if sig == signal.SIGKILL else 'stopped'} {name}")


def run(name, *args, timeout=1800):
    with open(LOGS / f"{name}.log", "a") as log:
        log.write(f"\n===== run {time.strftime('%H:%M:%S')} {' '.join(args)}\n")
        log.flush()
        out = subprocess.run([sys.executable, *args], cwd=ROOT, env=ENV, timeout=timeout,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        log.write(out.stdout)
    return out.stdout


def mock(path):
    with urllib.request.urlopen(f"http://127.0.0.1:{PORT}{path}", timeout=60) as r:
        return json.loads(r.read())


def mode(m):
    mock(f"/_control?mode={m}")
    say(f"mock Kalshi: {m}")


def at(minute):
    while time.time() < START + minute * 60:
        time.sleep(1)
        for name, p in list(procs.items()):
            if p.poll() is not None:
                say(f"!! {name} exited by itself with {p.returncode}")
                procs.pop(name)


def reset():
    subprocess.run(["rm", "-rf", str(WORK)])
    LOGS.mkdir(parents=True)
    (WORK / "state").mkdir()
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS pm_live_test CASCADE")
            cur.execute("CREATE SCHEMA pm_live_test")
    r = ratelimit.client()
    keys = list(r.scan_iter("pmtest:*"))
    if keys:
        r.delete(*keys)


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def d(x):
    return Decimal(str(x)).quantize(Decimal("0.0001"))


def check(t0):
    failures = []
    markets, _ = mock_kalshi.build(t0)
    floor = int(time.time() - settings.BACKFILL_DAYS * 86400) // 60 * 60
    rows = {r["ticker"]: r for r in db.query("SELECT * FROM kalshi_tracking")}
    say(f"checking {len(markets)} markets, {len(rows)} tracked")

    # 1  tracked, nothing left behind
    missing = [m["ticker"] for m in markets if m["ticker"] not in rows]
    behind = [t for t, r in rows.items() if r["candles_fetched_to"] < r["volume_measured_at"]]
    if missing:
        failures.append(f"1: {len(missing)} markets never tracked, e.g. {missing[:3]}")
    if behind:
        failures.append(f"1: {len(behind)} markets still behind their last reading, e.g. {behind[:3]}")

    # 2  history complete
    short = []
    for m in markets:
        r = rows.get(m["ticker"])
        if r and r["candles_fetched_from"].timestamp() > max(int(m["open"]) // 60 * 60, floor):
            short.append(m["ticker"])
    if short:
        failures.append(f"2: {len(short)} markets with history missing, e.g. {short[:3]}")

    # 3, 4  every candle in each covered stretch is exactly right
    wrong = gaps = extra = checked = 0
    examples = []
    for m in markets:
        r = rows.get(m["ticker"])
        if not r:
            continue
        lo = int(r["candles_fetched_from"].timestamp()) // 60 * 60
        hi = int(r["candles_fetched_to"].timestamp())
        stored = {int(c["ts"].timestamp()): c for c in db.query(
            "SELECT ts, open, high, low, close, volume FROM kalshi_candles"
            " WHERE ticker = %s AND ts >= to_timestamp(%s) AND ts < to_timestamp(%s)",
            (m["ticker"], lo, hi))}
        for minute in range(lo, hi, 60):
            # A minute only partly inside [from, to) can only be checked if
            # the covered part holds all its trades.
            want = mock_kalshi.candle(m, minute)
            got = stored.pop(minute, None)
            if minute + 60 > hi and want is not None:
                inside = [x for x in mock_kalshi.trades(m, minute) if x[0] < hi]
                if len(inside) < len(mock_kalshi.trades(m, minute)):
                    continue
            if want is None and got is None:
                continue
            checked += 1
            if got is None:
                gaps += 1
                if len(examples) < 5:
                    examples.append(f"gap {m['ticker']} {time.strftime('%H:%M', time.gmtime(minute))}")
            elif want is None:
                extra += 1
            elif (d(got["open"]), d(got["high"]), d(got["low"]), d(got["close"]),
                  d(got["volume"])) != tuple(map(d, want)):
                wrong += 1
                if len(examples) < 5:
                    examples.append(f"wrong {m['ticker']} {minute}: {got} vs {want}")
        extra += len(stored)
    say(f"compared {checked:,} traded minutes: {gaps} missing, {wrong} wrong, {extra} extra")
    if gaps or wrong or extra:
        failures.append(f"3: {gaps} missing, {wrong} wrong, {extra} extra candles; {examples}")
    zero = db.query("SELECT count(*) AS n FROM kalshi_candles WHERE NOT volume > 0")[0]["n"]
    if zero:
        failures.append(f"4: {zero} candles without a trade were stored")

    # 5, 6  the budget and the size limits
    log = mock("/_stats")["requests"]
    times = sorted(t for t, _, _ in log)
    worst, j = 0, 0
    for i, t in enumerate(times):
        while times[j] < t - 60:
            j += 1
        worst = max(worst, i - j + 1)
    statuses = {}
    for _, _, s in log:
        statuses[s] = statuses.get(s, 0) + 1
    say(f"{len(log):,} requests; busiest 60 s held {worst} (budget {BUDGET}); by status {statuses}")
    if worst > BUDGET:
        failures.append(f"5: {worst} requests in one 60 s window, budget {BUDGET}")
    if statuses.get(400):
        failures.append(f"6: {statuses[400]} requests rejected as too big")

    # 7  hot list
    hot = json.loads(ratelimit.client().get(ratelimit.KEY + "hot") or '{"tickers": [], "cap": 0}')
    busy = [m["ticker"] for m in markets if m["kind"] == "hot"]
    absent = [t for t in busy if t not in hot["tickers"]]
    say(f"hot list: {len(hot['tickers'])} markets, cap {hot['cap']}; busy ones missing: {len(absent)}")
    if absent:
        failures.append(f"7: {len(absent)} of the {len(busy)} busiest markets not hot")
    if len(hot["tickers"]) > hot["cap"]:
        failures.append("7: hot list over its cap")

    # 8  backfill's own verification
    out = run("verify", "backfill.py", "--verify")
    say("backfill --verify: " + out.strip().splitlines()[-1] if out.strip() else "no output")
    if " 0 do not" not in out:
        failures.append("8: backfill --verify found mismatches:\n" + out)
    return failures


def main():
    reset()
    start("mock", "tests/mock_kalshi.py", "--port", str(PORT), "--state", str(WORK / "mock.json"))
    for _ in range(50):
        if (WORK / "mock.json").exists():
            break
        time.sleep(0.2)
    t0 = json.loads((WORK / "mock.json").read_text())["t0"]

    start("discover", "discover.py")
    time.sleep(20)                      # schema first, then the others
    start("activity", "activity.py")
    start("live", "live.py")

    at(4);  kill("live")                                    # crash mid-cycle
    at(6);  start("live", "live.py")
    at(8);  mode("down")                                    # network gone
    at(10); mode("normal")
    at(11); mode("429")                                     # throttled
    at(12); mode("normal")
    at(13); kill("activity")                                # scans pile up unscored
    at(17); start("activity", "activity.py")
    at(18)                                                  # Redis gone for everyone
    for name in ("discover", "activity", "live"):
        kill(name, signal.SIGTERM)
        start(name, f"{name}.py", redis=DEAD_REDIS)
    at(21)
    for name in ("discover", "activity", "live"):
        kill(name, signal.SIGTERM)
        start(name, f"{name}.py")
    at(22.5); kill("discover")                              # dies mid-scan
    at(23); start("discover", "discover.py")
    start("backfill", "backfill.py")                        # alongside everything
    at(25); mode("slow")                                    # answers take 20 s
    at(26); mode("normal")
    at(29)
    for name in ("backfill", "live", "activity", "discover"):
        kill(name, signal.SIGTERM)

    say("catching up once: one scan, its scores, three live cycles, backfill")
    run("discover", "discover.py", "--once")
    run("activity", "activity.py", "--once")
    run("live", "live.py", "--cycles", "3", timeout=600)
    run("backfill", "backfill.py")
    run("activity", "activity.py", "--once")

    failures = check(t0)
    kill("mock", signal.SIGTERM)
    print()
    if failures:
        print("FAILED")
        for f in failures:
            print("  -", f)
    else:
        print("PASSED: no gaps, no wrong candles, budget held, hot list right")
    print(run("report", "backfill.py", "--report"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

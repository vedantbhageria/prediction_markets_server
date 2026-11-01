"""monitor.py's minute files and the /api-usage charts drawn from them.
Run on the server:

    .venv/bin/python tests/monitor_test.py          # about ten seconds

Uses its own schema (pm_monitor_test), Redis prefix and state folder, and checks:

  1  a minute's line: requests by program from Redis, the limit, Kalshi up
     or down, and the backfill count
  2  reading: lines in order across days; a line cut short is skipped
  3  a restart: the minutes missed are filled in from Redis, none twice
  4  files older than 30 days are deleted
  5  the page: the budget chart, the limit line, the outage shading and the
     backfill progress are drawn from the files
"""

import json
import os
import shutil
import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WORK = Path("/tmp/pmmonitortest")
os.environ.update(PGSCHEMA="pm_monitor_test", REDIS_PREFIX="pmmontest:",
                  PM_STATE_DIR=str(WORK / "state"), VIEWER_USER="", VIEWER_PASSWORD_HASH="")
sys.path.insert(0, str(ROOT))

import db           # noqa: E402
import monitor      # noqa: E402
import ratelimit    # noqa: E402
import settings     # noqa: E402
import store        # noqa: E402

failures = []
HOST = settings.KALSHI_API_URL.split("/")[2]


def check(ok, what):
    print(("ok   " if ok else "FAIL ") + what, flush=True)
    if not ok:
        failures.append(what)


def setup():
    shutil.rmtree(WORK, ignore_errors=True)
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS pm_monitor_test CASCADE")
            cur.execute("CREATE SCHEMA pm_monitor_test")
    store.ensure_schema()
    r = ratelimit.client()
    stale = list(r.scan_iter("pmmontest:*"))
    if stale:
        r.delete(*stale)
    # Three markets, two with history still to fetch.
    db.execute("INSERT INTO kalshi_series (series_ticker) VALUES ('S')")
    db.execute("INSERT INTO kalshi_events (event_ticker, series_ticker, raw) VALUES ('E', 'S', '{}')")
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    for ticker, start in (("A", now - timedelta(days=1)), ("B", now - timedelta(hours=5)),
                          ("C", now - timedelta(days=3))):
        db.execute("INSERT INTO kalshi_markets (ticker, event_ticker, open_time, close_time, raw)"
                   " VALUES (%s, 'E', %s, %s, '{}')", (ticker, now - timedelta(days=3),
                                                        now + timedelta(days=1)))
        db.execute("INSERT INTO kalshi_tracking (ticker, volume, volume_measured_at,"
                   " candles_fetched_from, candles_fetched_to) VALUES (%s, 1, %s, %s, %s)",
                   (ticker, now, start, now))


def count(source, minute, n):
    r = ratelimit.client()
    r.set(f"{ratelimit.KEY}used:{source}:{minute // 60}", n, ex=7200)


def test_line():
    minute = (int(time.time()) // 60 - 2) * 60
    count("hot", minute, 5)
    count("backfill", minute, 7)
    line = monitor.snapshot(minute, with_left=True)
    check(line["used"] == {"hot": 5, "backfill": 7} and line["limit"] == settings.KALSHI_REQUESTS_PER_MIN
          and line["up"] is True and line["left"] == 2,
          f"1 a minute's line: {line}")
    ratelimit.client().set(f"{settings.REDIS_PREFIX}down:{HOST}", time.time(), ex=60)
    check(monitor.snapshot(minute)["up"] is False, "1 Kalshi marked down while the link is")
    ratelimit.client().delete(f"{settings.REDIS_PREFIX}down:{HOST}")
    check(len(json.dumps(line)) < 250, f"1 a line is small: {len(json.dumps(line))} bytes")


def test_read():
    shutil.rmtree(monitor.DIR, ignore_errors=True)
    today = int(time.time()) // 86400 * 86400
    for t in (today - 120, today - 60, today, today + 60):     # across midnight UTC
        monitor.append({"t": t, "used": {"hot": 1}})
    with open(monitor.DIR / f"{datetime.fromtimestamp(today, timezone.utc):%Y-%m-%d}.jsonl",
              "a") as f:
        f.write('{"t": 12')                                     # cut short
    got = [line["t"] for line in monitor.read(today - 90)]
    check(got == [today - 60, today, today + 60], f"2 read across days, in order: {got}")
    check(monitor.last_minute() == today + 60, "2 the newest minute on file, past a broken line")


def test_restart():
    shutil.rmtree(monitor.DIR, ignore_errors=True)
    now_minute = (int(time.time()) // 60 - 1) * 60
    stopped_at = now_minute - 5 * 60
    monitor.append({"t": stopped_at, "used": {}})
    for back in range(1, 5):
        count("hot", now_minute - back * 60, 10 + back)
    stop = threading.Event()
    th = threading.Thread(target=monitor.run, args=(stop,), daemon=True)
    th.start()
    time.sleep(4)
    stop.set()
    th.join(timeout=10)
    lines = monitor.read(stopped_at)
    ts = [line["t"] for line in lines]
    want = list(range(stopped_at, now_minute + 60, 60))
    if ts[-1] > now_minute:             # the clock ticked over a minute meanwhile
        want.append(ts[-1])
    check(ts == want, f"3 every missed minute once, in order: {[(t - stopped_at) // 60 for t in ts]}")
    filled = {line["t"]: line["used"].get("hot") for line in lines}
    check(all(filled[now_minute - b * 60] == 10 + b for b in range(1, 5)),
          "3 the missed minutes carry their request counts from Redis")
    check("left" in lines[-1] and "up" in lines[-1], "3 the newest line is a full one")


def test_prune():
    old = monitor.DIR / f"{datetime.now(timezone.utc) - timedelta(days=40):%Y-%m-%d}.jsonl"
    old.write_text('{"t": 1}\n')
    monitor.prune()
    check(not old.exists() and any(monitor.DIR.glob("*.jsonl")),
          "4 a file 40 days old is deleted, newer ones kept")


def test_page():
    shutil.rmtree(monitor.DIR, ignore_errors=True)
    minute = (int(time.time()) // 60 - 1) * 60
    for back in range(30, 0, -1):
        t = minute - back * 60
        monitor.append({"t": t, "used": {"hot": 60, "backfill": 100 + back}, "limit": 240,
                        "factor": 1.0, "up": back not in (10, 11), "behind": 110 + back,
                        "hot": 5000, **({"left": 1000 - 10 * (30 - back)} if back % 10 == 0 else {})})
    import viewer
    viewer.LOGIN_USER = None
    page = viewer.app.test_client().get("/api-usage?window=1h&refresh=off")
    text = page.get_data(as_text=True)
    check(page.status_code == 200, f"5 /api-usage renders ({page.status_code})")
    check("Kalshi request budget" in text and "stroke-dasharray" in text and "#f6d5d4" in text,
          "5 the budget chart has its limit line and the outage shading")
    check("markets left" in text and "% <span" in text and "about" in text,
          "5 backfill progress with a pace")
    check(text.count("<svg") >= 4, f"5 charts drawn: {text.count('<svg')}")


def main():
    setup()
    test_line()
    test_read()
    test_restart()
    test_prune()
    test_page()
    print()
    if failures:
        print(f"FAILED: {len(failures)} checks")
        sys.exit(1)
    print("PASSED: every check")


if __name__ == "__main__":
    main()

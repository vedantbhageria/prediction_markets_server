"""A minute-by-minute record of how collection is doing, for the viewer's
/api-usage charts. Plain files, kept apart from the database:

    data/state/monitor/2026-11-01.jsonl     one line a minute, UTC days

    {"t": 1793490000,                       the minute (Unix seconds, its start)
     "used": {"hot": 67, "backfill": 120},  Kalshi requests in it, by program
     "limit": 240, "factor": 1.0,           the budget, and any cut after a 429
     "up": true,                            Kalshi reachable
     "behind": 118,                         live: seconds behind the clock
     "hot": 6627,                           markets on the hot list
     "left": 771987}                        markets with history still to fetch
                                            (every LEFT_EVERY minutes only)

About 200 bytes a minute; files older than KEEP_DAYS are deleted. The viewer
runs run() in a thread. If it was stopped, the request counts of the minutes
it missed are filled in from Redis, which keeps them for two hours.
"""

import json
import logging
import time
from datetime import datetime, timedelta, timezone

import backfill
import db
import paths
import ratelimit
import settings

log = logging.getLogger("monitor")
DIR = paths.STATE / "monitor"
KEEP_DAYS = 30
LEFT_EVERY = 10             # minutes between counts of the backfill still to do
REDIS_KEEPS = 120           # minutes of request counts Redis still has


def _file(minute):
    return DIR / f"{datetime.fromtimestamp(minute, timezone.utc):%Y-%m-%d}.jsonl"


def _used(minute):
    """{program: Kalshi requests} in that minute, from Redis; zeros left out."""
    m = minute // 60
    out = {}
    for source in ratelimit.SHARE:
        n = ratelimit.per_minute(source, m, m).get(m, 0)
        if n:
            out[source] = n
    return out


def backfill_left():
    floor = backfill.floor_ts()
    return db.query("""
        SELECT count(*) AS n FROM kalshi_tracking t JOIN kalshi_markets m USING (ticker)
        WHERE t.candles_fetched_from > to_timestamp(GREATEST(%s,
                  EXTRACT(EPOCH FROM date_trunc('minute', COALESCE(m.open_time,
                                                                   t.candles_fetched_from)))))""",
                    (floor,))[0]["n"]


def snapshot(minute, with_left=False):
    """The line for one finished minute."""
    line = {"t": minute, "used": _used(minute), "limit": settings.KALSHI_REQUESTS_PER_MIN,
            "factor": round(ratelimit.factor(), 3)}
    r = ratelimit.client()
    host = settings.KALSHI_API_URL.split("/")[2]
    line["up"] = not r.exists(f"{settings.REDIS_PREFIX}down:{host}")
    live = r.get(ratelimit.KEY + "live")
    if live:
        line["behind"] = round(time.time() - json.loads(live)["minute"])
    hot = r.get(ratelimit.KEY + "hot")
    if hot:
        line["hot"] = len(json.loads(hot)["tickers"])
    if with_left:
        line["left"] = backfill_left()
    return line


def append(line):
    DIR.mkdir(parents=True, exist_ok=True)
    with open(_file(line["t"]), "a", encoding="utf-8") as f:
        f.write(json.dumps(line, separators=(",", ":")) + "\n")


def read(since):
    """Every line from `since` (Unix seconds) on, oldest first."""
    out = []
    day = datetime.fromtimestamp(since, timezone.utc).date()
    today = datetime.now(timezone.utc).date()
    while day <= today:
        path = DIR / f"{day:%Y-%m-%d}.jsonl"
        if path.exists():
            for raw in path.read_text(encoding="utf-8").splitlines():
                try:
                    line = json.loads(raw)
                except ValueError:
                    continue            # a line cut short by a crash
                if line.get("t", 0) >= since:
                    out.append(line)
        day += timedelta(days=1)
    return out


def last_minute():
    """The newest minute on file, or None."""
    files = sorted(DIR.glob("*.jsonl")) if DIR.exists() else []
    for path in reversed(files):
        lines = path.read_text(encoding="utf-8").splitlines()
        for raw in reversed(lines):
            try:
                return json.loads(raw)["t"]
            except (ValueError, KeyError):
                continue
    return None


def prune():
    cutoff = f"{datetime.now(timezone.utc) - timedelta(days=KEEP_DAYS):%Y-%m-%d}"
    for path in DIR.glob("*.jsonl") if DIR.exists() else []:
        if path.stem < cutoff:
            path.unlink()


def run(stop):
    """Write a line for each finished minute until `stop` is set."""
    done = last_minute()
    since_left = LEFT_EVERY        # count the backfill on the first line
    while not stop.is_set():
        minute = (int(time.time()) // 60 - 1) * 60          # the last finished one
        if done is None or done < minute - REDIS_KEEPS * 60:
            done = minute - 60
        try:
            # Minutes missed while the viewer was stopped: request counts only.
            for gap in range(done + 60, minute, 60):
                append({"t": gap, "used": _used(gap), "limit": settings.KALSHI_REQUESTS_PER_MIN})
            if minute > done:
                since_left += 1
                append(snapshot(minute, with_left=since_left >= LEFT_EVERY))
                if since_left >= LEFT_EVERY:
                    since_left = 0
                done = minute
            prune()
        except Exception as exc:
            log.warning("monitor line not written: %s: %s", type(exc).__name__, exc)
        stop.wait(60 - time.time() % 60 + 2)       # just after the next minute

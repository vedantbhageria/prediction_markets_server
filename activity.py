"""Activity scores: how busy each Kalshi market is, scored after every scan.

    python activity.py              # run forever (the activity service)
    python activity.py --once       # score whatever is waiting, then exit

Discovery records each market's volume in kalshi_observations, with the two
moments the reading was true between: asked_at (just before the request, less
OBSERVATION_LAG) and answered_at. Once a scan is over this scores it, one
statement per step, entirely inside PostgreSQL (about 3 s for 185k markets):

    new market     score = total volume / minutes since it opened
    seen before    dv = volume change, dt = minutes since last measured
                   score += (1 - 2^(-dt/60)) * (dv/dt - score)
                   -- an average with a one-hour half-life
    no change      no trades happened between the two readings, so the candles
                   are complete up to the new one without asking Kalshi

The "no change" rule needs care, because a reading is true at some unknown
moment between asked_at and answered_at. volume_measured_at is therefore kept
so that one thing always holds: once candles_fetched_to has reached
volume_measured_at, no trade up to the reading is missing. After a change it
is the answer time, which coverage has to pass; after a no-change step it is
the ask time, because coverage then provably runs up to the reading itself.

Then it picks the hot markets for live.py: score >= HOT_ENTER (or >= HOT_EXIT
for markets already hot), busiest first, no more than the request budget can
fetch every minute. The list goes to Redis; if Redis is away live.py works it
out from the same table.

Scans are scored once each, oldest first, so a restart after a long stop works
through the backlog in order and every step uses the right dt.
"""

import argparse
import json
import logging
import math
import signal
import threading
import time

import db
import ratelimit
import settings
from db import connection

log = logging.getLogger("activity")

HOT_KEY = ratelimit.KEY + "hot"
# Trading is over in these states, so a stored volume is the final one.
FINISHED = ("closed", "determined", "finalized", "settled", "disputed", "amended")

# Markets in the store with no bookkeeping row yet and trading already over --
# the 30-day closed import, mostly. Their stored volume was read after they
# closed, so it is their final volume, and nothing can trade after close_time.
BOOTSTRAP_CLOSED = """
INSERT INTO kalshi_tracking (ticker, volume, volume_measured_at, activity_score,
                             candles_fetched_from, candles_fetched_to)
SELECT m.ticker, COALESCE(m.volume, 0), m.close_time, 0,
       CASE WHEN COALESCE(m.volume, 0) = 0 THEN COALESCE(LEAST(m.open_time, m.close_time),
                                                         m.close_time)
            ELSE m.close_time END,
       m.close_time
FROM kalshi_markets m
WHERE m.status = ANY(%(finished)s) AND m.close_time <= now()
  AND NOT EXISTS (SELECT 1 FROM kalshi_tracking t WHERE t.ticker = m.ticker)
ON CONFLICT (ticker) DO NOTHING
"""

# One reading per market: a market can turn up twice in a scan (open list,
# then closed import); the later reading wins.
LATEST = """
SELECT DISTINCT ON (o.ticker) o.ticker, o.volume, o.asked_at, o.answered_at
FROM kalshi_observations o
WHERE o.scan_id = %(scan)s
ORDER BY o.ticker, o.answered_at DESC
"""

# First sighting.
#   already over   as BOOTSTRAP_CLOSED: nothing after close_time to fetch
#   never traded   complete from the open up to the reading
#   opened lately  live.py fetches it all, from the open
#   otherwise      live.py takes it from here, backfill.py fills what came before
SEED = f"""
INSERT INTO kalshi_tracking (ticker, volume, volume_measured_at, activity_score,
                             candles_fetched_from, candles_fetched_to)
SELECT o.ticker, o.volume,
       CASE WHEN o.over THEN m.close_time
            WHEN o.volume = 0 THEN o.asked_at ELSE o.answered_at END,
       CASE WHEN o.over OR m.open_time IS NULL THEN 0
            ELSE o.volume / GREATEST(EXTRACT(EPOCH FROM o.answered_at - m.open_time) / 60, 1) END,
       CASE WHEN o.over AND o.volume = 0 THEN COALESCE(LEAST(m.open_time, m.close_time),
                                                      m.close_time)
            WHEN o.over THEN m.close_time
            WHEN o.volume = 0 THEN LEAST(COALESCE(m.open_time, o.asked_at), o.asked_at)
            WHEN m.open_time > o.asked_at - %(new)s * interval '1 hour'
                 THEN date_trunc('minute', m.open_time)
            ELSE o.asked_at END,
       CASE WHEN o.over THEN m.close_time
            WHEN o.volume = 0 THEN o.asked_at
            WHEN m.open_time > o.asked_at - %(new)s * interval '1 hour'
                 THEN date_trunc('minute', m.open_time)
            ELSE o.asked_at END
FROM (SELECT l.*, (m2.status = ANY(%(finished)s) AND m2.close_time <= l.asked_at) AS over
      FROM ({LATEST}) l JOIN kalshi_markets m2 USING (ticker)) o
JOIN kalshi_markets m USING (ticker)
ON CONFLICT (ticker) DO NOTHING
"""

# Seen before. Unchanged volume and coverage already at the last reading: no
# trade is missing up to this reading, so coverage moves to its ask time and
# so does volume_measured_at. Anything else: coverage stays, and
# volume_measured_at becomes the answer time, which coverage must pass before
# the rule can apply again.
SCORE = f"""
UPDATE kalshi_tracking t SET
  activity_score = t.activity_score
      + (1 - power(2, -o.dt / %(half)s)) * (GREATEST(o.volume - t.volume, 0) / o.dt
                                            - t.activity_score),
  candles_fetched_to = CASE WHEN o.quiet THEN GREATEST(t.candles_fetched_to, o.asked_at)
                            ELSE t.candles_fetched_to END,
  volume_measured_at = CASE WHEN o.quiet THEN o.asked_at ELSE o.answered_at END,
  volume = o.volume
FROM (SELECT l.*,
             EXTRACT(EPOCH FROM l.answered_at - t2.volume_measured_at) / 60 AS dt,
             l.volume = t2.volume AND t2.candles_fetched_to >= t2.volume_measured_at AS quiet
      FROM ({LATEST}) l JOIN kalshi_tracking t2 USING (ticker)) o
WHERE t.ticker = o.ticker AND o.dt > 0
"""

MOVED = f"""
SELECT count(*) FROM ({LATEST}) o JOIN kalshi_tracking t USING (ticker)
WHERE o.volume <> t.volume AND o.answered_at > t.volume_measured_at
"""


def score_scan(scan):
    """Score one scan and mark it done. Returns (readings, new, moved)."""
    args = {"scan": scan, "finished": list(FINISHED), "new": settings.NEW_MARKET_HOURS,
            "half": settings.HALF_LIFE_MINUTES}
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(DISTINCT ticker) FROM kalshi_observations"
                        " WHERE scan_id = %(scan)s", args)
            readings = cur.fetchone()[0]
            cur.execute(MOVED, args)
            moved = cur.fetchone()[0]
            # Markets already tracked first, then the new ones -- the other way
            # round, a new market would be scored twice off one reading.
            cur.execute(SCORE, args)
            cur.execute(SEED, args)
            new = cur.rowcount
            cur.execute(BOOTSTRAP_CLOSED, args)
            new += cur.rowcount
            cur.execute("DELETE FROM kalshi_observations WHERE scan_id = %(scan)s", args)
            cur.execute("UPDATE kalshi_scans SET scored_at = now() WHERE scan_id = %(scan)s", args)
    return readings, new, moved


def waiting_scans():
    """Scans ready to score: finished, or abandoned (a later one has begun)."""
    return [r["scan_id"] for r in db.query(
        "SELECT scan_id FROM kalshi_scans s WHERE scored_at IS NULL"
        " AND (finished_at IS NOT NULL"
        "      OR EXISTS (SELECT 1 FROM kalshi_scans n WHERE n.scan_id > s.scan_id))"
        " ORDER BY scan_id")]


# ---------------------------------------------------------------------------
# The hot list
# ---------------------------------------------------------------------------

def warm_backlog():
    """Markets that traded since their candles were last fetched."""
    return db.query("SELECT count(*) AS n FROM kalshi_tracking"
                    " WHERE candles_fetched_to < volume_measured_at")[0]["n"]


def hot_cap(warm=None):
    """How many markets can be fetched every minute inside the budget.

    budget (after any 429 cut) - headroom - what discovery and API backfills
    used over the last hour - one pass over the warm backlog per scan
    interval, then 100 markets per remaining request per minute.
    """
    budget = settings.KALSHI_REQUESTS_PER_MIN * ratelimit.factor()
    discover = ratelimit.used("discover", 60)
    discover = 20 if not discover else discover / 60
    discover += (ratelimit.used("api", 60) or 0) / 60
    warm = warm_backlog() if warm is None else warm
    warm_rate = math.ceil(warm / 100) / max(settings.MARKET_SYNC_MINUTES, 1)
    spare = budget * (1 - settings.BUDGET_HEADROOM) - discover - warm_rate
    return max(0, int(spare)) * 100, {"budget": round(budget, 1),
                                      "discover": round(discover, 1),
                                      "warm": round(warm_rate, 1)}


def pick_hot(cap, previous=()):
    rows = db.query("""
        SELECT t.ticker FROM kalshi_tracking t JOIN kalshi_markets m USING (ticker)
        WHERE (t.activity_score >= %(enter)s
               OR (t.activity_score >= %(exit)s AND t.ticker = ANY(%(prev)s)))
          AND m.close_time > now() AND NOT (m.status = ANY(%(finished)s))
        ORDER BY t.activity_score DESC, t.ticker
        LIMIT %(cap)s""", {"enter": settings.HOT_ENTER, "exit": settings.HOT_EXIT,
                           "prev": list(previous), "finished": list(FINISHED), "cap": cap})
    return [r["ticker"] for r in rows]


def load_hot():
    """The current hot list, or None if Redis has none."""
    try:
        raw = ratelimit.client().get(HOT_KEY)
    except Exception:
        return None
    return json.loads(raw)["tickers"] if raw else None


def publish_hot():
    previous = load_hot() or []
    warm = warm_backlog()
    cap, parts = hot_cap(warm)
    hot = pick_hot(cap, previous)
    wanted = len(pick_hot(10 ** 7, previous)) if len(hot) == cap else len(hot)
    try:
        ratelimit.client().set(HOT_KEY, json.dumps({"tickers": hot, "cap": cap,
                                                    "at": time.time(), **parts}))
    except Exception as exc:
        log.warning("could not publish the hot list: %s", exc)
    log.info("hot: %s markets (cap %s, %s qualify; budget %s/min, discover %s, warm %s);"
             " warm backlog %s", f"{len(hot):,}", f"{cap:,}", f"{wanted:,}",
             parts["budget"], parts["discover"], parts["warm"], f"{warm:,}")
    return hot


def run_once():
    did = False
    for scan in waiting_scans():
        started = time.time()
        readings, new, moved = score_scan(scan)
        log.info("scan %s scored in %.1fs: %s readings, %s new markets, %s moved",
                 scan, time.time() - started, f"{readings:,}", f"{new:,}", f"{moved:,}")
        did = True
    if did:
        publish_hot()
    return did


def run(stop):
    # Publish once on start, so live.py has a list straight away.
    try:
        db.query("SELECT 1")
        run_once() or publish_hot()
    except Exception as exc:
        log.warning("first round failed: %s: %s", type(exc).__name__, exc)
    while not stop.wait(10):
        try:
            run_once()
        except Exception as exc:
            log.warning("scoring failed: %s: %s", type(exc).__name__, exc)
            stop.wait(30)
    log.info("stopped")


def main():
    parser = argparse.ArgumentParser(description="Score Kalshi market activity after each scan.")
    parser.add_argument("--once", action="store_true", help="score what is waiting, then exit")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    if args.once:
        run_once() or publish_hot()
    else:
        run(stop)


if __name__ == "__main__":
    main()

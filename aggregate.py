"""Polymarket candles from Polymarket trades.

    python aggregate.py             # run forever (the aggregate service)
    python aggregate.py --once      # make every candle that is due, then exit
    python aggregate.py --rebuild --from 2026-10-20 [--to ...] [--market C ...]
                                    # make those minutes again from the trades

Whoever stores a trade also marks its minute in poly_dirty, in the same
transaction (poly.write_trades). This takes marked minutes a batch at a time,
in one transaction of two statements:

  1. unmark them, holding their rows locked until the transaction ends
  2. make each one's candle again from every trade in it

The second statement reads the trades after the first has the minutes in
hand, so it sees every trade committed before then. A trade committed later
marks its minute again -- its writer waits for the lock and then finds the
mark gone -- and the minute is made again next round. (Done in one statement,
the trades would be read as of the statement's start, and a trade committed
in between would be left out of a minute marked done.)

A candle is the price of the market's first outcome (poly_markets.yes_token_id):
a trade in the other outcome at p counts as 1 - p. open and close are the
first and last trade in the minute by time, then by trade_id where two share a
second -- the venue stamps trades to the second. volume is shares, notional
the USDC paid, trades how many there were.

Minutes of markets discovery has not listed yet stay marked until it does.
"""

import argparse
import logging
import signal
import threading
import time
from datetime import datetime, timezone

import db
import store
from db import connection

log = logging.getLogger("aggregate")

BATCH = 5000                # minutes made per statement
IDLE_SECONDS = 5            # wait when nothing is marked

CLAIM = """
WITH d AS (
    SELECT d.condition_id, d.minute FROM poly_dirty d
    WHERE EXISTS (SELECT 1 FROM poly_markets m WHERE m.condition_id = d.condition_id)
    ORDER BY d.condition_id, d.minute
    LIMIT %(n)s FOR UPDATE OF d SKIP LOCKED
)
DELETE FROM poly_dirty p USING d
WHERE p.condition_id = d.condition_id AND p.minute = d.minute
RETURNING p.condition_id, p.minute
"""

MAKE = """
WITH d AS (
    SELECT * FROM unnest(%(c)s::text[], %(m)s::timestamptz[]) AS d(condition_id, minute)
), x AS (
    SELECT d.condition_id, d.minute, t.ts, t.trade_id, t.size, t.size * t.price AS paid,
           CASE WHEN m.yes_token_id IS NULL OR t.token_id = m.yes_token_id
                THEN t.price ELSE 1 - t.price END AS p
    FROM d
    JOIN poly_markets m ON m.condition_id = d.condition_id
    JOIN poly_trades t ON t.condition_id = d.condition_id
                      AND t.ts >= d.minute AND t.ts < d.minute + interval '1 minute'
), c AS (
    SELECT condition_id, minute,
           (array_agg(p ORDER BY ts, trade_id))[1] AS o, max(p) AS h, min(p) AS l,
           (array_agg(p ORDER BY ts DESC, trade_id DESC))[1] AS cl,
           sum(size) AS v, sum(paid) AS n, count(*) AS k
    FROM x GROUP BY condition_id, minute
), written AS (
    INSERT INTO poly_candles AS pc (condition_id, ts, open, high, low, close, volume,
                                    notional, trades, source)
    SELECT condition_id, minute, o, h, l, cl, v, n, k, 'trades' FROM c
    ORDER BY condition_id, minute
    ON CONFLICT (condition_id, ts) DO UPDATE SET
        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
        close = EXCLUDED.close, volume = EXCLUDED.volume, notional = EXCLUDED.notional,
        trades = EXCLUDED.trades, source = 'trades'
    RETURNING 1
)
SELECT count(*) FROM written
"""


def make(limit=BATCH):
    """One batch. Returns (minutes unmarked, candles written)."""
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(store.UNLIMITED_DECOMPRESSION)
            cur.execute(CLAIM, {"n": limit})
            claimed = cur.fetchall()
            if not claimed:
                return 0, 0
            cur.execute(MAKE, {"c": [r[0] for r in claimed], "m": [r[1] for r in claimed]})
            made = cur.fetchone()[0]
    return len(claimed), made


def drain(stop):
    """Every marked minute, batch after batch. Returns (minutes, candles)."""
    minutes = candles = 0
    while not stop.is_set():
        m, c = make()
        minutes, candles = minutes + m, candles + c
        if m < BATCH:
            break
    return minutes, candles


def rebuild(since, until, markets=None):
    """Mark [since, until) again -- every minute with a trade, for these
    markets or all -- so the next rounds make it from the trades."""
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO poly_dirty (condition_id, minute)
                SELECT DISTINCT condition_id, date_trunc('minute', ts) FROM poly_trades
                WHERE ts >= %s AND ts < %s AND (%s::text[] IS NULL OR condition_id = ANY(%s))
                ON CONFLICT DO NOTHING""", (since, until, markets, markets))
            return cur.rowcount


def backlog():
    r = db.query("SELECT count(*) AS n, EXTRACT(EPOCH FROM now() - min(minute)) AS age"
                 " FROM poly_dirty d WHERE EXISTS"
                 " (SELECT 1 FROM poly_markets m WHERE m.condition_id = d.condition_id)")[0]
    return r["n"], r["age"]


def run(stop):
    last_report, done = time.time(), [0, 0]
    while not stop.is_set():
        try:
            m, c = drain(stop)
            done = [done[0] + m, done[1] + c]
        except Exception as exc:
            log.warning("making candles failed: %s: %s", type(exc).__name__, exc)
            m = 0
        if time.time() - last_report >= 60:
            try:
                n, age = backlog()
                log.info("last minute: %s minutes made, %s candles; %s still marked%s",
                         f"{done[0]:,}", f"{done[1]:,}", f"{n:,}",
                         f", oldest {age / 60:.0f} min" if age else "")
            except Exception:
                pass
            last_report, done = time.time(), [0, 0]
        if not m:
            stop.wait(IDLE_SECONDS)
    log.info("stopped")


def parse_day(text):
    stamp = datetime.fromisoformat(text)
    return stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


def main():
    parser = argparse.ArgumentParser(description="Polymarket candles from trades.")
    parser.add_argument("--once", action="store_true", help="make every candle due, then exit")
    parser.add_argument("--rebuild", action="store_true", help="make --from .. --to again")
    parser.add_argument("--from", dest="since")
    parser.add_argument("--to", dest="until")
    parser.add_argument("--market", nargs="+", help="condition ids (default: all)")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    store.ensure_schema()
    if args.rebuild:
        if not args.since:
            parser.error("--rebuild needs --from")
        n = rebuild(parse_day(args.since),
                    parse_day(args.until) if args.until else datetime.now(timezone.utc),
                    args.market)
        log.info("%s minutes marked", f"{n:,}")
    if args.once or args.rebuild:
        m, c = drain(stop)
        log.info("%s minutes made, %s candles", f"{m:,}", f"{c:,}")
    else:
        run(stop)


if __name__ == "__main__":
    main()

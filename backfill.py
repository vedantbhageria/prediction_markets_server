"""History and repairs for Kalshi candles. Run by hand; stop and restart freely.

    python backfill.py                      # fill every market's history
    python backfill.py --report             # coverage summary, no downloads
    python backfill.py --verify             # candle volumes vs market volumes
    python backfill.py --verify --repair    # ...and re-download the mismatches
    python backfill.py --ticker KXBTC-... --refetch --from 2026-10-20 --to 2026-10-21

Which markets (all of them unless narrowed):
    --ticker T [T ...]   --event E [E ...]   --series S [S ...]   --category C
    --open / --closed    --limit N

What "history" means: every minute from when the market opened -- or
BACKFILL_DAYS ago, if that is later -- up to candles_fetched_from, where live
collection took over. It is fetched newest first and the bookkeeping moves
back after every request, so a stopped run resumes exactly where it was.
Markets that have never traded need no requests at all.

Every request uses the shared budget at the lowest priority, so this can run
alongside discover.py and live.py without ever pushing the total over it.
"""

import argparse
import logging
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import db
import fetch
import ratelimit
import settings

log = logging.getLogger("backfill")

ROUND = 2000                # markets planned at a time
WORKERS = 4


def floor_ts():
    return fetch.minute(time.time() - settings.BACKFILL_DAYS * 86400)


def ceil_minute(ts):
    return -(-int(ts) // 60) * 60


def selection(args):
    """SQL condition and parameters for the markets the arguments name."""
    where, params = [], []
    if args.ticker:
        where.append("m.ticker = ANY(%s)")
        params.append(args.ticker)
    if args.event:
        where.append("m.event_ticker = ANY(%s)")
        params.append(args.event)
    if args.series or args.category:
        where.append("m.event_ticker IN (SELECT event_ticker FROM kalshi_events WHERE "
                     + " AND ".join((["series_ticker = ANY(%s)"] if args.series else [])
                                    + (["category = %s"] if args.category else [])) + ")")
        params += ([args.series] if args.series else []) + ([args.category] if args.category else [])
    if args.open:
        where.append("m.close_time > now()")
    if args.closed:
        where.append("m.close_time <= now()")
    return (" AND ".join(where) or "true"), params


# ---------------------------------------------------------------------------
# Filling history
# ---------------------------------------------------------------------------

def mark_untraded(cond, params):
    """Markets whose volume was 0 when last measured have no trades before
    that, so their history is complete without asking Kalshi."""
    return db.execute(f"""
        UPDATE kalshi_tracking t
        SET candles_fetched_from = to_timestamp(GREATEST(
                %s, EXTRACT(EPOCH FROM date_trunc('minute', COALESCE(m.open_time, t.candles_fetched_from)))))
        FROM kalshi_markets m
        WHERE m.ticker = t.ticker AND t.volume = 0 AND {cond}
          AND t.candles_fetched_from > to_timestamp(GREATEST(
                %s, EXTRACT(EPOCH FROM date_trunc('minute', COALESCE(m.open_time, t.candles_fetched_from)))))""",
        [floor_ts()] + params + [floor_ts()])


def history_spans(cond, params, limit):
    """The next batch of markets with history still to fetch, busiest first:
    each one's newest missing stretch."""
    floor = floor_ts()
    rows = db.query(f"""
        SELECT t.ticker, t.candles_fetched_from AS have,
               GREATEST(%s, EXTRACT(EPOCH FROM date_trunc('minute', m.open_time))) AS want
        FROM kalshi_tracking t JOIN kalshi_markets m USING (ticker)
        WHERE {cond}
          AND t.candles_fetched_from > to_timestamp(GREATEST(%s,
                  EXTRACT(EPOCH FROM date_trunc('minute', COALESCE(m.open_time, t.candles_fetched_from)))))
        ORDER BY t.activity_score DESC, t.volume DESC, t.ticker
        LIMIT %s""", [floor] + params + [floor, limit])
    spans = []
    for r in rows:
        hi = ceil_minute(r["have"].timestamp())
        lo = int(r["want"]) if r["want"] is not None else floor
        if hi > lo:
            spans.append(fetch.split((r["ticker"], lo, hi), newest_first=True)[0])
    return spans


def run_requests(requests, stop):
    """Run planned requests in parallel. Returns (succeeded, failed, candles)."""
    ok = failed = candles = 0
    with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix="backfill") as pool:
        def one(request):
            with ratelimit.using("backfill"):
                return None if stop.is_set() else fetch.run(request, stop)
        for n in pool.map(one, requests):
            if n is None:
                failed += 1
            else:
                ok += 1
                candles += n
    return ok, failed, candles


def fill_history(cond, params, stop, limit=None, dry_run=False):
    marked = 0 if dry_run else mark_untraded(cond, params)
    if marked:
        log.info("%s markets never traded: history complete without a request", f"{marked:,}")
    total = [0, 0, 0]
    started, done_markets, quiet_rounds = time.time(), 0, 0
    while not stop.is_set():
        size = ROUND if limit is None else min(ROUND, limit - done_markets)
        if size <= 0:
            break
        spans = history_spans(cond, params, size)
        if not spans:
            break
        requests = fetch.plan(spans, newest_first=True)
        if dry_run:
            minutes = sum((hi - lo) // 60 for _, lo, hi in spans)
            print(f"next round: {len(spans):,} markets, {minutes:,} market-minutes,"
                  f" {len(requests):,} requests")
            return
        ok, failed, candles = run_requests(requests, stop)
        total = [total[0] + ok, total[1] + failed, total[2] + candles]
        if limit is not None:
            done_markets += len(spans)
        log.info("round: %s markets, %s requests (%s failed), %s candles; %s requests so far,"
                 " %.0f min", f"{len(spans):,}", f"{ok + failed:,}", f"{failed:,}",
                 f"{candles:,}", f"{total[0] + total[1]:,}", (time.time() - started) / 60)
        if ok == 0:
            # A network outage never gets here -- requests wait for the link
            # and resume where they were (venues.Link). This is Kalshi
            # refusing the requests themselves, so try again shortly, and give
            # up if it keeps happening.
            quiet_rounds += 1
            if quiet_rounds >= 10:
                log.warning("ten rounds in a row failed; giving up for now")
                break
            stop.wait(30)
        else:
            quiet_rounds = 0
    log.info("history: %s requests, %s failed, %s candles", f"{total[0] + total[1]:,}",
             f"{total[1]:,}", f"{total[2]:,}")


# ---------------------------------------------------------------------------
# Repairs
# ---------------------------------------------------------------------------

def parse_day(text):
    stamp = datetime.fromisoformat(text)
    if stamp.tzinfo is None:
        stamp = stamp.replace(tzinfo=timezone.utc)
    return int(stamp.timestamp())


def refetch(cond, params, lo, hi, stop):
    """Download [lo, hi) again for the selected markets, replacing what is
    stored. Clipped to each market's own lifetime."""
    rows = db.query(f"""
        SELECT m.ticker, EXTRACT(EPOCH FROM m.open_time) AS opened,
               EXTRACT(EPOCH FROM m.close_time) AS closed
        FROM kalshi_markets m WHERE {cond}""", params)
    now = fetch.minute(time.time() - settings.SETTLE_SECONDS)
    spans = []
    for r in rows:
        a = max(lo, fetch.minute(r["opened"]) if r["opened"] else lo)
        b = min(hi, now, ceil_minute(r["closed"]) + 120 if r["closed"] else now)
        if b > a:
            spans.append((r["ticker"], fetch.minute(a), ceil_minute(b)))
    requests = fetch.plan(spans)
    log.info("refetching %s markets in %s requests", f"{len(spans):,}", f"{len(requests):,}")
    ok, failed, candles = run_requests(requests, stop)
    log.info("refetch: %s requests, %s failed, %s candles", f"{ok + failed:,}",
             f"{failed:,}", f"{candles:,}")


# A reading is true somewhere between its ask time (less the observation lag)
# and its answer, and volume_measured_at is one of those two: so the trades it
# counts are all before volume_measured_at + this, and include everything
# before volume_measured_at - this. Allows two minutes for an answer.
def reading_slack():
    return settings.OBSERVATION_LAG + 180


def verify(cond, params, limit=None):
    """Compare stored candle volume with each market's own volume.

    Only markets whose whole life is in the window and whose candles cover it
    can be checked. Their candle volume from well before the reading can't be
    more than the reading, and their candle volume up to well after it can't
    be less. A finished market's candles must add up to its final volume
    exactly.
    """
    slack = reading_slack()
    rows = db.query(f"""
        SELECT t.ticker, t.volume, t.volume_measured_at AS measured, t.candles_fetched_to AS have_to,
               m.close_time <= t.volume_measured_at AS final,
               COALESCE((SELECT sum(c.volume) FROM kalshi_candles c WHERE c.ticker = t.ticker
                          AND c.ts < t.volume_measured_at - %s * interval '1 second'), 0) AS low,
               COALESCE((SELECT sum(c.volume) FROM kalshi_candles c WHERE c.ticker = t.ticker
                          AND c.ts < t.volume_measured_at + %s * interval '1 second'), 0) AS high,
               COALESCE((SELECT sum(c.volume) FROM kalshi_candles c WHERE c.ticker = t.ticker), 0) AS everything
        FROM kalshi_tracking t JOIN kalshi_markets m USING (ticker)
        WHERE {cond}
          AND m.open_time >= to_timestamp(%s)
          AND t.candles_fetched_from <= date_trunc('minute', m.open_time)
          AND t.candles_fetched_to >= t.volume_measured_at
        ORDER BY t.volume DESC
        {'LIMIT %s' if limit else ''}""",
        [slack, slack] + params + [floor_ts()] + ([limit] if limit else []))
    bad = []
    for r in rows:
        if r["final"]:
            good = r["everything"] == r["volume"]
        elif r["have_to"].timestamp() >= r["measured"].timestamp() + slack:
            good = r["low"] <= r["volume"] <= r["high"]
        else:
            good = r["low"] <= r["volume"]
        if not good:
            bad.append(r)
    print(f"checked {len(rows):,} fully covered markets: {len(rows) - len(bad):,} match,"
          f" {len(bad):,} do not")
    for r in bad[:20]:
        print(f"  {r['ticker']:<44} market volume {r['volume']:>12}   candles"
              f" {r['low']} .. {r['high']} (all {r['everything']})")
    return [r["ticker"] for r in bad]


def report(cond, params):
    floor = floor_ts()
    r = db.query(f"""
        SELECT count(*) AS markets,
               count(t.ticker) AS tracked,
               count(*) FILTER (WHERE t.ticker IS NOT NULL AND t.candles_fetched_from
                   <= to_timestamp(GREATEST(%s, EXTRACT(EPOCH FROM date_trunc('minute',
                          COALESCE(m.open_time, t.candles_fetched_from)))))) AS history_done,
               COALESCE(sum(GREATEST(0, EXTRACT(EPOCH FROM t.candles_fetched_from - to_timestamp(
                   GREATEST(%s, EXTRACT(EPOCH FROM COALESCE(m.open_time, t.candles_fetched_from))))) / 60)), 0)::bigint
                   AS history_minutes_left,
               count(*) FILTER (WHERE t.candles_fetched_to < t.volume_measured_at) AS behind,
               count(*) FILTER (WHERE t.volume = 0) AS never_traded
        FROM kalshi_markets m LEFT JOIN kalshi_tracking t USING (ticker)
        WHERE {cond}""", [floor, floor] + params)[0]
    print(f"markets               {r['markets']:>12,}")
    print(f"  tracked             {r['tracked']:>12,}   (the rest are waiting for their first scan)")
    print(f"  never traded        {r['never_traded']:>12,}")
    print(f"  history complete    {r['history_done']:>12,}")
    print(f"  history to fetch    {r['tracked'] - r['history_done']:>12,}   markets,"
          f" {r['history_minutes_left']:,} market-minutes")
    print(f"  behind the present  {r['behind']:>12,}   (traded since their last fetch; live.py's warm queue)")
    candles = db.query("SELECT count(*) AS n FROM kalshi_candles")[0]["n"]
    print(f"candles stored        {candles:>12,}")


def main():
    parser = argparse.ArgumentParser(description="Kalshi candle history and repairs.",
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     epilog=__doc__.split("Which markets")[0])
    parser.add_argument("--ticker", nargs="+")
    parser.add_argument("--event", nargs="+")
    parser.add_argument("--series", nargs="+")
    parser.add_argument("--category")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--open", action="store_true", help="only markets still open")
    group.add_argument("--closed", action="store_true", help="only markets already closed")
    parser.add_argument("--limit", type=int, help="at most this many markets")
    parser.add_argument("--report", action="store_true", help="coverage summary; no downloads")
    parser.add_argument("--verify", action="store_true",
                        help="check candle volumes against market volumes")
    parser.add_argument("--repair", action="store_true",
                        help="with --verify: re-download every market that does not match")
    parser.add_argument("--refetch", action="store_true",
                        help="re-download --from .. --to, replacing what is stored")
    parser.add_argument("--from", dest="since", help="ISO date or time, UTC unless given")
    parser.add_argument("--to", dest="until", help="ISO date or time, UTC unless given")
    parser.add_argument("--dry-run", action="store_true", help="show the next round; fetch nothing")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    ratelimit.DEFAULT = "backfill"
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    cond, params = selection(args)

    if args.report:
        report(cond, params)
    elif args.verify:
        bad = verify(cond, params, args.limit)
        if args.repair and bad:
            refetch("m.ticker = ANY(%s)", [bad], floor_ts(), int(time.time()), stop)
            verify("m.ticker = ANY(%s)", [bad])
    elif args.refetch:
        if not args.since:
            sys.exit("--refetch needs --from (and optionally --to)")
        until = parse_day(args.until) if args.until else int(time.time())
        refetch(cond, params, fetch.minute(parse_day(args.since)), ceil_minute(until), stop)
    else:
        fill_history(cond, params, stop, args.limit, args.dry_run)


if __name__ == "__main__":
    main()

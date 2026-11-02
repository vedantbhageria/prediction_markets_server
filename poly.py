"""Polymarket trades: read, store, book-keep.

Shared by trades.py (the feed of every trade, and catching up after a gap),
backfill.py (history) and the API's pushed backfills. aggregate.py turns the
stored trades into candles; nothing here makes candles, except the midpoint
fallback below.

Two ways to read trades, both newest first and both cut off by the venue at
the newest ~20,000 (see read_list):

    the feed      every trade on Polymarket -- about 15 a second, so the cut-off
                  is ~20 minutes back. trades.py reads it every few seconds.
    one market    every trade in one market. For all but the busiest that is
                  its whole life, in one request.

The venue answers from copies that run 2 to 6 minutes behind, each by its own
amount, so what a read covers is never taken from the clock:

  - the feed covers up to its newest trade, less POLY_FEED_LAG
  - a market's list covers up to the time it was asked, less POLY_LIST_LAG -- a
    market that has not traded lately says nothing about the copy's age

Bookkeeping is in poly_tracking and poly_feed (see schema.sql). Every writer
of poly_tracking locks the rows it touches in condition_id order first (LOCK),
so writers queue rather than deadlock.
"""

import hashlib
import logging
import time
from datetime import datetime, timezone

from psycopg2.extras import execute_values

import db
import settings
import store
import venues
from db import connection

log = logging.getLogger("poly")

LOCK = ("SELECT 1 FROM poly_tracking WHERE condition_id = ANY(%s)"
        " ORDER BY condition_id FOR UPDATE")
# Midpoint history is asked for a day at a time.
MID_STEP = 86400


def stamp(ts):
    return datetime.fromtimestamp(ts, timezone.utc)


def row(t):
    """The stored row for one of the venue's trades, or None if it lacks
    something a trade needs."""
    try:
        size, price = float(t["size"]), float(t["price"])
        ident = "|".join([str(t["transactionHash"]), str(t["asset"]), str(t.get("proxyWallet")),
                          str(t.get("side")), repr(size), repr(price)])
        return (t["conditionId"], stamp(int(t["timestamp"])),
                hashlib.md5(ident.encode()).hexdigest(), str(t["asset"]), t.get("side"),
                price, size, t.get("proxyWallet"), t.get("transactionHash"))
    except (KeyError, TypeError, ValueError):
        return None


# The page past the largest single request must share this many trades with
# it: a page from a staler copy of the venue would otherwise skip trades.
OVERLAP = 50
DEEP_TRIES = 4


def read_list(market, since, stop=None):
    """A trade list -- one market's, or with `market` None the feed of every
    trade -- newest first, back to `since` (unix time, or None for as far as
    it goes). Returns (trades, reached): `reached` is False if the list was cut
    off before getting there.

    The venue answers each request from one of several copies, each running
    a few minutes behind by its own amount, so pages from different requests
    do not line up. The list is read as one request from the newest trade,
    asked again larger until it reaches far enough (settings.POLY_LIST_SIZES):
    every answer is whole. Only past the largest is a second page read, at an
    offset, and it must overlap the first or it is asked for again."""
    out = []
    for size in settings.POLY_LIST_SIZES:
        out = venues.poly_trades(market, 0, size, stop)
        if len(out) < size or (since is not None and out and int(out[-1]["timestamp"]) < since):
            return out, True
    seen = {_key(t) for t in out}
    offset = min(venues.POLY_MAX_OFFSET, len(out) - OVERLAP)
    size = min(settings.POLY_LIST_SIZES[-1], venues.POLY_MAX_PAGE)
    for _ in range(DEEP_TRIES):
        page = venues.poly_trades(market, offset, size, stop)
        if not page:
            return out, True
        if any(_key(t) in seen for t in page):
            break
        if stop is not None and stop.wait(2):
            return out, False
    else:
        log.warning("trade list of %s: the page past %s trades never overlapped; stopping there",
                    market or "the feed", f"{len(out):,}")
        return out, False
    out += [t for t in page if _key(t) not in seen]
    if len(page) < size or (since is not None and int(page[-1]["timestamp"]) < since):
        return out, True
    return out, False


def _key(t):
    return (t.get("transactionHash"), t.get("asset"), t.get("proxyWallet"), t.get("side"),
            t.get("size"), t.get("price"))


def write_trades(cur, trades):
    """Store trades (the venue's dicts) not stored yet, and mark the minutes
    they fall in for aggregate.py. Returns how many were new."""
    rows = sorted({r[0:3]: r for r in map(row, trades) if r is not None}.values(),
                  key=lambda r: (r[0], r[1], r[2]))
    if not rows:
        return 0
    cur.execute(store.UNLIMITED_DECOMPRESSION)
    new = execute_values(cur, """
        INSERT INTO poly_trades (condition_id, ts, trade_id, token_id, side, price, size,
                                 wallet, tx_hash)
        VALUES %s ON CONFLICT DO NOTHING
        RETURNING condition_id, date_trunc('minute', ts)""", rows, page_size=1000, fetch=True)
    minutes = sorted(set(new))
    if minutes:
        # DO UPDATE, not DO NOTHING: a mark already there is locked until this
        # commits, so aggregate.py (SKIP LOCKED) cannot make the minute before
        # these trades are visible to it. Sorted, so writers lock in one order.
        execute_values(cur, "INSERT INTO poly_dirty (condition_id, minute) VALUES %s"
                            " ON CONFLICT (condition_id, minute) DO UPDATE SET minute = EXCLUDED.minute",
                       minutes, page_size=1000)
    return len(new)


# ---------------------------------------------------------------------------
# One market
# ---------------------------------------------------------------------------

def read_market(cid, since, stop=None):
    """Every trade of one market back to `since` (unix time) that the venue
    will list, newest first. Returns (trades, whole): `whole` is True if the
    list reached `since` or the market's first trade, False if it was cut off
    -- then nothing older than its oldest trade can be had this way."""
    return read_list(cid, since, stop)


def fetch_market(cid, stop=None, since=None):
    """Read one market's trades as far back as its history needs (or to
    `since`, unix time), store them and record what is now covered. Returns
    (new trades, whole) or None if the market is not tracked or the venue
    failed."""
    t = db.query("SELECT EXTRACT(EPOCH FROM history_from) AS h,"
                 " EXTRACT(EPOCH FROM trades_from) AS f,"
                 " EXTRACT(EPOCH FROM poly_covered_to(trades_to)) AS c"
                 " FROM poly_tracking WHERE condition_id = %s", (cid,))
    if not t:
        return None
    # Back to the start of its history if that is still missing, else to
    # where its coverage ends -- with a minute either side to spare.
    have_from, covered = float(t[0]["f"]), float(t[0]["c"])
    if since is None:
        since = (float(t[0]["h"]) if have_from > float(t[0]["h"]) else covered) - 60
    asked = time.time()
    try:
        trades, whole = read_market(cid, since, stop)
    except venues.VenueError as exc:
        log.warning("trades of %s: %s", cid, exc)
        return None
    except Exception as exc:
        log.warning("trades of %s: %s: %s", cid, type(exc).__name__, exc)
        return None
    to = asked - settings.POLY_LIST_LAG
    oldest = min((int(x["timestamp"]) for x in trades), default=None)
    # What this read covers: [lo, to). The whole list back to `since` covers
    # from there; a list cut off covers from just after its oldest trade (the
    # second it stopped in may have more).
    lo = since if whole else oldest + 1
    with connection() as conn:
        with conn.cursor() as cur:
            n = write_trades(cur, trades)
            record(cur, cid, lo, to, whole and since <= float(t[0]["h"]))
    return n, whole


def record(cur, cid, lo, hi, from_start):
    """[lo, hi) of this market's trades are stored. Merged into its coverage
    where the two touch; a read that does not touch it replaces it (the newer
    stretch is the one live collection carries on from) and the hole becomes
    history to fill. `from_start`: the read went back to history_from."""
    cur.execute(LOCK, ([cid],))
    cur.execute("""
        UPDATE poly_tracking t SET
          trades_from = CASE WHEN %(start)s THEN t.history_from
                             WHEN to_timestamp(%(lo)s) <= poly_covered_to(t.trades_to)
                                  AND to_timestamp(%(hi)s) >= t.trades_from
                             THEN LEAST(t.trades_from, to_timestamp(%(lo)s))
                             ELSE to_timestamp(%(lo)s) END,
          trades_to = CASE WHEN to_timestamp(%(lo)s) <= poly_covered_to(t.trades_to)
                                AND to_timestamp(%(hi)s) >= t.trades_from
                           THEN GREATEST(poly_covered_to(t.trades_to), to_timestamp(%(hi)s))
                           ELSE to_timestamp(%(hi)s) END
        WHERE t.condition_id = %(cid)s""", {"cid": cid, "lo": lo, "hi": hi, "start": from_start})


def fill_mid(cid, stop=None):
    """History older than the venue will list trades for: the per-minute
    order-book midpoint over [history_from, trades_from), stored as candles
    marked source 'mid' (one price, no volume). Never replaces a candle made
    from trades. Returns candles written, or None on failure."""
    t = db.query("""SELECT m.yes_token_id, EXTRACT(EPOCH FROM t.history_from) AS h,
                           EXTRACT(EPOCH FROM t.trades_from) AS f
                    FROM poly_tracking t JOIN poly_markets m USING (condition_id)
                    WHERE t.condition_id = %s""", (cid,))
    if not t or not t[0]["yes_token_id"]:
        return None
    lo, hi = int(t[0]["h"]), int(t[0]["f"])
    points = []
    try:
        for a in range(lo, hi, MID_STEP):
            points += venues.poly_prices_history(t[0]["yes_token_id"], a, min(hi, a + MID_STEP), stop)
    except Exception as exc:
        log.warning("midpoints of %s: %s: %s", cid, type(exc).__name__, exc)
        return None
    minutes = {}
    for ts, p in points:
        if lo <= ts < hi:
            minutes[ts // 60 * 60] = p
    rows = sorted((cid, stamp(m), p, p, p, p) for m, p in minutes.items())
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(store.UNLIMITED_DECOMPRESSION)
            if rows:
                execute_values(cur, """
                    INSERT INTO poly_candles (condition_id, ts, open, high, low, close, source)
                    SELECT v.c, v.ts::timestamptz, v.o::numeric, v.h::numeric, v.l::numeric,
                           v.cl::numeric, 'mid'
                    FROM (VALUES %s) AS v(c, ts, o, h, l, cl)
                    WHERE EXISTS (SELECT 1 FROM poly_markets m WHERE m.condition_id = v.c)
                    ON CONFLICT DO NOTHING""", rows, page_size=1000)
            cur.execute(LOCK, ([cid],))
            cur.execute("UPDATE poly_tracking SET mid_to = trades_from WHERE condition_id = %s",
                        (cid,))
    return len(rows)


# ---------------------------------------------------------------------------
# The feed
# ---------------------------------------------------------------------------

def feed_state():
    """(covered_from, covered_to) as unix times, or None before the first read."""
    r = db.query("SELECT EXTRACT(EPOCH FROM covered_from) AS a, EXTRACT(EPOCH FROM covered_to) AS b"
                 " FROM poly_feed")
    return (float(r[0]["a"]), float(r[0]["b"])) if r else None


def read_feed(since, stop=None):
    """The feed back to `since` (unix time), newest first, or as far as it
    goes. Returns (trades, reached)."""
    return read_list(None, since, stop)


def poll_feed(stop=None, overlap=30):
    """One read of the feed: everything since the last one (less `overlap`
    seconds, for trades that reach it out of order), stored, and the feed's
    coverage moved on. If the feed no longer reaches back that far, there
    was a gap: every market the old coverage carried keeps it (its trades_to
    moves up to where the feed stopped) and the feed starts again from what
    it reaches now. Returns (new trades, trades read, gap or None)."""
    state = feed_state()
    trades, reached = read_feed(state[1] - overlap if state else None, stop)
    if not trades:
        return 0, 0, None
    newest = max(int(x["timestamp"]) for x in trades)
    oldest = min(int(x["timestamp"]) for x in trades)
    # Continuous if the read got back to where the last one stopped, even if
    # not through all of the overlap.
    reached = reached or (state is not None and oldest <= state[1])
    to = newest - settings.POLY_FEED_LAG
    gap = None
    with connection() as conn:
        with conn.cursor() as cur:
            n = write_trades(cur, trades)
            if state and reached:
                cur.execute("UPDATE poly_feed SET covered_to = GREATEST(covered_to, to_timestamp(%s)),"
                            " updated_at = now()", (to,))
            else:
                if state:
                    gap = (state[1], oldest + 1)
                    cur.execute("SELECT condition_id FROM poly_tracking"
                                " WHERE trades_to >= to_timestamp(%s) AND trades_to < to_timestamp(%s)"
                                " ORDER BY condition_id FOR UPDATE", state)
                    cur.execute("UPDATE poly_tracking SET trades_to = to_timestamp(%s)"
                                " WHERE trades_to >= to_timestamp(%s) AND trades_to < to_timestamp(%s)",
                                (state[1], state[0], state[1]))
                cur.execute("""
                    INSERT INTO poly_feed (id, covered_from, covered_to)
                    VALUES (true, to_timestamp(%s), to_timestamp(%s))
                    ON CONFLICT (id) DO UPDATE SET covered_from = EXCLUDED.covered_from,
                        covered_to = EXCLUDED.covered_to, updated_at = now()""",
                            (oldest + 1, max(to, oldest + 1)))
    return n, len(trades), gap


# ---------------------------------------------------------------------------
# Which markets need reading
# ---------------------------------------------------------------------------

# (mid_to is NULL for most markets: the COALESCE keeps "not done" from
# becoming NULL, which NOT would leave NULL and every filter would drop.)
HISTORY_DONE = "(t.trades_from <= t.history_from OR COALESCE(t.mid_to >= t.trades_from, false))"
# Traded at a time the market's coverage does not reach -- but only judged
# once the feed has read past that reading, so a market the feed is carrying
# is never counted while the feed catches up to it.
BEHIND = """(t.moved_at IS NOT NULL AND t.moved_at > poly_covered_to(t.trades_to)
             AND t.moved_at <= COALESCE((SELECT covered_to FROM poly_feed), '-infinity'))"""
TRADED = "(t.volume > 0 OR t.volume_24h > 0 OR t.moved_at IS NOT NULL)"


def behind(limit):
    """Markets with history complete that traded where their coverage does
    not reach: the gap catch-up, busiest first."""
    return [r["condition_id"] for r in db.query(f"""
        SELECT t.condition_id FROM poly_tracking t
        WHERE {HISTORY_DONE} AND {BEHIND}
        ORDER BY t.volume_24h DESC, t.condition_id LIMIT %s""", (limit,))]


def history_todo(limit, cond="true", params=()):
    """Markets whose history is still to read, busiest first."""
    return [r["condition_id"] for r in db.query(f"""
        SELECT t.condition_id FROM poly_tracking t JOIN poly_markets m USING (condition_id)
        WHERE NOT {HISTORY_DONE} AND {TRADED} AND {cond}
        ORDER BY t.volume_24h DESC, t.volume DESC, t.condition_id LIMIT %s""",
        list(params) + [limit])]


def mark_untraded(cond="true", params=()):
    """Markets never seen trading have no history to read."""
    with db.cursor(dict_rows=False) as cur:
        cur.execute(f"""SELECT 1 FROM poly_tracking t JOIN poly_markets m USING (condition_id)
                        WHERE NOT {HISTORY_DONE} AND NOT {TRADED} AND {cond}
                        ORDER BY t.condition_id FOR UPDATE OF t""", list(params))
        cur.execute(f"""UPDATE poly_tracking t SET trades_from = t.history_from
                        FROM poly_markets m WHERE m.condition_id = t.condition_id
                          AND NOT {HISTORY_DONE} AND NOT {TRADED} AND {cond}""", list(params))
        return cur.rowcount

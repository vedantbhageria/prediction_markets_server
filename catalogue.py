"""The markets store: which series, events and markets exist right now.

Every value written here is the venue's own -- nothing is worked out from it
yet. The columns reserved for derived data (strike, Polymarket's strike_type,
stated_date, an event's category and market_count) are left empty.

Rows go down the chain the schema enforces, parent first:

    Kalshi:      kalshi_series -> kalshi_events -> kalshi_markets
    Polymarket:                   poly_events   -> poly_markets

Writes only touch rows whose watched fields changed, so a re-run over data
already stored costs reads, not writes.

Kalshi reads also record what they saw -- each market's volume, and when
Kalshi was asked for it -- in kalshi_observations, tagged with the discovery
scan they belong to. activity.py turns those into activity scores.
"""

import queue
import threading
import time
from datetime import datetime, timedelta, timezone

from psycopg2.extras import Json, execute_values

import db
import settings
import store
import venues
from db import connection
from load_db import num

SERIES_COLS = ["series_ticker", "category", "title", "frequency"]
EVENT_COLS = ["event_ticker", "series_ticker", "title", "sub_title", "category",
              "mutually_exclusive", "strike_period", "raw"]
EVENT_WATCH = ["title", "sub_title", "category", "mutually_exclusive", "strike_period"]
MARKET_COLS = ["ticker", "event_ticker", "title", "status", "result", "open_time",
               "close_time", "strike_type", "floor_strike", "cap_strike",
               "volume", "open_interest", "raw"]
MARKET_WATCH = ["status", "result", "volume", "open_interest", "close_time", "title"]

POLY_EVENT_COLS = ["event_id", "slug", "title", "tags"]
POLY_EVENT_WATCH = ["slug", "title", "tags"]
POLY_COLS = ["condition_id", "event_id", "slug", "question", "yes_token_id",
             "end_date", "closed", "volume", "liquidity", "raw"]
POLY_WATCH = ["closed", "volume", "liquidity", "end_date", "question"]

# Most stored markets re-read per pass after leaving a venue's open list.
STALE_MAX = 5000


def _expired(stamp):
    """True if this close time is already past retention.

    Listings still show markets that closed long ago; storing those only for
    the next prune to delete them would churn the table every pass.
    """
    if not stamp or settings.MARKET_RETENTION_DAYS <= 0:
        return False
    try:
        closed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return False
    return closed < datetime.now(timezone.utc) - timedelta(days=settings.MARKET_RETENTION_DAYS)


def _stopped(stop):
    return stop is not None and stop.is_set()


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

def _market_row(m):
    return (m["ticker"], m.get("event_ticker"), m.get("title"), m.get("status"),
            m.get("result"), m.get("open_time"), m.get("close_time"),
            m.get("strike_type"), num(m.get("floor_strike")), num(m.get("cap_strike")),
            num(m.get("volume_fp")), num(m.get("open_interest_fp")), Json(m))


def _kalshi_rows(page, categories=None):
    """(events, markets) from a page of events with nested markets."""
    events, markets = [], []
    wanted = categories if categories is not None else settings.MARKET_CATEGORIES
    for event in page:
        if wanted and event.get("category") not in wanted:
            continue
        nested = [m for m in event.get("markets") or []
                  if not m["ticker"].startswith("KXMVE")
                  and not _expired(m.get("close_time"))]
        if not nested:
            continue
        payload = {k: v for k, v in event.items() if k != "markets"}
        # Every event names its series; fall back to the ticker prefix rather
        # than lose the event if one ever does not.
        series = event.get("series_ticker") or event["event_ticker"].split("-")[0]
        events.append((event["event_ticker"], series, event.get("title"),
                       event.get("sub_title"), event.get("category"),
                       event.get("mutually_exclusive"), event.get("strike_period") or None,
                       Json(payload)))
        markets += [_market_row(m) for m in nested]
    return events, markets


VOLUME = MARKET_COLS.index("volume")


def _observe(cur, markets, scan, asked, answered):
    """Record each market's volume and the window it was true in: no earlier
    than just before the request (less OBSERVATION_LAG, in case Kalshi's
    listing runs a little behind its trades) and no later than the answer.
    activity.py needs both ends to treat "volume unchanged" as "no trades"."""
    if scan is None or not markets:
        return
    lo = datetime.fromtimestamp(asked - settings.OBSERVATION_LAG, timezone.utc)
    hi = datetime.fromtimestamp(answered, timezone.utc)
    rows = [(scan, m[0], m[VOLUME], lo, hi) for m in markets if m[VOLUME] is not None]
    execute_values(cur, "INSERT INTO kalshi_observations"
                        " (scan_id, ticker, volume, asked_at, answered_at) VALUES %s",
                   rows, page_size=1000)


def _write_kalshi(events, markets, scan=None, asked=None, answered=None):
    """Series (placeholders until the daily series sync), events, markets --
    and, inside a scan, what was observed."""
    with connection() as conn:
        with conn.cursor() as cur:
            series = sorted({e[1] for e in events if e[1]})
            if series:
                cur.execute("INSERT INTO kalshi_series (series_ticker)"
                            " SELECT unnest(%s::text[]) ON CONFLICT DO NOTHING", (series,))
            store.upsert(cur, "kalshi_events", EVENT_COLS, events, "event_ticker", EVENT_WATCH)
            written = store.upsert(cur, "kalshi_markets", MARKET_COLS, markets,
                                   "ticker", MARKET_WATCH)
            _observe(cur, markets, scan, asked, answered)
            return written


def _timed(pages):
    """(page, cursor, asked, answered) from a page generator: just before the
    request that produced the page (retries included) and just after it came
    back, so the page describes some moment in between."""
    pages = iter(pages)
    while True:
        asked = time.time()
        try:
            page, cursor = next(pages)
        except StopIteration:
            return
        yield page, cursor, asked, time.time()


def _ahead(items, stop=None):
    """Iterate `items` with the next one already being fetched.

    The pages come from a background thread one step ahead, so the next
    request is in flight while this page is written to the database. An error
    in the fetching thread is raised here, where the pages are used.
    """
    q = queue.Queue(maxsize=1)
    finished = threading.Event()
    end = object()

    def produce():
        try:
            for item in items:
                while not finished.is_set():
                    try:
                        q.put((item, None), timeout=1)
                        break
                    except queue.Full:
                        pass
                if finished.is_set():
                    return
            q.put((end, None))
        except BaseException as exc:
            q.put((end, exc))

    threading.Thread(target=produce, daemon=True, name="pages-ahead").start()
    try:
        while True:
            try:
                item, exc = q.get(timeout=1)
            except queue.Empty:
                if _stopped(stop):
                    return
                continue
            if item is end:
                if exc is not None:
                    raise exc
                return
            yield item
    finally:
        finished.set()


def sync_series(stop=None):
    """Kalshi's full series list, which also fills in placeholder series."""
    rows = [(s["ticker"], s.get("category"), s.get("title"), s.get("frequency"))
            for s in venues.kalshi_series()]
    with connection() as conn:
        with conn.cursor() as cur:
            return store.upsert(cur, "kalshi_series", SERIES_COLS, rows,
                                "series_ticker", ["category", "title", "frequency"])


def sync_kalshi(stop=None, categories=None, scan=None, progress=None, checkpoint=None):
    """Everything open now, then a re-read of stored markets that left the
    open list since -- which is how their final status and result land.

    Resumable like the closed import: `progress` records the cursor of the
    next page and `checkpoint` is called after every page, so a run cut short
    starts again at the page after the last one written.
    """
    progress = progress if progress is not None else {}
    seen, total = set(), 0
    if progress.get("cursor") != "done":
        try:
            pages = _ahead(_timed(venues.kalshi_events("open", None, progress.get("cursor"))), stop)
            for page, cursor, asked, answered in pages:
                if _stopped(stop):
                    return total
                events, markets = _kalshi_rows(page, categories)
                seen.update(row[0] for row in markets)
                total += _write_kalshi(events, markets, scan, asked, answered)
                progress["cursor"] = cursor or "done"
                if checkpoint:
                    checkpoint()
        except venues.VenueError as exc:
            if not progress.get("cursor") or exc.status != 400:
                raise
            # Kalshi no longer accepts the saved cursor: read the list again.
            progress.pop("cursor", None)
            if checkpoint:
                checkpoint()
            return total + sync_kalshi(stop, categories, scan, progress, checkpoint)
    if _stopped(stop):
        return total

    # Markets this scan has already read, including pages read before a
    # restart: they are in its observations.
    if scan is not None:
        seen.update(r["ticker"] for r in db.query(
            "SELECT DISTINCT ticker FROM kalshi_observations WHERE scan_id = %s", (scan,)))
    stale = [r["ticker"] for r in db.query(
        "SELECT ticker FROM kalshi_markets WHERE COALESCE(result, '') = ''"
        " ORDER BY close_time DESC") if r["ticker"] not in seen][:STALE_MAX]
    asked = time.time()
    rows = [_market_row(m) for m in venues.kalshi_markets(stale, timeout=30, attempts=5)]
    answered = time.time()
    with connection() as conn:
        with conn.cursor() as cur:
            total += store.upsert(cur, "kalshi_markets", MARKET_COLS, rows,
                                  "ticker", MARKET_WATCH)
            _observe(cur, rows, scan, asked, answered)
    return total


def import_kalshi_closed(stop=None, since=None, progress=None, checkpoint=None, scan=None):
    """Every market whose event closed or settled since `since` (unix time).

    `progress` is a dict the caller persists, and `checkpoint` is called after
    every page so it can: it records which listing and cursor the import has
    reached, so a stopped import resumes from the last page it finished
    instead of starting over.
    """
    progress = progress if progress is not None else {}
    total = 0
    statuses = ["closed", "settled"]
    # `status` is the listing to (re)start, `cursor` the page inside it; a
    # missing cursor means the start of that listing.
    if progress.get("status") == "done":
        return total
    start = statuses.index(progress["status"]) if progress.get("status") in statuses else 0
    for i, status in enumerate(statuses[start:], start):
        cursor = progress.get("cursor") if progress.get("status") == status else None
        for page, cursor, asked, answered in _ahead(_timed(venues.kalshi_events(status, since,
                                                                                 cursor)), stop):
            if _stopped(stop):
                return total
            total += _write_kalshi(*_kalshi_rows(page), scan, asked, answered)
            # After the last page, point at the next listing instead.
            nxt = (status, cursor) if cursor else (statuses[i + 1] if i + 1 < len(statuses) else "done", None)
            progress.update(status=nxt[0], cursor=nxt[1])
            if checkpoint:
                checkpoint()
    return total


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

def _poly_row(m, event_id):
    return (m["conditionId"], event_id, m.get("slug"), m.get("question"),
            venues.yes_token(m), m.get("endDate"), m.get("closed"),
            num(m.get("volume")), num(m.get("liquidity")), Json(m))


def _poly_rows(page):
    """(events, markets) from a page of events with nested markets."""
    events, markets = [], []
    for event in page:
        nested = [m for m in event.get("markets") or [] if m.get("conditionId")
                  and not (m.get("closed") and _expired(m.get("endDate")))]
        if not nested:
            continue
        tags = [t.get("slug") for t in event.get("tags") or [] if t.get("slug")]
        events.append((str(event["id"]), event.get("slug"), event.get("title"), tags))
        markets += [_poly_row(m, str(event["id"])) for m in nested]
    return events, markets


def _write_poly(events, markets):
    """Events first, then the markets that point at them."""
    with connection() as conn:
        with conn.cursor() as cur:
            store.upsert(cur, "poly_events", POLY_EVENT_COLS, events, "event_id",
                         POLY_EVENT_WATCH)
            return store.upsert(cur, "poly_markets", POLY_COLS, markets,
                                "condition_id", POLY_WATCH)


def sync_poly(stop=None, scan=None):
    """Everything open now, then a re-read of stored markets that left the
    open list since -- which is how their closed flag gets set."""
    seen, total = set(), 0
    for page, _ in venues.poly_events(closed=False):
        if _stopped(stop):
            return total
        events, markets = _poly_rows(page)
        seen.update(row[0] for row in markets)
        total += _write_poly(events, markets)

    stale = {r["condition_id"]: r["event_id"] for r in db.query(
        "SELECT condition_id, event_id FROM poly_markets"
        " WHERE closed IS NOT TRUE ORDER BY end_date NULLS LAST")
        if r["condition_id"] not in seen}
    rows = [_poly_row(m, stale[m["conditionId"]])
            for m in venues.poly_markets(list(stale)[:STALE_MAX])
            if m.get("conditionId") in stale]
    with connection() as conn:
        with conn.cursor() as cur:
            total += store.upsert(cur, "poly_markets", POLY_COLS, rows,
                                  "condition_id", POLY_WATCH)
    return total


def import_poly_closed(stop=None, since=None, progress=None, checkpoint=None, scan=None):
    """Every closed event whose end date is after `since` (unix time), with
    its markets. Resumable through `progress`, as for Kalshi."""
    progress = progress if progress is not None else {}
    total = 0
    if progress.get("cursor") == "done":
        return total
    for page, cursor in venues.poly_events(closed=True, end_date_min=since,
                                           cursor=progress.get("cursor")):
        if _stopped(stop):
            return total
        total += _write_poly(*_poly_rows(page))
        progress.update(cursor=cursor or "done")
        if checkpoint:
            checkpoint()
    return total


SYNC = {"kalshi": sync_kalshi, "poly": sync_poly}
IMPORT_CLOSED = {"kalshi": import_kalshi_closed, "poly": import_poly_closed}

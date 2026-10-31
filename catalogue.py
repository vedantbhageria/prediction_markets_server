"""The markets store: which series, events and markets exist right now.

Every value written here is the venue's own -- nothing is worked out from it
yet. The columns reserved for derived data (strike, Polymarket's strike_type,
stated_date, an event's category and market_count) are left empty.

Rows go down the chain the schema enforces, parent first:

    Kalshi:      kalshi_series -> kalshi_events -> kalshi_markets
    Polymarket:                   poly_events   -> poly_markets

Writes only touch rows whose watched fields changed, so a re-run over data
already stored costs reads, not writes.
"""

from datetime import datetime, timedelta, timezone

from psycopg2.extras import Json

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


def _write_kalshi(events, markets):
    """Series (placeholders until the daily series sync), events, markets."""
    with connection() as conn:
        with conn.cursor() as cur:
            series = sorted({e[1] for e in events if e[1]})
            if series:
                cur.execute("INSERT INTO kalshi_series (series_ticker)"
                            " SELECT unnest(%s::text[]) ON CONFLICT DO NOTHING", (series,))
            store.upsert(cur, "kalshi_events", EVENT_COLS, events, "event_ticker", EVENT_WATCH)
            return store.upsert(cur, "kalshi_markets", MARKET_COLS, markets,
                                "ticker", MARKET_WATCH)


def sync_series(stop=None):
    """Kalshi's full series list, which also fills in placeholder series."""
    rows = [(s["ticker"], s.get("category"), s.get("title"), s.get("frequency"))
            for s in venues.kalshi_series()]
    with connection() as conn:
        with conn.cursor() as cur:
            return store.upsert(cur, "kalshi_series", SERIES_COLS, rows,
                                "series_ticker", ["category", "title", "frequency"])


def sync_kalshi(stop=None, categories=None):
    """Everything open now, then a re-read of stored markets that left the
    open list since -- which is how their final status and result land."""
    seen, total = set(), 0
    for page, _ in venues.kalshi_events("open"):
        if _stopped(stop):
            return total
        events, markets = _kalshi_rows(page, categories)
        seen.update(row[0] for row in markets)
        total += _write_kalshi(events, markets)

    stale = [r["ticker"] for r in db.query(
        "SELECT ticker FROM kalshi_markets WHERE COALESCE(result, '') = ''"
        " ORDER BY close_time DESC") if r["ticker"] not in seen][:STALE_MAX]
    rows = [_market_row(m) for m in venues.kalshi_markets(stale, timeout=30, attempts=5)]
    with connection() as conn:
        with conn.cursor() as cur:
            total += store.upsert(cur, "kalshi_markets", MARKET_COLS, rows,
                                  "ticker", MARKET_WATCH)
    return total


def import_kalshi_closed(stop=None, since=None, progress=None, checkpoint=None):
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
        for page, cursor in venues.kalshi_events(status, since, cursor):
            if _stopped(stop):
                return total
            total += _write_kalshi(*_kalshi_rows(page))
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


def sync_poly(stop=None):
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


def import_poly_closed(stop=None, since=None, progress=None, checkpoint=None):
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

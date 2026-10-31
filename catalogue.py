"""The markets store: which series, events and markets exist right now.

Replaces the run-by-hand catalogue scrapers. Each sync lists what is open on
both venues, picks up the final state of anything that just closed, and writes
it down the chain the schema enforces:

    Kalshi:      kalshi_series -> kalshi_events -> kalshi_markets
    Polymarket:                   poly_events   -> poly_markets

so a parent row is always written before the rows that point at it.
"""

import re
import time
from datetime import datetime, timedelta, timezone

import requests
from psycopg2.extras import Json

import db
import settings
import store
import venues
from db import connection
# The derivations are the project's own and must not drift from the loader's.
from load_db import category_from_tags, num, poly_deadline, poly_strike
from polymarket import get_markets as poly_list_markets

SERIES_COLS = ["series_ticker", "category", "title", "frequency"]
EVENT_COLS = ["event_ticker", "series_ticker", "title", "sub_title", "category",
              "mutually_exclusive", "strike_period", "raw"]
EVENT_WATCH = ["title", "sub_title", "category", "mutually_exclusive", "strike_period"]
MARKET_COLS = ["ticker", "event_ticker", "title", "status", "result", "open_time",
               "close_time", "strike_type", "floor_strike", "cap_strike", "strike",
               "volume", "open_interest", "raw"]
MARKET_WATCH = ["status", "result", "volume", "open_interest", "close_time", "title"]

POLY_EVENT_COLS = ["event_id", "slug", "title", "category", "tags", "market_count"]
POLY_COLS = ["condition_id", "event_id", "slug", "question", "yes_token_id",
             "end_date", "closed", "strike_type", "strike", "stated_date",
             "volume", "liquidity", "raw"]
POLY_WATCH = ["closed", "volume", "liquidity", "end_date", "question"]

# Most stored markets re-read per sync after leaving a venue's open list.
STALE_MAX = 5000


def _expired(stamp):
    """True if this close time is already past retention.

    Open events still list markets that closed long ago; storing those only
    for the next prune to delete them would churn the table every sync.
    """
    if not stamp or settings.MARKET_RETENTION_DAYS <= 0:
        return False
    try:
        closed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return False
    return closed < datetime.now(timezone.utc) - timedelta(days=settings.MARKET_RETENTION_DAYS)


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

def _market_row(m, event_ticker):
    """A market exactly as Kalshi sent it, plus `strike`: the one level the
    question turns on, so it can be lined up against Polymarket."""
    floor, cap = num(m.get("floor_strike")), num(m.get("cap_strike"))
    return (m["ticker"], m.get("event_ticker") or event_ticker, m.get("title"),
            m.get("status"), m.get("result"), m.get("open_time"), m.get("close_time"),
            m.get("strike_type"), floor, cap, floor if floor is not None else cap,
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
        markets += [_market_row(m, event["event_ticker"]) for m in nested]
    return events, markets


def _write_kalshi(cur, events, markets):
    """Series (placeholders if not listed yet), then events, then markets."""
    series = {e[1] for e in events if e[1]}
    if series:
        cur.execute("INSERT INTO kalshi_series (series_ticker)"
                    " SELECT unnest(%s::text[]) ON CONFLICT DO NOTHING", (sorted(series),))
    store.upsert(cur, "kalshi_events", EVENT_COLS, events, "event_ticker", EVENT_WATCH)
    return store.upsert(cur, "kalshi_markets", MARKET_COLS, markets, "ticker", MARKET_WATCH)


def sync_kalshi(stop=None, categories=None):
    """Open events, then a re-read of stored markets that dropped off that
    list, which is how results and final volumes land.

    Markets that closed without ever being seen open are import_kalshi_closed's
    job, not this one's.
    """
    seen, total = set(), 0
    for page in venues.kalshi_events("open"):
        if stop is not None and stop.is_set():
            return total
        events, markets = _kalshi_rows(page, categories)
        seen.update(row[0] for row in markets)
        with connection() as conn:
            with conn.cursor() as cur:
                total += _write_kalshi(cur, events, markets)

    stale = [r["ticker"] for r in db.query(
        "SELECT ticker FROM kalshi_markets WHERE COALESCE(result, '') = ''"
        " ORDER BY close_time DESC") if r["ticker"] not in seen]
    rows = [_market_row(m, None)
            for m in venues.kalshi_markets(stale[:STALE_MAX], timeout=30, attempts=5)]
    with connection() as conn:
        with conn.cursor() as cur:
            total += store.upsert(cur, "kalshi_markets", MARKET_COLS, rows,
                                  "ticker", MARKET_WATCH)
    return total


def sync_series():
    """Kalshi's series list. Changes rarely; refreshed once a day. Also fills
    in any placeholder series the event sync created."""
    rows = [(s["ticker"], s.get("category"), s.get("title"), s.get("frequency"))
            for s in venues.kalshi_series()]
    with connection() as conn:
        with conn.cursor() as cur:
            return store.upsert(cur, "kalshi_series", SERIES_COLS, rows,
                                "series_ticker", ["category", "title", "frequency"])


def import_kalshi_closed(stop=None, since=None):
    """Every market whose event closed or settled since `since` (unix time).

    Run over the whole retention window on start, so the store holds the last
    30 days of closed markets even after the collector was down. The venue
    lists events, not markets, and an event can still list markets that closed
    long ago; those are dropped by _kalshi_rows before they are stored.
    """
    total = 0
    for status in ("closed", "settled"):
        for page in venues.kalshi_events(status, since):
            if stop is not None and stop.is_set():
                return total
            events, markets = _kalshi_rows(page)
            with connection() as conn:
                with conn.cursor() as cur:
                    total += _write_kalshi(cur, events, markets)
    return total


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

_BETWEEN = re.compile(r"between\s+\$\s*[\d,.]+\s*[kmb]?\s+and\s+\$", re.I)


def poly_strike_type(question):
    """Kalshi's strike_type vocabulary, worked out from Polymarket's wording.

    Polymarket has no such field, so this is best-effort: "at or above" and
    "above" read the same in a question, so the result is greater or less,
    never the _or_equal forms.
    """
    q = (question or "").lower()
    if poly_strike(q) is None:
        return None
    if _BETWEEN.search(q):
        return "between"
    if "all time high" in q or "all-time high" in q or "up or down" in q:
        return "custom"
    if any(w in q for w in ("dip to", "fall to", "drop to", "below", "under")):
        return "less"
    if any(w in q for w in ("reach", "hit", "above", "over", "exceed")):
        return "greater"
    return "custom"


def _poly_row(m, event_id):
    """A market as Polymarket sent it, plus the fields worked out from it."""
    # The old list scraper writes its own keys into the payload; keep `raw`
    # as the venue sent it.
    payload = {k: v for k, v in m.items() if k not in ("asset", "tags", "event_id", "event_slug")}
    question = m.get("question")
    return (m["conditionId"], event_id, m.get("slug"), question, venues.yes_token(m),
            m.get("endDate"), m.get("closed"), poly_strike_type(question),
            poly_strike(question), poly_deadline(question, m.get("endDate")),
            num(m.get("volume")), num(m.get("liquidity")), Json(payload))


def _event_row(event_id, slug, title, tags, count):
    return (event_id, slug, title, category_from_tags(tags), tags, count)


def _write_poly(cur, events, markets):
    """Events first, then the markets that point at them."""
    store.upsert(cur, "poly_events", POLY_EVENT_COLS, events, "event_id",
                 ["title", "category", "market_count"])
    return store.upsert(cur, "poly_markets", POLY_COLS, markets, "condition_id", POLY_WATCH)


def sync_poly(stop=None):
    """The busiest open events, then a re-read of stored markets that dropped
    off that list -- which is how a market's closed flag gets set."""
    seen, total = set(), 0
    for page in venues.poly_events(settings.POLY_MAX_EVENTS):
        if stop is not None and stop.is_set():
            return total
        events, markets = [], []
        for event in page:
            tags = [t.get("slug") for t in event.get("tags") or [] if t.get("slug")]
            nested = [m for m in event.get("markets") or [] if m.get("conditionId")
                      and not (m.get("closed") and _expired(m.get("endDate")))]
            if not nested:
                continue
            events.append(_event_row(str(event["id"]), event.get("slug"), event.get("title"),
                                     tags, len(nested)))
            for m in nested:
                markets.append(_poly_row(m, str(event["id"])))
                seen.add(m["conditionId"])
        with connection() as conn:
            with conn.cursor() as cur:
                total += _write_poly(cur, events, markets)

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


def import_poly_closed(stop=None, since=None):
    """Markets whose end date falls between `since` and now.

    Gamma has no "closed since" listing, so this walks end dates a day at a
    time, busiest first. Gamma stops paging at 1,000 results per query, so a
    day with more markets than that keeps its 1,000 busiest.
    """
    since = since or time.time() - 30 * 86400
    found = poly_list_markets(requests.Session(), int(since), 0, tag=None, forward_days=0)
    events, rows = {}, []
    for m in found:
        if stop is not None and stop.is_set():
            return 0
        if m.get("closed") and _expired(m.get("endDate")):
            continue
        event_id = m.get("event_id") or None
        if event_id:
            embedded = (m.get("events") or [{}])[0]
            entry = events.setdefault(event_id, [m.get("event_slug"), embedded.get("title"),
                                                 m.get("tags") or [], 0])
            entry[3] += 1
        rows.append(_poly_row(m, event_id))
    event_rows = [_event_row(eid, *e) for eid, e in events.items()]
    with connection() as conn:
        with conn.cursor() as cur:
            # All events before any market, so every market's parent exists.
            for i in range(0, len(event_rows), 1000):
                store.upsert(cur, "poly_events", POLY_EVENT_COLS, event_rows[i:i + 1000],
                             "event_id", ["title", "category"])
            for i in range(0, len(rows), 1000):
                store.upsert(cur, "poly_markets", POLY_COLS, rows[i:i + 1000],
                             "condition_id", POLY_WATCH)
    return len(rows)


SYNC = {"kalshi": sync_kalshi, "poly": sync_poly}
IMPORT_CLOSED = {"kalshi": import_kalshi_closed, "poly": import_poly_closed}

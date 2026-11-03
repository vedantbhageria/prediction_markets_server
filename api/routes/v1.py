"""Version 1 of the data API. Every endpoint needs a key (X-API-Key).

Lists take `limit` and `page_token` (the previous page's next_page_token) and
`format` = json, csv or parquet. Times go in as ISO 8601 or Unix seconds and
come out as ISO 8601 in UTC; a candle is stamped with the start of its bucket.

Both venues, one API. A Polymarket market is named by its condition id (0x...)
wherever a ticker goes, so candles, latest and backfill take either venue's
markets, mixed. Lists of markets take `venue` (kalshi by default, poly, or
all); Polymarket's raw trades are at /v1/markets/{condition_id}/trades.
"""

import json
import logging
import time
from typing import Literal

from fastapi import APIRouter, Body, Depends, Path, Query, Request

import poly
import ratelimit
import settings
from api import candles as candle_reader
from api import similar as similar_markets
from api import config, jobs
from api.common import (ApiError, clean, iso, json_response, make_token, not_found,
                        parse_time, read, read_token, require_full, require_key,
                        split_list, table)

router = APIRouter(prefix="/v1", dependencies=[Depends(require_key)])
log = logging.getLogger("api")

Format = Literal["json", "csv", "parquet"]
FORMAT = Query("json", description="json, csv or parquet")
LIMIT = Query(config.DEFAULT_ROWS, ge=1, le=config.MAX_ROWS)
TOKEN = Query(None, description="next_page_token from the previous page")
TICKER = Path(description="a Kalshi market ticker, e.g. KXBTCD-26NOV0117-T70000, or a"
                          " Polymarket condition id, 0x...")
VENUE = Query("kalshi", description="kalshi, poly or all")


def _offset(page_token):
    return int(read_token(page_token).get("o", 0))


def _paged(rows, limit, offset):
    """Rows were fetched with limit + 1: is there another page?"""
    more = len(rows) > limit
    return rows[:limit], (make_token(o=offset + limit) if more else None)


def _names(value):
    return [p.strip() for p in value.split(",") if p.strip()] if value else None


# ---------------------------------------------------------------------------
# Index
# ---------------------------------------------------------------------------

@router.get("", tags=["meta"], summary="What this version offers")
def index():
    return {"version": 1, "docs": "/docs", "endpoints": [
        "GET /v1/markets", "GET /v1/markets/{ticker}", "GET /v1/markets/{ticker}/candles",
        "GET /v1/markets/{ticker}/latest", "GET /v1/markets/{ticker}/similar",
        "GET /v1/markets/{condition_id}/trades",
        "GET /v1/candles", "GET /v1/latest",
        "GET /v1/events", "GET /v1/events/{event}", "GET /v1/series", "GET /v1/series/{series}",
        "GET /v1/categories", "GET /v1/hot", "GET /v1/status", "GET /v1/checks",
        "POST /v1/backfill", "GET /v1/backfill", "GET /v1/backfill/{job_id}"]}


# ---------------------------------------------------------------------------
# Markets, events, series, categories
# ---------------------------------------------------------------------------

MARKET_COLUMNS = ["ticker", "event_ticker", "series_ticker", "category", "title", "status",
                  "result", "open_time", "close_time", "strike_type", "strike", "floor_strike",
                  "cap_strike", "volume", "open_interest", "activity_score", "venue"]
MARKET_SELECT = """
    SELECT m.ticker, m.event_ticker, e.series_ticker, e.category, m.title, m.status, m.result,
           m.open_time, m.close_time, m.strike_type, m.strike, m.floor_strike, m.cap_strike,
           m.volume, m.open_interest, t.activity_score, 'kalshi' AS venue
    FROM kalshi_markets m
    JOIN kalshi_events e USING (event_ticker)
    LEFT JOIN kalshi_tracking t USING (ticker)"""
# Polymarket in the same columns: the condition id is the ticker, the event id
# the event, the question the title; status is open or closed; volume is
# Polymarket's (USDC) and activity_score its last 24 hours' volume.
POLY_SELECT = f"""
    SELECT m.condition_id AS ticker, m.event_id AS event_ticker, NULL::text AS series_ticker,
           e.category, m.question AS title,
           CASE WHEN m.closed THEN 'closed' ELSE 'open' END AS status, NULL::text AS result,
           {candle_reader.POLY_START} AS open_time, m.end_date AS close_time, m.strike_type,
           m.strike, NULL::numeric AS floor_strike, NULL::numeric AS cap_strike,
           m.volume, NULL::numeric AS open_interest, t.volume_24h AS activity_score,
           'poly' AS venue
    FROM poly_markets m
    LEFT JOIN poly_events e USING (event_id)
    LEFT JOIN poly_tracking t USING (condition_id)"""
SORTS = {"volume": "m.volume", "open_interest": "m.open_interest",
         "activity": "t.activity_score", "close_time": "m.close_time",
         "open_time": "m.open_time", "ticker": "m.ticker"}


def _status(where, params, status, table_alias="m"):
    if status == "open":
        where.append(f"{table_alias}.close_time > now()")
    elif status == "closed":
        where.append(f"{table_alias}.close_time <= now()")
    elif status:
        where.append(f"{table_alias}.status = ANY(%s)")
        params.append(_names(status))


def _poly_markets(status, event, q, tickers, tag, closes_after, closes_before, min_volume, sort):
    """WHERE and ORDER BY for Polymarket, from the same filters."""
    where, params = [], []
    if status == "open":
        where.append("m.closed IS NOT TRUE")
    elif status == "closed":
        where.append("m.closed IS TRUE")
    elif status:
        raise ApiError(400, "invalid_parameter", "status: open or closed for Polymarket")
    if event:
        where.append("m.event_id = ANY(%s)")
        params.append(_names(event))
    if q:
        where.append("(m.question ILIKE %s OR m.slug ILIKE %s OR m.condition_id ILIKE %s)")
        params += [f"%{q}%", f"%{q}%", f"{q}%"]
    if tickers:
        where.append("m.condition_id = ANY(%s)")
        params.append(split_list(tickers, "tickers", config.MAX_ROWS))
    if tag:
        where.append("e.tags && %s::text[]")
        params.append([t.lower() for t in _names(tag)])
    for name, value, op in (("closes_after", closes_after, ">="),
                            ("closes_before", closes_before, "<")):
        ts = parse_time(value, name)
        if ts is not None:
            where.append(f"m.end_date {op} to_timestamp(%s)")
            params.append(ts)
    if min_volume is not None:
        where.append("m.volume >= %s")
        params.append(min_volume)
    sorts = {"volume": "m.volume", "activity": "t.volume_24h", "close_time": "m.end_date",
             "open_time": "open_time", "ticker": "m.condition_id", "open_interest": "m.volume"}
    field = sorts.get(sort.lstrip("-"))
    if field is None:
        raise ApiError(400, "invalid_parameter", f"sort: one of {', '.join(SORTS)}")
    order = f"{field} {'DESC' if sort.startswith('-') else 'ASC'} NULLS LAST, ticker"
    return " AND ".join(where) or "true", params, order


@router.get("/markets", tags=["markets"], summary="Search and filter markets")
def markets(venue: Literal["kalshi", "poly", "all"] = VENUE,
            status: str | None = Query(None, description="open, closed, or Kalshi's own"
                                       " status (active, settled, ...)"),
            series: str | None = Query(None, description="series tickers, comma-separated"),
            event: str | None = Query(None, description="event tickers, comma-separated"),
            category: str | None = Query(None, description="e.g. Crypto, Sports"),
            q: str | None = Query(None, description="words in the title, or a ticker prefix"),
            tickers: str | None = Query(None, description="these markets, comma-separated"),
            closes_after: str | None = None, closes_before: str | None = None,
            min_volume: float | None = None,
            tag: str | None = Query(None, description="Polymarket tags (slugs), comma-separated:"
                                    " markets with any of them"),
            sort: str = Query("-volume", description="volume, open_interest, activity,"
                              " close_time, open_time or ticker; a leading - for descending"),
            limit: int = LIMIT, page_token: str | None = TOKEN, format: Format = FORMAT):
    offset = _offset(page_token)
    if venue != "kalshi":
        pw, pp, porder = _poly_markets(status if venue == "poly" or status in ("open", "closed")
                                       else None, event, q, tickers, tag, closes_after,
                                       closes_before, min_volume, sort)
        if venue == "poly":
            with read() as cur:
                cur.execute(f"{POLY_SELECT} WHERE {pw} ORDER BY {porder} LIMIT %s OFFSET %s",
                            pp + [limit + 1, offset])
                rows, nxt = _paged([clean(r) for r in cur.fetchall()], limit, offset)
            return table(rows, MARKET_COLUMNS, format, key="markets", next_token=nxt)
    where, params = [], []
    _status(where, params, status)
    if series:
        where.append("e.series_ticker = ANY(%s)")
        params.append(_names(series))
    if event:
        where.append("m.event_ticker = ANY(%s)")
        params.append(_names(event))
    if category:
        where.append("lower(e.category) = ANY(%s)")
        params.append([c.lower() for c in _names(category)])
    if q:
        where.append("(m.title ILIKE %s OR m.ticker ILIKE %s)")
        params += [f"%{q}%", f"{q}%"]
    if tickers:
        where.append("m.ticker = ANY(%s)")
        params.append(split_list(tickers, "tickers", config.MAX_ROWS))
    for name, value, op in (("closes_after", closes_after, ">="),
                            ("closes_before", closes_before, "<")):
        ts = parse_time(value, name)
        if ts is not None:
            where.append(f"m.close_time {op} to_timestamp(%s)")
            params.append(ts)
    if min_volume is not None:
        where.append("m.volume >= %s")
        params.append(min_volume)
    field = SORTS.get(sort.lstrip("-"))
    if field is None:
        raise ApiError(400, "invalid_parameter", f"sort: one of {', '.join(SORTS)}")
    order = f"{field} {'DESC' if sort.startswith('-') else 'ASC'} NULLS LAST, m.ticker"
    with read() as cur:
        if venue == "all":
            # Both lists, merged on the sort: each side sorted and cut to the
            # page's end, then the two put together and the page taken.
            both = f"""({MARKET_SELECT} WHERE {' AND '.join(where) or 'true'}
                         ORDER BY {order} LIMIT %s)
                       UNION ALL
                       ({POLY_SELECT} WHERE {pw} ORDER BY {porder} LIMIT %s)"""
            outer = sort.lstrip("-") if sort.lstrip("-") != "activity" else "activity_score"
            cur.execute(f"SELECT * FROM ({both}) x ORDER BY {outer}"
                        f" {'DESC' if sort.startswith('-') else 'ASC'} NULLS LAST, ticker"
                        f" LIMIT %s OFFSET %s",
                        params + [offset + limit + 1] + pp + [offset + limit + 1,
                                                             limit + 1, offset])
        else:
            cur.execute(f"{MARKET_SELECT} WHERE {' AND '.join(where) or 'true'}"
                        f" ORDER BY {order} LIMIT %s OFFSET %s", params + [limit + 1, offset])
        rows, nxt = _paged([clean(r) for r in cur.fetchall()], limit, offset)
    return table(rows, MARKET_COLUMNS, format, key="markets", next_token=nxt)


_hot_cache = {"at": 0.0, "set": frozenset(), "raw": None}


def _hot():
    """The hot list as activity.py last published it, cached for 30 s."""
    if time.time() - _hot_cache["at"] > 30:
        try:
            raw = ratelimit.client().get(ratelimit.KEY + "hot")
            data = json.loads(raw) if raw else None
        except Exception:
            data = None
        _hot_cache.update(at=time.time(), raw=data,
                          set=frozenset(data["tickers"]) if data else frozenset())
    return _hot_cache


PROPOSITION = """
    SELECT kind, subject, entity, comparator, low, high, unit, when_at AS "when", when_kind,
           template, topic, placeholder FROM propositions WHERE venue = %s AND market_id = %s"""


def _proposition(cur, venue, market_id):
    """The market as the similar-markets search reads it (propositions.py), or
    None before match.py has made its record."""
    cur.execute(PROPOSITION, (venue, market_id))
    row = cur.fetchone()
    return clean(row) if row else None


def _poly_market(cid, include_raw):
    with read() as cur:
        cur.execute(f"""
            SELECT m.*, {candle_reader.POLY_START} AS open_time, e.slug AS event_slug,
                   e.title AS event_title, e.category, e.tags,
                   t.volume_24h, t.moved_at, t.history_from, t.trades_from AS fetched_from,
                   poly_covered_to(t.trades_to) AS fetched_to
            FROM poly_markets m
            LEFT JOIN poly_events e USING (event_id)
            LEFT JOIN poly_tracking t USING (condition_id)
            WHERE m.condition_id = %s""", (cid,))
        row = cur.fetchone()
        proposition = _proposition(cur, "poly", cid) if row else None
    if not row:
        not_found("market", cid)
    r = clean(row)
    raw = row["raw"] or {}

    def parsed(key):
        try:
            return json.loads(raw.get(key) or "null")
        except (TypeError, ValueError):
            return None
    out = {"ticker": cid, "venue": "poly", "slug": r["slug"], "title": r["question"],
           "status": "closed" if r["closed"] else "open", "open_time": r["open_time"],
           "close_time": r["end_date"], "strike_type": r["strike_type"], "strike": r["strike"],
           "volume": r["volume"], "volume_24h": r["volume_24h"], "liquidity": r["liquidity"],
           "outcomes": parsed("outcomes"), "token_ids": parsed("clobTokenIds"),
           "outcome_prices": parsed("outcomePrices"), "yes_token_id": r["yes_token_id"],
           "rules": raw.get("description"), "resolution_source": raw.get("resolutionSource"),
           "event": {"event_ticker": r["event_id"], "slug": r["event_slug"],
                     "title": r["event_title"], "category": r["category"], "tags": r["tags"]},
           "activity": {"volume_24h": r["volume_24h"], "traded_by": r["moved_at"]},
           "candles": {"from": r["fetched_from"], "to": r["fetched_to"]},
           "proposition": proposition}
    if include_raw:
        out["raw"] = raw
    return json_response(out)


@router.get("/markets/{ticker}", tags=["markets"], summary="One market in full")
def market(ticker: str = TICKER,
           include_raw: bool = Query(False, description="add the venue's own payload")):
    if candle_reader.venue_of(ticker) == "poly":
        return _poly_market(ticker, include_raw)
    with read() as cur:
        cur.execute("""
            SELECT m.*, e.series_ticker, e.title AS event_title, e.sub_title, e.category,
                   e.mutually_exclusive, e.strike_period, s.title AS series_title, s.frequency,
                   t.activity_score, t.volume_measured_at,
                   t.candles_fetched_from, t.candles_fetched_to
            FROM kalshi_markets m
            JOIN kalshi_events e USING (event_ticker)
            LEFT JOIN kalshi_series s ON s.series_ticker = e.series_ticker
            LEFT JOIN kalshi_tracking t USING (ticker)
            WHERE m.ticker = %s""", (ticker,))
        row = cur.fetchone()
        proposition = _proposition(cur, "kalshi", ticker) if row else None
    if not row:
        not_found("market", ticker)
    r = clean(row)
    raw = row["raw"] or {}
    out = {k: r[k] for k in ("ticker", "title", "status", "result", "open_time", "close_time",
                             "strike_type", "strike", "floor_strike", "cap_strike",
                             "volume", "open_interest")}
    out.update({
        "yes_sub_title": raw.get("yes_sub_title"), "no_sub_title": raw.get("no_sub_title"),
        "rules_primary": raw.get("rules_primary"), "rules_secondary": raw.get("rules_secondary"),
        "event": {"event_ticker": r["event_ticker"], "title": r["event_title"],
                  "sub_title": r["sub_title"], "category": r["category"],
                  "mutually_exclusive": r["mutually_exclusive"],
                  "strike_period": r["strike_period"]},
        "series": {"series_ticker": r["series_ticker"], "title": r["series_title"],
                   "frequency": r["frequency"]},
        "activity": {"score": r["activity_score"], "hot": ticker in _hot()["set"],
                     "volume_measured_at": r["volume_measured_at"]},
        "candles": {"from": r["candles_fetched_from"], "to": r["candles_fetched_to"]},
        "proposition": proposition,
    })
    if include_raw:
        out["raw"] = raw
    return json_response(out)


EVENT_COLUMNS = ["event_ticker", "series_ticker", "category", "title", "sub_title",
                 "mutually_exclusive", "strike_period", "market_count", "open_market_count",
                 "close_time", "venue"]
EVENT_COUNTS = """
    LEFT JOIN LATERAL (
        SELECT count(*) AS market_count,
               count(*) FILTER (WHERE m.close_time > now()) AS open_market_count,
               max(m.close_time) AS close_time
        FROM kalshi_markets m WHERE m.event_ticker = e.event_ticker) c ON true"""
# Polymarket's events in the same columns: the event id is the ticker, tags
# instead of a series; a market is open until it resolves.
POLY_EVENT_SELECT = """
    SELECT e.event_id AS event_ticker, NULL::text AS series_ticker, e.category, e.title,
           e.slug AS sub_title, NULL::boolean AS mutually_exclusive, NULL::text AS strike_period,
           c.market_count, c.open_market_count, c.close_time, 'poly' AS venue
    FROM {source} e
    LEFT JOIN LATERAL (
        SELECT count(*) AS market_count,
               count(*) FILTER (WHERE m.closed IS NOT TRUE) AS open_market_count,
               max(m.end_date) AS close_time
        FROM poly_markets m WHERE m.event_id = e.event_id) c ON true"""


@router.get("/events", tags=["markets"], summary="Search and filter events")
def events(venue: Literal["kalshi", "poly"] = Query("kalshi", description="kalshi or poly"),
           series: str | None = Query(None, description="series tickers, comma-separated"),
           category: str | None = None,
           q: str | None = Query(None, description="words in the title"),
           status: Literal["open", "closed"] | None = Query(
               None, description="open: has a market still open; closed: has none"),
           limit: int = LIMIT, page_token: str | None = TOKEN, format: Format = FORMAT):
    if venue == "poly":
        return _poly_events(category, q, status, limit, page_token, format)
    where, params = [], []
    if series:
        where.append("e.series_ticker = ANY(%s)")
        params.append(_names(series))
    if category:
        where.append("lower(e.category) = ANY(%s)")
        params.append([c.lower() for c in _names(category)])
    if q:
        where.append("e.title ILIKE %s")
        params.append(f"%{q}%")
    if status:
        where.append(("" if status == "open" else "NOT ") + "EXISTS (SELECT 1 FROM kalshi_markets"
                     " m WHERE m.event_ticker = e.event_ticker AND m.close_time > now())")
    offset = _offset(page_token)
    with read() as cur:
        cur.execute(f"""
            SELECT e.event_ticker, e.series_ticker, e.category, e.title, e.sub_title,
                   e.mutually_exclusive, e.strike_period, c.market_count, c.open_market_count,
                   c.close_time, 'kalshi' AS venue
            FROM (SELECT * FROM kalshi_events e WHERE {' AND '.join(where) or 'true'}
                  ORDER BY e.event_ticker LIMIT %s OFFSET %s) e
            {EVENT_COUNTS}
            ORDER BY e.event_ticker""", params + [limit + 1, offset])
        rows, nxt = _paged([clean(r) for r in cur.fetchall()], limit, offset)
    return table(rows, EVENT_COLUMNS, format, key="events", next_token=nxt)


def _poly_events(category, q, status, limit, page_token, format):
    where, params = [], []
    if category:
        where.append("lower(e.category) = ANY(%s)")
        params.append([c.lower() for c in _names(category)])
    if q:
        where.append("(e.title ILIKE %s OR e.slug ILIKE %s)")
        params += [f"%{q}%", f"%{q}%"]
    if status:
        where.append(("" if status == "open" else "NOT ") + "EXISTS (SELECT 1 FROM poly_markets m"
                     " WHERE m.event_id = e.event_id AND m.closed IS NOT TRUE)")
    offset = _offset(page_token)
    source = (f"(SELECT * FROM poly_events e WHERE {' AND '.join(where) or 'true'}"
              " ORDER BY e.event_id LIMIT %s OFFSET %s)")
    with read() as cur:
        cur.execute(POLY_EVENT_SELECT.format(source=source) + " ORDER BY e.event_id",
                    params + [limit + 1, offset])
        rows, nxt = _paged([clean(r) for r in cur.fetchall()], limit, offset)
    return table(rows, EVENT_COLUMNS, format, key="events", next_token=nxt)


@router.get("/events/{event}", tags=["markets"], summary="An event and all its markets")
def event(event: str = Path(description="a Kalshi event ticker or a Polymarket event id")):
    with read() as cur:
        cur.execute(f"""
            SELECT e.event_ticker, e.series_ticker, e.category, e.title, e.sub_title,
                   e.mutually_exclusive, e.strike_period, c.market_count, c.open_market_count,
                   c.close_time, 'kalshi' AS venue
            FROM kalshi_events e {EVENT_COUNTS} WHERE e.event_ticker = %s""", (event,))
        row = cur.fetchone()
        select = MARKET_SELECT + " WHERE m.event_ticker = %s ORDER BY m.strike NULLS LAST, m.ticker"
        if not row:
            cur.execute(POLY_EVENT_SELECT.format(source="poly_events") + " WHERE e.event_id = %s",
                        (event,))
            row = cur.fetchone()
            select = POLY_SELECT + " WHERE m.event_id = %s ORDER BY m.strike NULLS LAST, ticker"
        if not row:
            not_found("event", event)
        cur.execute(select, (event,))
        markets = [clean(r) for r in cur.fetchall()]
    return json_response({**clean(row), "markets": markets})


SERIES_COLUMNS = ["series_ticker", "category", "title", "frequency"]


@router.get("/series", tags=["markets"], summary="Search and filter series")
def series_list(category: str | None = None,
                frequency: str | None = Query(None, description="e.g. daily, hourly, one_off"),
                q: str | None = Query(None, description="words in the title, or a ticker prefix"),
                limit: int = LIMIT, page_token: str | None = TOKEN, format: Format = FORMAT):
    where, params = [], []
    if category:
        where.append("lower(category) = ANY(%s)")
        params.append([c.lower() for c in _names(category)])
    if frequency:
        where.append("frequency = ANY(%s)")
        params.append(_names(frequency))
    if q:
        where.append("(title ILIKE %s OR series_ticker ILIKE %s)")
        params += [f"%{q}%", f"{q}%"]
    offset = _offset(page_token)
    with read() as cur:
        cur.execute(f"SELECT series_ticker, category, title, frequency FROM kalshi_series"
                    f" WHERE {' AND '.join(where) or 'true'} ORDER BY series_ticker"
                    f" LIMIT %s OFFSET %s", params + [limit + 1, offset])
        rows, nxt = _paged([clean(r) for r in cur.fetchall()], limit, offset)
    return table(rows, SERIES_COLUMNS, format, key="series", next_token=nxt)


@router.get("/series/{series}", tags=["markets"],
            summary="A series, with its counts and latest 100 events")
def series(series: str = Path(description="a series ticker, e.g. KXBTCD")):
    with read() as cur:
        cur.execute("SELECT series_ticker, category, title, frequency FROM kalshi_series"
                    " WHERE series_ticker = %s", (series,))
        row = cur.fetchone()
        if not row:
            not_found("series", series)
        cur.execute("""
            SELECT count(DISTINCT e.event_ticker) AS event_count, count(m.ticker) AS market_count,
                   count(m.ticker) FILTER (WHERE m.close_time > now()) AS open_market_count
            FROM kalshi_events e LEFT JOIN kalshi_markets m USING (event_ticker)
            WHERE e.series_ticker = %s""", (series,))
        counts = cur.fetchone()
        cur.execute(f"""
            SELECT e.event_ticker, e.title, e.sub_title, c.market_count, c.open_market_count,
                   c.close_time
            FROM kalshi_events e {EVENT_COUNTS}
            WHERE e.series_ticker = %s ORDER BY c.close_time DESC NULLS LAST LIMIT 100""",
                    (series,))
        latest = [clean(r) for r in cur.fetchall()]
    return json_response({**clean(row), **counts, "events": latest})


# Counting 2.9 million markets takes seconds, so the counts are kept ready:
# refreshed every CATEGORIES_EVERY seconds by keep_categories_fresh().
CATEGORIES_EVERY = 600
_categories = {"at": 0.0, "rows": None}


def refresh_categories():
    with read() as cur:
        # Markets counted per event first, then per category: a third of the
        # time of joining every market to its event.
        cur.execute("""
            SELECT e.category, count(DISTINCT e.series_ticker) AS series_count,
                   count(*) AS event_count, sum(c.n)::bigint AS market_count,
                   sum(c.open)::bigint AS open_market_count
            FROM kalshi_events e
            JOIN (SELECT event_ticker, count(*) AS n,
                         count(*) FILTER (WHERE close_time > now()) AS open
                  FROM kalshi_markets GROUP BY event_ticker) c USING (event_ticker)
            GROUP BY e.category ORDER BY market_count DESC""")
        _categories.update(at=time.time(), rows=[clean(r) for r in cur.fetchall()])


def keep_categories_fresh(stop):
    """Run in a thread for the life of the API."""
    while not stop.is_set():
        try:
            refresh_categories()
        except Exception as exc:
            log.warning("could not count categories: %s: %s", type(exc).__name__, exc)
        stop.wait(CATEGORIES_EVERY)


@router.get("/categories", tags=["markets"], summary="Every category, with counts")
def categories(format: Format = FORMAT):
    if _categories["rows"] is None:         # only before the first refresh finishes
        refresh_categories()
    return table(_categories["rows"], ["category", "series_count", "event_count",
                                       "market_count", "open_market_count"], format,
                 key="categories")


# ---------------------------------------------------------------------------
# Candles
# ---------------------------------------------------------------------------

Interval = Literal["1m", "5m", "15m", "1h", "4h", "1d"]
INTERVAL = Query("1m", description="1m, 5m, 15m, 1h, 4h or 1d")
FILL = Query("forward", description="forward: a row for every bucket, quiet ones repeating"
             " the last close (filled = true); raw: only buckets with a trade")
START = Query(None, description="ISO 8601 or Unix seconds; default: a day before end")
END = Query(None, description="ISO 8601 or Unix seconds, not included; default: now, or the"
            " market's close")
CANDLE_LIMIT = Query(config.MAX_ROWS, ge=1, le=config.MAX_ROWS)


def _candles(tickers, interval, start, end, fill, limit, page_token):
    a, b = parse_time(start, "start"), parse_time(end, "end")
    with read() as cur:
        return candle_reader.read(cur, tickers, candle_reader.INTERVALS[interval], a, b, fill,
                                  limit, read_token(page_token))


@router.get("/markets/{ticker}/candles", tags=["candles"], summary="Candles for one market")
def market_candles(ticker: str = TICKER, interval: Interval = INTERVAL,
                   start: str | None = START, end: str | None = END,
                   fill: Literal["forward", "raw"] = FILL, limit: int = CANDLE_LIMIT,
                   page_token: str | None = TOKEN, format: Format = FORMAT):
    rows, covered, nxt = _candles([ticker], interval, start, end, fill, limit, page_token)
    for r in rows:
        del r["ticker"]
    cov = covered[ticker]
    headers = {"X-Coverage-From": cov["from"] or "", "X-Coverage-To": cov["to"] or "",
               "X-Coverage-Complete": str(cov["complete"]).lower()}
    return table(rows, candle_reader.COLUMNS, format, key="candles", next_token=nxt,
                 meta={"ticker": ticker, "interval": interval, "fill": fill, "coverage": cov},
                 headers=headers if format != "json" else None)


@router.get("/candles", tags=["candles"],
            summary="Candles for up to 100 markets, as one long table")
def many_candles(tickers: str = Query(description="up to 100, comma-separated"),
                 interval: Interval = INTERVAL, start: str | None = START,
                 end: str | None = END, fill: Literal["forward", "raw"] = FILL,
                 limit: int = CANDLE_LIMIT, page_token: str | None = TOKEN,
                 format: Format = FORMAT):
    names = split_list(tickers, "tickers", 100)
    rows, covered, nxt = _candles(names, interval, start, end, fill, limit, page_token)
    return table(rows, ["ticker"] + candle_reader.COLUMNS, format, key="candles", next_token=nxt,
                 meta={"interval": interval, "fill": fill, "coverage": covered})


LATEST_COLUMNS = ["ticker", "status", "close_time", "volume", "open_interest", "last_trade_ts",
                  "last_price", "last_minute_volume", "candles_to"]


def _latest(tickers):
    with read() as cur:
        cur.execute("""
            SELECT q.n, m.ticker, m.status, m.close_time, m.volume, m.open_interest,
                   c.ts AS last_trade_ts, c.close AS last_price, c.volume AS last_minute_volume,
                   c.open, c.high, c.low, c.open_interest AS candle_open_interest,
                   t.candles_fetched_to AS candles_to
            FROM unnest(%s::text[]) WITH ORDINALITY AS q(ticker, n)
            JOIN kalshi_markets m USING (ticker)
            LEFT JOIN kalshi_tracking t USING (ticker)
            LEFT JOIN LATERAL (
                SELECT * FROM kalshi_candles k WHERE k.ticker = m.ticker
                ORDER BY k.ts DESC LIMIT 1) c ON true
            UNION ALL
            SELECT q.n, m.condition_id, CASE WHEN m.closed THEN 'closed' ELSE 'open' END,
                   m.end_date, m.volume, NULL::numeric,
                   c.ts, c.close, c.volume, c.open, c.high, c.low, NULL::numeric,
                   poly_covered_to(t.trades_to)
            FROM unnest(%s::text[]) WITH ORDINALITY AS q(ticker, n)
            JOIN poly_markets m ON m.condition_id = q.ticker
            LEFT JOIN poly_tracking t USING (condition_id)
            LEFT JOIN LATERAL (
                SELECT * FROM poly_candles k WHERE k.condition_id = m.condition_id
                ORDER BY k.ts DESC LIMIT 1) c ON true
            ORDER BY n""", (tickers, tickers))
        return [{k: v for k, v in clean(r).items() if k != "n"} for r in cur.fetchall()]


@router.get("/markets/{ticker}/latest", tags=["candles"],
            summary="The last trade's candle, with the market's volume and open interest")
def market_latest(ticker: str = TICKER):
    rows = _latest([ticker])
    if not rows:
        not_found("market", ticker)
    r = rows[0]
    last = None if r["last_trade_ts"] is None else {
        "ts": r["last_trade_ts"], "open": r["open"], "high": r["high"], "low": r["low"],
        "close": r["last_price"], "volume": r["last_minute_volume"],
        "open_interest": r["candle_open_interest"]}
    return json_response({"ticker": ticker, "status": r["status"], "close_time": r["close_time"],
                          "volume": r["volume"], "open_interest": r["open_interest"],
                          "last_trade": last, "candles_to": r["candles_to"]})


@router.get("/latest", tags=["candles"], summary="The last trade of up to 1,000 markets")
def latest(tickers: str = Query(description="up to 1,000, comma-separated"),
           format: Format = FORMAT):
    names = split_list(tickers, "tickers", 1000)
    rows = _latest(names)
    found = {r["ticker"] for r in rows}
    missing = [t for t in names if t not in found]
    if missing:
        raise ApiError(404, "not_found", "not in the store: " + ", ".join(missing[:20]))
    return table(rows, LATEST_COLUMNS, format, key="latest")


@router.get("/markets/{ticker}/similar", tags=["markets"],
            summary="The markets nearest to this one, on either venue")
def market_similar(ticker: str = TICKER,
                   limit: int = Query(50, ge=1, le=1000),
                   venue: Literal["kalshi", "poly", "all"] = Query(
                       "all", description="search Kalshi, Polymarket or both"),
                   status: Literal["open", "closed", "all"] = Query(
                       "open", description="markets still open, closed, or both"),
                   max_cosine: float | None = Query(
                       0.3, ge=0, le=2, description="largest text distance (cosine) kept;"
                       " 0 = same wording"),
                   max_days: float | None = Query(
                       None, ge=0, description="largest gap in days between the dates the"
                       " questions turn on"),
                   max_level: float | None = Query(
                       None, ge=0, description="largest relative gap between thresholds,"
                       " e.g. 0.1 = 10%"),
                   gates: str | None = Query(
                       None, description="subject, keywords (comma-separated): drop pairs"
                       " about a different subject, or whose key words or names differ"),
                   format: Format = FORMAT):
    """Nearest first, by one distance made of the text, timing, level and name
    gaps (each also given on its own). See /docs for the formula. `relation` reads
    how the two questions relate, from their fields; it filters nothing."""
    wanted = set(_names(gates) or [])
    unknown = wanted - {"subject", "keywords"}
    if unknown:
        raise ApiError(400, "invalid_parameter",
                       f"gates: subject and/or keywords, not {', '.join(sorted(unknown))}")
    with read() as cur:
        rows, me = similar_markets.similar(cur, ticker, limit, venue, status, max_cosine,
                                           max_days, max_level, wanted)
    asked = {k: me.get(k) for k in ("venue", "kind", "subject", "entity", "comparator", "low",
                                     "high", "unit", "when", "when_kind", "template")}
    return table(rows, similar_markets.COLUMNS, format, key="markets",
                 meta={"ticker": ticker, "market": clean(asked),
                       "limits": {"max_cosine": max_cosine, "max_days": max_days,
                                  "max_level": max_level, "gates": sorted(wanted),
                                  "venue": venue, "status": status}})


TRADE_COLUMNS = ["ts", "outcome", "side", "price", "yes_price", "size", "notional", "wallet",
                 "tx_hash"]


@router.get("/markets/{ticker}/trades", tags=["candles"],
            summary="Every stored trade of one Polymarket market, oldest first")
def market_trades(ticker: str = TICKER, start: str | None = START, end: str | None = END,
                  limit: int = CANDLE_LIMIT, page_token: str | None = TOKEN,
                  format: Format = FORMAT):
    """Each row is one taker's order as filled: `price` in the token traded,
    `yes_price` the same trade as a price of the market's first outcome,
    `size` in shares, `notional` the USDC paid. `wallet` is the taker's
    Polymarket proxy wallet; `tx_hash` the Polygon transaction."""
    if candle_reader.venue_of(ticker) != "poly":
        raise ApiError(400, "invalid_parameter", "trades are stored for Polymarket markets"
                       " (condition ids, 0x...) only; Kalshi gives candles")
    a, b = parse_time(start, "start"), parse_time(end, "end")
    state = read_token(page_token)
    with read() as cur:
        cur.execute("SELECT yes_token_id, outcomes FROM (SELECT yes_token_id,"
                    " raw->>'outcomes' AS outcomes, raw->>'clobTokenIds' AS tokens"
                    " FROM poly_markets WHERE condition_id = %s) m", (ticker,))
        m = cur.fetchone()
        if not m:
            not_found("market", ticker)
        cur.execute("""
            SELECT ts, token_id, side, price, size, wallet, tx_hash, trade_id FROM poly_trades
            WHERE condition_id = %s AND ts >= to_timestamp(%s) AND ts < to_timestamp(%s)
              AND (ts, trade_id) > (to_timestamp(%s), %s)
            ORDER BY ts, trade_id LIMIT %s""",
                    (ticker, a if a is not None else 0, b if b is not None else time.time() + 86400,
                     float(state.get("t", 0)), state.get("i", ""), limit + 1))
        got = cur.fetchall()
    rows = []
    for r in got[:limit]:
        yes = m["yes_token_id"] is None or r["token_id"] == m["yes_token_id"]
        price = float(r["price"])
        rows.append({"ts": r["ts"], "outcome": 0 if yes else 1, "side": r["side"], "price": price,
                     "yes_price": price if yes else round(1 - price, 6),
                     "size": float(r["size"]), "notional": round(float(r["size"]) * price, 6),
                     "wallet": r["wallet"], "tx_hash": r["tx_hash"]})
    nxt = None
    if len(got) > limit:
        last = got[limit - 1]
        nxt = make_token(t=last["ts"].timestamp(), i=last["trade_id"])
    return table(rows, TRADE_COLUMNS, format, key="trades", next_token=nxt,
                 meta={"ticker": ticker})


# ---------------------------------------------------------------------------
# Collection state
# ---------------------------------------------------------------------------

@router.get("/hot", tags=["collection"],
            summary="The markets live collection fetches every minute, busiest first")
def hot(limit: int = Query(config.MAX_ROWS, ge=1, le=100_000), format: Format = FORMAT):
    data = _hot()["raw"]
    if not data:
        return table([], ["ticker"], format, key="markets", meta={"at": None, "count": 0})
    with read() as cur:
        cur.execute("""
            SELECT t.ticker, m.title, e.category, t.activity_score, m.volume, m.close_time
            FROM kalshi_tracking t JOIN kalshi_markets m USING (ticker)
            JOIN kalshi_events e USING (event_ticker)
            WHERE t.ticker = ANY(%s) ORDER BY t.activity_score DESC, t.ticker LIMIT %s""",
                    (data["tickers"], limit))
        rows = [clean(r) for r in cur.fetchall()]
    return table(rows, ["ticker", "title", "category", "activity_score", "volume", "close_time"],
                 format, key="markets",
                 meta={"at": iso(data["at"]), "count": len(data["tickers"]), "cap": data["cap"]})


@router.get("/checks", tags=["collection"], summary="The daily data check's newest runs")
def checks(runs: int = Query(1, ge=1, le=60, description="how many daily runs, newest first"),
           format: Format = FORMAT):
    """One row per check per run (check.py): ok, warn or fail, a one-line
    value and the detail behind it."""
    with read() as cur:
        cur.execute("""
            SELECT run_at, name, status, value, detail FROM checks
            WHERE run_at IN (SELECT DISTINCT run_at FROM checks ORDER BY run_at DESC LIMIT %s)
            ORDER BY run_at DESC,
                     CASE status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, name""", (runs,))
        rows = [clean(r) for r in cur.fetchall()]
    if format != "json":                # one text column; the details differ per check
        for r in rows:
            r["detail"] = json.dumps(r["detail"])
    return table(rows, ["run_at", "name", "status", "value", "detail"], format, key="checks")


@router.get("/status", tags=["collection"], summary="Is collection keeping up?")
def status():
    now = time.time()
    out = {"time": iso(now)}
    r = ratelimit.client()
    try:
        live = json.loads(r.get(ratelimit.KEY + "live") or "null")
        down = r.get(f"{settings.REDIS_PREFIX}down:{settings.KALSHI_API_URL.split('/')[2]}")
        used = {s: ratelimit.used(s, 1) for s in ratelimit.SHARE}
        factor = ratelimit.factor()
    except Exception as exc:
        live = down = None
        used, factor = {}, None
        out["redis_error"] = type(exc).__name__
    out["kalshi"] = {"reachable": down is None,
                     "unreachable_since": iso(float(down)) if down else None}
    out["live"] = None if not live else {
        "last_cycle_at": iso(live["at"]), "fetched_up_to": iso(live["minute"]),
        "behind_seconds": round(now - live["minute"]), "cycle_seconds": live["seconds"],
        "hot": live["hot"], "warm": live["warm"]}
    hot_list = _hot()["raw"]
    out["hot"] = None if not hot_list else {
        "markets": len(hot_list["tickers"]), "cap": hot_list["cap"], "at": iso(hot_list["at"])}
    out["budget"] = {"requests_per_min": settings.KALSHI_REQUESTS_PER_MIN, "factor": factor,
                     "used_last_minute": used}
    with read() as cur:
        cur.execute("SELECT scan_id, started_at, finished_at, scored_at FROM kalshi_scans"
                    " ORDER BY scan_id DESC LIMIT 3")
        out["scans"] = [clean(x) for x in cur.fetchall()]
        cur.execute("""
            SELECT (SELECT reltuples::bigint FROM pg_class
                    WHERE oid = 'kalshi_markets'::regclass) AS markets,
                   (SELECT count(*) FROM kalshi_tracking
                    WHERE candles_fetched_to < volume_measured_at) AS behind,
                   (SELECT count(*) FROM api_jobs WHERE status = 'queued') AS jobs_queued,
                   (SELECT count(*) FROM api_jobs WHERE status = 'running') AS jobs_running""")
        counts = cur.fetchone()
    out["store"] = {"markets_about": counts["markets"],
                    "markets_behind": counts["behind"]}
    out["backfill_jobs"] = {"queued": counts["jobs_queued"], "running": counts["jobs_running"]}
    out["poly"] = _poly_status(now)
    return json_response(out)


def _poly_status(now):
    """Polymarket's side: the trade feed, catching up, candles waiting, budget."""
    try:
        down = ratelimit.client().get(f"{settings.REDIS_PREFIX}down:data-api.polymarket.com")
        # (a route noted while a VPN was set means nothing once it is not)
        route = (ratelimit.client().get(f"{settings.REDIS_PREFIX}route:data-api.polymarket.com")
                 if settings.VPN_PROXY else "direct")
        used = {s: ratelimit.used(s, 1, "poly") for s in ratelimit.SHARE}
        factor = ratelimit.factor("poly")
    except Exception:
        down = route = factor = None
        used = {}
    with read() as cur:
        cur.execute(f"""
            SELECT (SELECT covered_to FROM poly_feed) AS feed_to,
                   (SELECT updated_at FROM poly_feed) AS feed_at,
                   (SELECT count(*) FROM poly_tracking t WHERE {poly.HISTORY_DONE}
                       AND {poly.BEHIND}) AS behind,
                   (SELECT count(*) FROM poly_dirty) AS minutes_waiting""")
        c = cur.fetchone()
    return {"reachable": down is None, "route": route or "direct",
            "feed": None if c["feed_to"] is None else {
                "covered_to": iso(c["feed_to"]), "read_at": iso(c["feed_at"]),
                "behind_seconds": round(now - c["feed_to"].timestamp())},
            "markets_behind": c["behind"], "minutes_waiting_for_candles": c["minutes_waiting"],
            "budget": {"requests_per_min": settings.POLY_REQUESTS_PER_MIN, "factor": factor,
                       "used_last_minute": used}}


# ---------------------------------------------------------------------------
# The push command: backfill one market now
# ---------------------------------------------------------------------------

BACKFILL_BODY = Body(examples=[{"ticker": "KXBTCD-26NOV0117-T70000"},
                               {"ticker": "KXBTCD-26NOV0117-T70000", "from": "2026-10-31",
                                "to": "2026-11-01T12:00:00Z"}])


@router.post("/backfill", tags=["backfill"], status_code=202,
             summary="Re-download one market's candles now",
             dependencies=[Depends(require_full)])
def backfill(request: Request, body: dict = BACKFILL_BODY):
    """By default its whole life inside the store window; give `from` and `to`
    for less. Stored candles in that stretch are overwritten. Answers at once
    with a job to follow at GET /v1/backfill/{job_id}."""
    ticker = body.get("ticker")
    if not isinstance(ticker, str) or not ticker.strip():
        raise ApiError(400, "invalid_parameter", 'the body needs a "ticker"')
    extra = set(body) - {"ticker", "from", "to"}
    if extra:
        raise ApiError(400, "invalid_parameter", f"unknown fields: {', '.join(sorted(extra))}")
    start, end = parse_time(body.get("from"), "from"), parse_time(body.get("to"), "to")
    if start is not None and end is not None and start >= end:
        raise ApiError(400, "invalid_parameter", "from must be before to")
    request.state.tickers = [ticker.strip()]
    job = jobs.enqueue(request.state.key["key_id"], ticker.strip(), start, end)
    return json_response(jobs.describe(job), 202,
                         {"Location": f"/v1/backfill/{job['job_id']}"})


@router.get("/backfill", tags=["backfill"], summary="Recent backfill jobs")
def backfill_jobs(limit: int = Query(50, ge=1, le=1000)):
    with read() as cur:
        cur.execute("SELECT * FROM api_jobs ORDER BY job_id DESC LIMIT %s", (limit,))
        return json_response({"jobs": [jobs.describe(j) for j in cur.fetchall()]})


@router.get("/backfill/{job_id}", tags=["backfill"], summary="How a backfill job is going")
def backfill_job(job_id: int):
    return json_response(jobs.describe(jobs.get(job_id)))

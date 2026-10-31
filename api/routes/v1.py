"""Version 1 of the data API. Every endpoint needs a key (X-API-Key).

Lists take `limit` and `page_token` (the previous page's next_page_token) and
`format` = json, csv or parquet. Times go in as ISO 8601 or Unix seconds and
come out as ISO 8601 in UTC; a candle is stamped with the start of its bucket.
"""

import json
import time
from typing import Literal

from fastapi import APIRouter, Body, Depends, Path, Query, Request

import ratelimit
import settings
from api import candles as candle_reader
from api import config, jobs
from api.common import (ApiError, clean, iso, json_response, make_token, not_found,
                        parse_time, read, read_token, require_full, require_key,
                        split_list, table)

router = APIRouter(prefix="/v1", dependencies=[Depends(require_key)])

Format = Literal["json", "csv", "parquet"]
FORMAT = Query("json", description="json, csv or parquet")
LIMIT = Query(config.DEFAULT_ROWS, ge=1, le=config.MAX_ROWS)
TOKEN = Query(None, description="next_page_token from the previous page")
TICKER = Path(description="a Kalshi market ticker, e.g. KXBTCD-26NOV0117-T70000")


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
        "GET /v1/markets/{ticker}/latest", "GET /v1/candles", "GET /v1/latest",
        "GET /v1/events", "GET /v1/events/{event}", "GET /v1/series", "GET /v1/series/{series}",
        "GET /v1/categories", "GET /v1/hot", "GET /v1/status",
        "POST /v1/backfill", "GET /v1/backfill", "GET /v1/backfill/{job_id}"]}


# ---------------------------------------------------------------------------
# Markets, events, series, categories
# ---------------------------------------------------------------------------

MARKET_COLUMNS = ["ticker", "event_ticker", "series_ticker", "category", "title", "status",
                  "result", "open_time", "close_time", "strike_type", "strike", "floor_strike",
                  "cap_strike", "volume", "open_interest", "activity_score"]
MARKET_SELECT = """
    SELECT m.ticker, m.event_ticker, e.series_ticker, e.category, m.title, m.status, m.result,
           m.open_time, m.close_time, m.strike_type, m.strike, m.floor_strike, m.cap_strike,
           m.volume, m.open_interest, t.activity_score
    FROM kalshi_markets m
    JOIN kalshi_events e USING (event_ticker)
    LEFT JOIN kalshi_tracking t USING (ticker)"""
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


@router.get("/markets", tags=["markets"], summary="Search and filter markets")
def markets(status: str | None = Query(None, description="open, closed, or Kalshi's own"
                                       " status (active, settled, ...)"),
            series: str | None = Query(None, description="series tickers, comma-separated"),
            event: str | None = Query(None, description="event tickers, comma-separated"),
            category: str | None = Query(None, description="e.g. Crypto, Sports"),
            q: str | None = Query(None, description="words in the title, or a ticker prefix"),
            tickers: str | None = Query(None, description="these markets, comma-separated"),
            closes_after: str | None = None, closes_before: str | None = None,
            min_volume: float | None = None,
            sort: str = Query("-volume", description="volume, open_interest, activity,"
                              " close_time, open_time or ticker; a leading - for descending"),
            limit: int = LIMIT, page_token: str | None = TOKEN, format: Format = FORMAT):
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
    offset = _offset(page_token)
    with read() as cur:
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


@router.get("/markets/{ticker}", tags=["markets"], summary="One market in full")
def market(ticker: str = TICKER,
           include_raw: bool = Query(False, description="add Kalshi's own payload")):
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
    })
    if include_raw:
        out["raw"] = raw
    return json_response(out)


EVENT_COLUMNS = ["event_ticker", "series_ticker", "category", "title", "sub_title",
                 "mutually_exclusive", "strike_period", "market_count", "open_market_count",
                 "close_time"]
EVENT_COUNTS = """
    LEFT JOIN LATERAL (
        SELECT count(*) AS market_count,
               count(*) FILTER (WHERE m.close_time > now()) AS open_market_count,
               max(m.close_time) AS close_time
        FROM kalshi_markets m WHERE m.event_ticker = e.event_ticker) c ON true"""


@router.get("/events", tags=["markets"], summary="Search and filter events")
def events(series: str | None = Query(None, description="series tickers, comma-separated"),
           category: str | None = None,
           q: str | None = Query(None, description="words in the title"),
           status: Literal["open", "closed"] | None = Query(
               None, description="open: has a market still open; closed: has none"),
           limit: int = LIMIT, page_token: str | None = TOKEN, format: Format = FORMAT):
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
                   c.close_time
            FROM (SELECT * FROM kalshi_events e WHERE {' AND '.join(where) or 'true'}
                  ORDER BY e.event_ticker LIMIT %s OFFSET %s) e
            {EVENT_COUNTS}
            ORDER BY e.event_ticker""", params + [limit + 1, offset])
        rows, nxt = _paged([clean(r) for r in cur.fetchall()], limit, offset)
    return table(rows, EVENT_COLUMNS, format, key="events", next_token=nxt)


@router.get("/events/{event}", tags=["markets"], summary="An event and all its markets")
def event(event: str = Path(description="an event ticker")):
    with read() as cur:
        cur.execute(f"""
            SELECT e.event_ticker, e.series_ticker, e.category, e.title, e.sub_title,
                   e.mutually_exclusive, e.strike_period, c.market_count, c.open_market_count,
                   c.close_time
            FROM kalshi_events e {EVENT_COUNTS} WHERE e.event_ticker = %s""", (event,))
        row = cur.fetchone()
        if not row:
            not_found("event", event)
        cur.execute(f"{MARKET_SELECT} WHERE m.event_ticker = %s ORDER BY m.strike NULLS LAST,"
                    " m.ticker", (event,))
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


_categories = {"at": 0.0, "rows": None}


@router.get("/categories", tags=["markets"], summary="Every category, with counts")
def categories(format: Format = FORMAT):
    if _categories["rows"] is None or time.time() - _categories["at"] > 600:
        with read() as cur:
            cur.execute("""
                SELECT e.category, count(DISTINCT e.series_ticker) AS series_count,
                       count(DISTINCT e.event_ticker) AS event_count, count(*) AS market_count,
                       count(*) FILTER (WHERE m.close_time > now()) AS open_market_count
                FROM kalshi_markets m JOIN kalshi_events e USING (event_ticker)
                GROUP BY e.category ORDER BY market_count DESC""")
            _categories.update(at=time.time(), rows=[clean(r) for r in cur.fetchall()])
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
            SELECT m.ticker, m.status, m.close_time, m.volume, m.open_interest,
                   c.ts AS last_trade_ts, c.close AS last_price, c.volume AS last_minute_volume,
                   c.open, c.high, c.low, c.open_interest AS candle_open_interest,
                   t.candles_fetched_to AS candles_to
            FROM unnest(%s::text[]) WITH ORDINALITY AS q(ticker, n)
            JOIN kalshi_markets m USING (ticker)
            LEFT JOIN kalshi_tracking t USING (ticker)
            LEFT JOIN LATERAL (
                SELECT * FROM kalshi_candles k WHERE k.ticker = m.ticker
                ORDER BY k.ts DESC LIMIT 1) c ON true
            ORDER BY q.n""", (tickers,))
        return [clean(r) for r in cur.fetchall()]


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
    return json_response(out)


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

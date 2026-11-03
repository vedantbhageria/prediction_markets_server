"""Reading candles: any interval, forward-filled or raw, page by page.

Only minutes with a trade are stored. Every interval is made from those
one-minute candles when it is asked for -- not from the rollups, which can
trail a backfill by up to half an hour -- so an answer always matches what is
stored right now.

Forward fill (the default) gives a row for every bucket, repeating the last
close with volume 0 and filled = true where nothing traded -- but only where
that is known to be true:

  - inside the market's coverage [candles_fetched_from, candles_fetched_to),
    the stretch in which every trade is known to be stored
  - after its first trade in that coverage (before it, there is no price)
  - before the market closed

So a minute that has not been downloaded is never invented. A window that
starts part-way through a market's life carries the last close before it in.
`fill=raw` gives only the buckets that had a trade.

The last bucket may still be in progress, as with any exchange's candles.

Polymarket markets -- named by condition id, 0x... -- are read the same way
from poly_candles, which aggregate.py makes from every trade. Their coverage
is poly_tracking's [trades_from, poly_covered_to(trades_to)). They have no
open interest.
"""

import time
from datetime import datetime, timezone

from api import config
from api.common import ApiError, iso, make_token

INTERVALS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "4h": 14400, "1d": 86400}
COLUMNS = ["ts", "open", "high", "low", "close", "volume", "open_interest", "filled"]
DEFAULT_WINDOW = 86400          # with no start or end: the last day of the market's life


def floor(ts, step):
    return int(ts // step * step)


def ceil(ts, step):
    return int(-(-ts // step) * step)


def _epoch(dt):
    return dt.timestamp() if dt is not None else None


# Per venue: the candle table, its key, and what stands in for open interest.
TABLES = {"kalshi": ("kalshi_candles", "ticker", "open_interest"),
          "poly": ("poly_candles", "condition_id", "NULL::numeric")}


def venue_of(ticker):
    return "poly" if ticker.startswith("0x") else "kalshi"


# When a Polymarket market started, from the venue's own payload.
POLY_START = """CASE WHEN m.raw->>'startDate' ~ '^\\d{4}-'
                     THEN (m.raw->>'startDate')::timestamptz END"""
# ...and when it stopped trading: its closed time, else its end date.
POLY_CLOSE = """CASE WHEN m.closed THEN COALESCE(CASE WHEN m.raw->>'closedTime' ~ '^\\d{4}-'
                     THEN (m.raw->>'closedTime')::timestamptz END, m.end_date) END"""


def market_info(cur, tickers):
    """{ticker: what reading its candles needs}, for the tickers that exist."""
    tickers = list(tickers)
    kalshi = [t for t in tickers if venue_of(t) == "kalshi"]
    polys = [t for t in tickers if venue_of(t) == "poly"]
    out = {}
    if kalshi:
        cur.execute("""
            SELECT m.ticker, m.open_time, m.close_time,
                   t.candles_fetched_from, t.candles_fetched_to
            FROM kalshi_markets m LEFT JOIN kalshi_tracking t USING (ticker)
            WHERE m.ticker = ANY(%s)""", (kalshi,))
        out.update({r["ticker"]: {"ticker": r["ticker"], "venue": "kalshi",
                                  "open": _epoch(r["open_time"]),
                                  "close": _epoch(r["close_time"]),
                                  "from": _epoch(r["candles_fetched_from"]),
                                  "to": _epoch(r["candles_fetched_to"])}
                    for r in cur.fetchall()})
    if polys:
        cur.execute(f"""
            SELECT m.condition_id AS ticker, {POLY_START} AS open_time, {POLY_CLOSE} AS close_time,
                   t.trades_from AS fetched_from,
                   poly_covered_to(t.trades_to) AS fetched_to
            FROM poly_markets m LEFT JOIN poly_tracking t USING (condition_id)
            WHERE m.condition_id = ANY(%s)""", (polys,))
        out.update({r["ticker"]: {"ticker": r["ticker"], "venue": "poly",
                                  "open": _epoch(r["open_time"]),
                                  "close": _epoch(r["close_time"]),
                                  "from": _epoch(r["fetched_from"]),
                                  "to": _epoch(r["fetched_to"])}
                    for r in cur.fetchall()})
    return out


def window(info, start, end, now):
    """The asked-for [start, end), defaulting to the last day of the market's
    life: up to now, or up to its close if it has closed."""
    life_end = min(now, info["close"]) if info["close"] else now
    b = end if end is not None else life_end
    a = start if start is not None else b - DEFAULT_WINDOW
    if a >= b:
        raise ApiError(400, "invalid_parameter", "start must be before end")
    return a, b


def coverage(info, a, b, now):
    """What is held for this market, and whether it covers [a, b) -- clipped
    to the market's life and to what live collection can have reached."""
    if info["from"] is None:
        return {"from": None, "to": None, "complete": False}
    lo = max(a, info["open"] or a)
    hi = min(b, info["close"] or b, now - config.LIVE_LAG)
    complete = hi <= lo or (info["from"] <= lo and info["to"] >= hi)
    return {"from": iso(info["from"]), "to": iso(info["to"]), "complete": complete}


def _stored(cur, ticker, step, a, b, limit=None):
    """Stored candles with bucket start in [a, b), one row per bucket that
    had a trade, oldest first: [(t, open, high, low, close, volume, oi)]."""
    table, key, oi = TABLES[venue_of(ticker)]
    if step == 60:
        sql = f"""SELECT EXTRACT(EPOCH FROM ts)::bigint AS t, open AS o, high AS h, low AS l,
                         close AS c, volume AS v, {oi} AS oi
                  FROM {table}
                  WHERE {key} = %(t)s AND ts >= to_timestamp(%(a)s) AND ts < to_timestamp(%(b)s)
                  ORDER BY ts"""
    else:
        sql = f"""SELECT (floor(EXTRACT(EPOCH FROM ts) / %(s)s) * %(s)s)::bigint AS t,
                         first(open, ts) AS o, max(high) AS h, min(low) AS l, last(close, ts) AS c,
                         sum(volume) AS v, last({oi}, ts) AS oi
                  FROM {table}
                  WHERE {key} = %(t)s AND ts >= to_timestamp(%(a)s) AND ts < to_timestamp(%(b)s)
                  GROUP BY 1 ORDER BY 1"""
    if limit is not None:
        sql += " LIMIT %(n)s"
    cur.execute(sql, {"t": ticker, "s": step, "a": a, "b": b, "n": limit})
    return [(int(r["t"]), *[None if r[k] is None else float(r[k])
                            for k in ("o", "h", "l", "c", "v", "oi")])
            for r in cur.fetchall()]


def _before(cur, ticker, lo, a):
    """(close, open_interest) of the last stored candle in [lo, a), or None."""
    table, key, oi = TABLES[venue_of(ticker)]
    cur.execute(f"""SELECT close, {oi} AS open_interest FROM {table}
                    WHERE {key} = %s AND ts >= to_timestamp(%s) AND ts < to_timestamp(%s)
                    ORDER BY ts DESC LIMIT 1""", (ticker, lo, a))
    row = cur.fetchone()
    return (float(row["close"]), None if row["open_interest"] is None
            else float(row["open_interest"])) if row else None


def _row(t, o, h, low, c, v, oi, filled):
    return {"ts": datetime.fromtimestamp(t, timezone.utc), "open": o, "high": h, "low": low,
            "close": c, "volume": v, "open_interest": oi, "filled": filled}


def page(cur, info, step, a, b, fill, limit):
    """Up to `limit` candles with bucket start in [a, b), both on bucket
    boundaries. Returns (rows, where the next page starts, or None)."""
    ticker = info["ticker"]
    if fill == "raw":
        got = _stored(cur, ticker, step, a, b, limit + 1)
        nxt = got[limit][0] if len(got) > limit else None
        return [_row(*g, False) for g in got[:limit]], nxt

    # Forward: at most one row per bucket, so a page is `limit` buckets long.
    hi = min(b, a + limit * step)
    got = {g[0]: g for g in _stored(cur, ticker, step, a, hi)}
    rows, last = [], None
    if info["from"] is not None:
        fill_lo = floor(info["from"], step)
        fill_hi = min(info["to"], info["close"]) if info["close"] else info["to"]
        if fill_lo < a < fill_hi:
            last = _before(cur, ticker, info["from"], a)
    for t in range(a, hi, step):
        g = got.get(t)
        inside = info["from"] is not None and fill_lo <= t < fill_hi
        if g:
            rows.append(_row(*g, False))
            # Only a price from inside the coverage may be carried: before
            # it, trades in between could be missing.
            last = (g[4], g[6]) if inside else None
        elif last and inside:
            rows.append(_row(t, last[0], last[0], last[0], last[0], 0.0, last[1], True))
    return rows, (hi if hi < b else None)


def read(cur, tickers, step, start, end, fill, limit, state):
    """Candles for several markets in one long table, market by market.

    Returns (rows -- each with its ticker --, {ticker: coverage}, next token).
    """
    infos = market_info(cur, tickers)
    missing = [t for t in tickers if t not in infos]
    if missing:
        raise ApiError(404, "not_found", "not in the store: " + ", ".join(missing[:20]))
    now = time.time()
    first, resume = int(state.get("i", 0)), state.get("t")
    windows = {t: window(infos[t], start, end, now) for t in tickers}
    covered = {t: coverage(infos[t], *windows[t], now) for t in tickers}
    rows = []
    for i, ticker in enumerate(tickers):
        if i < first:
            continue
        info = infos[ticker]
        a, b = windows[ticker]
        # Nothing before the market opened or after it closed.
        a = floor(max(a, info["open"] or a), step)
        b = ceil(min(b, info["close"] or b, now), step)
        if i == first and resume is not None:
            a = max(a, int(resume))
        if a >= b:
            continue
        got, nxt = page(cur, info, step, a, b, fill, limit - len(rows))
        rows += [{"ticker": ticker, **r} for r in got]
        if nxt is not None:
            return rows, covered, make_token(i=i, t=nxt)
        if len(rows) >= limit and i + 1 < len(tickers):
            return rows, covered, make_token(i=i + 1)
    return rows, covered, None

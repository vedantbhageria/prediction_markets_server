"""Fill in a market's history from the venue when it is first collected.

Backfilled rows are the venues' own one-minute candles, the same width as the
live ones. Running this again after downtime fills the gap, because it resumes
from the newest candle already stored.
"""

import time
from datetime import datetime, timezone

import db
import settings
import store
import venues
from kalshi_markets import get_candles as kalshi_get_candles


def _floor(now):
    """How far back to fetch: the backfill window."""
    return now - settings.BACKFILL_DAYS * 86400


def backfill_kalshi(ticker):
    found = db.query(
        "SELECT e.series_ticker, m.open_time, m.close_time,"
        " (SELECT max(ts) FROM kalshi_candles c WHERE c.ticker = m.ticker) AS last"
        " FROM kalshi_markets m JOIN kalshi_events e USING (event_ticker)"
        " WHERE m.ticker = %s", (ticker,))
    if not found:
        return 0
    row, now = found[0], int(time.time())
    lo = max([_floor(now)] + [int(row[f].timestamp()) for f in ("open_time", "last")
                              if row[f]])
    # +120s: the last candle is stamped at the end of its minute, so it can
    # fall just after close_time.
    hi = min(int(row["close_time"].timestamp()) + 120, now) if row["close_time"] else now
    if hi - lo < 120:
        return 0
    got = kalshi_get_candles(venues.session(), row["series_ticker"], ticker, lo, hi, 1)
    return store.write_candles("kalshi", [
        (c["ticker"], c["ts"], c["open"], c["high"], c["low"], c["close"],
         c["volume"], c["open_interest"]) for c in got])


def backfill_poly(condition_id):
    found = db.query(
        "SELECT yes_token_id, end_date, raw->>'startDate' AS start_date,"
        " (SELECT max(ts) FROM poly_candles c WHERE c.condition_id = p.condition_id) AS last"
        " FROM poly_markets p WHERE condition_id = %s", (condition_id,))
    if not found or not found[0]["yes_token_id"]:
        return 0
    row, now = found[0], int(time.time())
    lo = _floor(now)
    if row["start_date"]:
        try:
            lo = max(lo, int(datetime.fromisoformat(
                row["start_date"].replace("Z", "+00:00")).timestamp()))
        except ValueError:
            pass
    if row["last"]:
        lo = max(lo, int(row["last"].timestamp()))
    if now - lo < 120:
        return 0
    data = venues.call("GET", venues.CLOB + "/prices-history",
                       params={"market": row["yes_token_id"], "startTs": lo,
                               "endTs": now, "fidelity": 1}, timeout=60, attempts=4)
    buckets = {}
    for point in data.get("history") or []:
        buckets.setdefault(point["t"] - point["t"] % 60, []).append(float(point["p"]))
    return store.write_candles("poly", [
        (condition_id, datetime.fromtimestamp(minute, timezone.utc),
         prices[0], max(prices), min(prices), prices[-1], None, None)
        for minute, prices in sorted(buckets.items())])


BACKFILL = {"kalshi": backfill_kalshi, "poly": backfill_poly}

"""Kalshi one-minute trade candles: plan requests, fetch, store, book-keep.

Shared by live.py (the newest edge of every market), backfill.py (history and
repairs) and the viewer's per-market download.

A *span* is (ticker, lo, hi): fetch every minute whose start is in [lo, hi),
unix seconds on minute boundaries. Kalshi's batch endpoint takes up to 100
markets and 10,000 market-minutes per request, all over one shared window, so
spans with similar windows are packed together. After a request succeeds the
candles and the window they cover are written in one transaction
(store.write_kalshi_fetch); a request that fails changes nothing and is simply
planned again next time.

Only minutes with a trade are kept. A minute Kalshi reports with quotes but no
trade is dropped.
"""

import logging
import time
from datetime import datetime, timezone

import store
import venues

log = logging.getLogger("fetch")

MAX_MARKETS = venues.KALSHI_BATCH
MAX_MINUTES = venues.KALSHI_MAX_CANDLES
# Tickers Kalshi rejects outright are left alone for this long.
BAD_TICKER_SECONDS = 3600
_bad = {}


def minute(ts):
    return int(ts) // 60 * 60


def stamp(ts):
    return datetime.fromtimestamp(ts, timezone.utc)


def split(span, newest_first=False):
    """A span cut into pieces one single-market request can carry."""
    ticker, lo, hi = span
    step = MAX_MINUTES * 60
    if newest_first:
        out, end = [], hi
        while end > lo:
            out.append((ticker, max(lo, end - step), end))
            end -= step
        return out
    return [(ticker, start, min(hi, start + step)) for start in range(lo, hi, step)]


def plan(spans, newest_first=False):
    """Requests for these spans: [(tickers, lo, hi)], each inside both limits.

    Spans are sorted by start, and each request takes as many consecutive
    spans as fit: its window runs from the first start to the latest end, and
    markets x minutes in that window may not pass 10,000.
    """
    pieces = []
    for span in spans:
        if span[2] > span[1] and _bad.get(span[0], 0) < time.time():
            pieces += split(span, newest_first)
    pieces.sort(key=lambda p: (p[1], p[2]))
    out, group, lo, hi = [], [], None, None
    for ticker, a, b in pieces:
        if group:
            new_hi = max(hi, b)
            if (len(group) < MAX_MARKETS and ticker not in group
                    and (len(group) + 1) * ((new_hi - lo) // 60) <= MAX_MINUTES):
                group.append(ticker)
                hi = new_hi
                continue
            out.append((group, lo, hi))
        group, lo, hi = [ticker], a, b
    if group:
        out.append((group, lo, hi))
    if newest_first:
        out.sort(key=lambda r: -r[2])
    return out


def _rows(ticker, candles, lo, hi):
    """Stored rows from Kalshi's payloads: traded minutes inside [lo, hi)."""
    rows = []
    for c in candles:
        price = c.get("price") or {}
        try:
            volume = float(c.get("volume_fp") or 0)
        except ValueError:
            continue
        if price.get("close_dollars") is None or volume <= 0:
            continue                    # quotes only, no trade
        ts = int(c["end_period_ts"]) - 60
        if not lo <= ts < hi:
            continue
        rows.append((ticker, stamp(ts), price.get("open_dollars"), price.get("high_dollars"),
                     price.get("low_dollars"), price["close_dollars"], c["volume_fp"],
                     c.get("open_interest_fp")))
    return rows


def run(request, stop=None):
    """Fetch and store one planned request. Returns candles written, or None
    if the request failed (nothing is recorded then)."""
    tickers, lo, hi = request
    try:
        # Kalshi's window is on end_period_ts, both ends inclusive: minute
        # starts in [lo, hi) end in [lo + 60, hi].
        got = venues.kalshi_candles(tickers, lo + 60, hi, stop=stop)
    except venues.VenueError as exc:
        if exc.status is not None and 400 <= exc.status < 500 and exc.status != 429:
            if len(tickers) == 1:
                _bad[tickers[0]] = time.time() + BAD_TICKER_SECONDS
                log.warning("kalshi rejects %s; skipping it for an hour (%s)", tickers[0], exc)
                return None
            # One bad ticker fails the whole batch: halve until it is found.
            half = len(tickers) // 2
            parts = [run((tickers[:half], lo, hi), stop), run((tickers[half:], lo, hi), stop)]
            return None if None in parts else sum(parts)
        log.warning("candle request failed (%d markets): %s", len(tickers), exc)
        return None
    except Exception as exc:
        log.warning("candle request failed (%d markets): %s: %s",
                    len(tickers), type(exc).__name__, exc)
        return None

    rows, fetched = [], []
    for ticker in tickers:
        if ticker not in got:
            continue                    # no answer for it: claim nothing
        rows += _rows(ticker, got[ticker], lo, hi)
        fetched.append((ticker, stamp(lo), stamp(hi)))
    try:
        store.write_kalshi_fetch(rows, fetched)
    except Exception as exc:
        log.warning("writing %d candles failed: %s: %s", len(rows), type(exc).__name__, exc)
        return None
    return len(rows)

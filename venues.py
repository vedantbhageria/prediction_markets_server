"""Read-only calls to Kalshi and Polymarket used by the live collector.

Everything here is a public endpoint; no API key is involved.
"""

import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import requests

KALSHI = "https://external-api.kalshi.com/trade-api/v2"
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

KALSHI_BATCH = 100      # tickers per /markets request
POLY_BATCH = 400        # tokens per /midpoints request; the API rejects ~1000
GAMMA_BATCH = 50        # condition ids per /markets request


class VenueError(RuntimeError):
    pass


# One requests.Session per thread; Sessions are not safe to share.
_local = threading.local()
_pool = ThreadPoolExecutor(max_workers=6, thread_name_prefix="venue")


def session():
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


def call(method, url, params=None, body=None, timeout=10, attempts=1):
    """One request. The live loop allows a single quick retry and no more: a
    late answer is worth less than the next poll."""
    status = None
    for attempt in range(attempts):
        try:
            response = session().request(method, url, params=params, json=body,
                                         timeout=timeout)
            status = response.status_code
            if status < 400:
                return response.json()
            if status != 429 and status < 500:
                raise VenueError(f"{url}: HTTP {status} {response.text[:120]}")
        except requests.RequestException:
            if attempt == attempts - 1:
                raise
        if attempt < attempts - 1:
            time.sleep(min(2 ** attempt, 30))
    raise VenueError(f"{url}: HTTP {status}, gave up")


def _chunks(items, size):
    return [items[i:i + size] for i in range(0, len(items), size)]


def _f(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

def kalshi_markets(tickers, timeout=10, attempts=2):
    """Current market payloads for these tickers, fetched in parallel batches."""
    def one(batch):
        data = call("GET", KALSHI + "/markets",
                    params={"tickers": ",".join(batch), "limit": 1000},
                    timeout=timeout, attempts=attempts)
        return data.get("markets") or []
    out = []
    for page in _pool.map(one, _chunks(list(tickers), KALSHI_BATCH)):
        out += page
    return out


def kalshi_sample(market):
    """(quote, trade, running volume, open interest) from a market payload.

    The quote is the mid, but only when both sides are really there: an empty
    side shows as a 0.00 bid or a 1.00 ask, and a mid taken across that would
    put a dead market at 50%.
    """
    bid, ask = _f(market.get("yes_bid_dollars")), _f(market.get("yes_ask_dollars"))
    last = _f(market.get("last_price_dollars"))
    quote = (bid + ask) / 2 if (bid and ask and bid > 0 and ask < 1) else None
    return (quote, last if last else None,
            _f(market.get("volume_fp")), _f(market.get("open_interest_fp")))


def kalshi_events(status, min_close_ts=None, cursor=None):
    """Pages of events with their markets nested, as (events, next_cursor).

    Combination (KXMVE) markets are not listed under events, so this is the
    real catalogue. Pass a cursor from an earlier page to resume there; the
    cursor after the last page is None.
    """
    params = {"status": status, "limit": 200, "with_nested_markets": "true"}
    if min_close_ts:
        params["min_close_ts"] = int(min_close_ts)
    if cursor:
        params["cursor"] = cursor
    seen = set()
    while True:
        data = call("GET", KALSHI + "/events", params=params, timeout=60, attempts=8)
        page = data.get("events") or []
        cursor = data.get("cursor") or None
        if not page:
            return
        if cursor in seen:
            cursor = None
        yield page, cursor
        if not cursor:
            return
        seen.add(cursor)
        params["cursor"] = cursor


def kalshi_series():
    return call("GET", KALSHI + "/series", timeout=120, attempts=6).get("series") or []


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

def poly_midpoints(tokens):
    """{token id: mid}. Tokens with an empty book are simply absent."""
    def one(batch):
        return call("POST", CLOB + "/midpoints",
                    body=[{"token_id": token} for token in batch], attempts=2)
    out = {}
    for page in _pool.map(one, _chunks(list(tokens), POLY_BATCH)):
        for token, mid in (page or {}).items():
            value = _f(mid)
            if value is not None:
                out[token] = value
    return out


def poly_events(closed=False, end_date_min=None, cursor=None):
    """Pages of events with their markets nested, as (events, next_cursor).

    Uses the keyset listing, which pages through everything; the offset
    listing stops after a few thousand events. `end_date_min` is unix time.
    """
    params = {"limit": 100, "closed": "true" if closed else "false"}
    if end_date_min:
        params["end_date_min"] = time.strftime("%Y-%m-%dT%H:%M:%SZ",
                                               time.gmtime(end_date_min))
    if cursor:
        params["after_cursor"] = cursor
    seen = set()
    while True:
        data = call("GET", GAMMA + "/events/keyset", params=params, timeout=60, attempts=6)
        page = data.get("events") or []
        cursor = data.get("next_cursor") or None
        if not page:
            return
        if cursor in seen:
            cursor = None
        yield page, cursor
        if not cursor:
            return
        seen.add(cursor)
        params["after_cursor"] = cursor


def poly_markets(condition_ids):
    """Current payloads by condition id, for markets that left the open list."""
    out = []
    for batch in _chunks(list(condition_ids), GAMMA_BATCH):
        out += call("GET", GAMMA + "/markets",
                    params={"condition_ids": batch, "limit": len(batch)},
                    timeout=60, attempts=6) or []
    return out


def yes_token(market):
    """The Yes-side token id. Its price is the probability."""
    try:
        return json.loads(market["clobTokenIds"])[0]
    except (KeyError, TypeError, ValueError, IndexError):
        return None

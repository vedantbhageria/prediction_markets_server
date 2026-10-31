"""Read-only calls to Kalshi and Polymarket.

Everything here is a public endpoint; no API key is involved. Every Kalshi
request first takes a token from the shared budget in ratelimit.py.
"""

import json
import logging
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import requests

import ratelimit
import settings

log = logging.getLogger("venues")

KALSHI = settings.KALSHI_API_URL
GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"

KALSHI_BATCH = 100      # tickers per /markets or /markets/candlesticks request
KALSHI_MAX_CANDLES = 10_000     # market-minutes one candlesticks request may ask for
POLY_BATCH = 400        # tokens per /midpoints request; the API rejects ~1000
GAMMA_BATCH = 50        # condition ids per /markets request


class VenueError(RuntimeError):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


# One requests.Session per thread; Sessions are not safe to share.
_local = threading.local()
_pool = ThreadPoolExecutor(max_workers=6, thread_name_prefix="venue")


def session():
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


class Link:
    """Whether a venue can be reached at all, shared by every thread.

    The first request that cannot connect (no DNS, refused, reset, no route)
    marks the link down. From then on every request to that host waits here
    instead of timing out and retrying on its own, and one background check
    looks the host up and opens a connection to it every NET_PROBE_SECONDS --
    no API request, so no budget spent. As soon as that works, everything
    waiting carries on with the request it was making.
    """

    def __init__(self, host):
        self.host = host
        self.up = threading.Event()
        self.up.set()
        self.lock = threading.Lock()
        self.since = None

    def down(self, exc):
        with self.lock:
            if not self.up.is_set():
                return
            self.up.clear()
            self.since = time.time()
        log.warning("%s unreachable (%s); checking again every %ds", self.host,
                    type(exc).__name__, settings.NET_PROBE_SECONDS)
        threading.Thread(target=self._probe, daemon=True, name=f"probe-{self.host}").start()

    def _probe(self):
        while True:
            time.sleep(settings.NET_PROBE_SECONDS)
            try:
                port = PORTS.get(self.host, 443)
                socket.getaddrinfo(self.host, port)
                socket.create_connection((self.host, port), timeout=10).close()
            except OSError:
                continue
            log.warning("%s reachable again after %.0fs", self.host, time.time() - self.since)
            self.up.set()
            return

    def wait(self, stop=None):
        """Block while the link is down. False only if `stop` was set."""
        while not self.up.wait(1):
            if stop is not None and stop.is_set():
                return False
        return True


_links = {}
_links_lock = threading.Lock()
PORTS = {}


def link(url):
    parts = urlparse(url)
    with _links_lock:
        if parts.hostname not in _links:
            if parts.port:
                PORTS[parts.hostname] = parts.port
            _links[parts.hostname] = Link(parts.hostname)
        return _links[parts.hostname]


# A request that never reached the venue: the network, not the venue, failed.
UNREACHABLE = (requests.ConnectionError,)
# Requests that reached the link but kept failing once it was back up give up
# after this many tries, so one bad request cannot wait forever.
MAX_UNREACHABLE = 5


def call(method, url, params=None, body=None, timeout=10, attempts=1, stop=None):
    """One request, retried up to `attempts` times on errors worth retrying.

    While the venue cannot be reached at all the request waits for the link
    to come back and then tries again from where it was; that does not use
    up `attempts`. Kalshi requests wait for the shared budget first, and a 429
    from Kalshi halves that budget for every program before the retry.
    """
    status = None
    kalshi = url.startswith(KALSHI)
    net = link(url)
    attempt = unreachable = 0
    while True:
        if not net.wait(stop):
            raise VenueError(f"{url}: stopped", status)
        if kalshi and not ratelimit.acquire(stop):
            raise VenueError(f"{url}: stopped", status)
        try:
            response = session().request(method, url, params=params, json=body,
                                         timeout=timeout)
            status = response.status_code
            if status < 400:
                return response.json()
            if status == 429 and kalshi:
                ratelimit.throttled()
            elif status != 429 and status < 500:
                raise VenueError(f"{url}: HTTP {status} {response.text[:120]}", status)
        except UNREACHABLE as exc:
            unreachable += 1
            if unreachable >= MAX_UNREACHABLE:
                raise
            net.down(exc)
            continue
        except requests.RequestException:
            if attempt == attempts - 1:
                raise
        attempt += 1
        if attempt >= attempts:
            raise VenueError(f"{url}: HTTP {status}, gave up", status)
        time.sleep(min(2 ** (attempt - 1), 30))


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


def kalshi_candles(tickers, start, end, timeout=30, attempts=3, stop=None):
    """{ticker: [candlestick payloads]} for one-minute candles ending in
    [start, end] (unix seconds, both inclusive, as Kalshi reads them).

    A ticker Kalshi did not answer for is missing from the result, which is
    different from one that is present with no candles.
    """
    tickers = list(tickers)
    if len(tickers) > KALSHI_BATCH:
        raise ValueError(f"{len(tickers)} tickers in one candle request")
    if len(tickers) * ((end - start) // 60 + 1) > KALSHI_MAX_CANDLES:
        raise ValueError(f"{len(tickers)} tickers x {(end - start) // 60 + 1} minutes"
                         " is more than one candle request may ask for")
    data = call("GET", KALSHI + "/markets/candlesticks",
                params={"market_tickers": ",".join(tickers), "start_ts": int(start),
                        "end_ts": int(end), "period_interval": 1},
                timeout=timeout, attempts=attempts, stop=stop)
    return {m.get("market_ticker"): m.get("candlesticks") or []
            for m in data.get("markets") or []}


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

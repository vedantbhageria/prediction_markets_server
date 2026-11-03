"""Read-only calls to Kalshi and Polymarket.

Everything here is a public endpoint; no API key is involved. Every request
first takes a token from its venue's shared budget in ratelimit.py.
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
GAMMA = settings.POLY_GAMMA_URL
DATA = settings.POLY_DATA_URL
POLY_HOSTS = (GAMMA, DATA)

KALSHI_BATCH = 100      # tickers per /markets or /markets/candlesticks request
KALSHI_MAX_CANDLES = 10_000     # market-minutes one candlesticks request may ask for
# Trade lists (data API v2) come a page of at most this many at a time,
# followed by cursor however far back they go.
POLY_PAGE = 1000
TRADE_FIELDS = ("condition_id", "token_id", "proxy_wallet", "side", "size", "price",
                "timestamp", "transaction_hash")
GAMMA_BATCH = 50        # condition ids per /markets request


class VenueError(RuntimeError):
    def __init__(self, message, status=None):
        super().__init__(message)
        self.status = status


# One requests.Session per thread; Sessions are not safe to share.
_local = threading.local()
_pool = ThreadPoolExecutor(max_workers=6, thread_name_prefix="venue")


def session(via="direct"):
    """This thread's Session for one way out: "direct", or "vpn" through
    settings.VPN_PROXY."""
    sessions = _local.__dict__.setdefault("sessions", {})
    if via not in sessions:
        s = requests.Session()
        if via == "vpn":
            s.proxies = {"http": settings.VPN_PROXY, "https": settings.VPN_PROXY}
            s.trust_env = False
        sessions[via] = s
    return sessions[via]


class Route:
    """Which way requests to one host go out: through the VPN proxy if it is
    configured for the host, otherwise direct.

    A request that cannot connect one way is tried the other way before the
    host counts as unreachable. After falling back to direct, the proxy is
    tried again every VPN_RETRY_SECONDS, so a tunnel that comes back is used
    again without a restart.
    """

    def __init__(self, host):
        self.host = host
        self.vpn = bool(settings.VPN_PROXY) and host in settings.VPN_HOSTS
        self.via = "vpn" if self.vpn else "direct"
        self.since = time.time()
        self.lock = threading.Lock()

    def ways(self):
        return ["vpn", "direct"] if self.vpn else ["direct"]

    def current(self):
        with self.lock:
            if (self.vpn and self.via == "direct"
                    and time.time() - self.since > settings.VPN_RETRY_SECONDS):
                self._set("vpn", "time to try it again")
            return self.via

    def failed(self, via, tried):
        """`via` could not connect. The other way, if one is left untried, or None."""
        with self.lock:
            left = [w for w in self.ways() if w not in tried]
            if not left:
                return None
            if self.via == via:
                self._set(left[0], f"{via} could not connect")
            return self.via if self.via in left else left[0]

    def worked(self, via):
        with self.lock:
            if self.via != via:
                self._set(via, "it answered")

    def _set(self, via, why):
        if via != self.via:
            log.warning("%s: going %s (%s)", self.host, "through the VPN" if via == "vpn"
                        else "direct", why)
            self.via = via
            self.since = time.time()
            try:
                ratelimit.client().set(f"{settings.REDIS_PREFIX}route:{self.host}", via)
            except Exception:
                pass


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
        self.route = Route(host)

    def _publish(self):
        """Tell the API's /v1/status that this host is out, for as long as
        the checks keep failing. Best effort: Redis may be out too."""
        try:
            ratelimit.client().set(f"{settings.REDIS_PREFIX}down:{self.host}", self.since,
                                   ex=int(settings.NET_PROBE_SECONDS * 3) + 10)
        except Exception:
            pass

    def down(self, exc):
        with self.lock:
            if not self.up.is_set():
                return
            self.up.clear()
            self.since = time.time()
        self._publish()
        log.warning("%s unreachable (%s); checking again every %ds", self.host,
                    type(exc).__name__, settings.NET_PROBE_SECONDS)
        threading.Thread(target=self._probe, daemon=True, name=f"probe-{self.host}").start()

    def _reachable(self, via):
        """Can a connection be opened this way? No API request, so no budget
        spent: direct opens a socket, the VPN fetches the host's front page
        through the proxy and takes any answer at all."""
        port = PORTS.get(self.host, 443)
        try:
            if via == "vpn":
                session("vpn").head(f"https://{self.host}:{port}/", timeout=10)
            else:
                socket.getaddrinfo(self.host, port)
                socket.create_connection((self.host, port), timeout=10).close()
            return True
        except (OSError, requests.RequestException):
            return False

    def _probe(self):
        while True:
            time.sleep(settings.NET_PROBE_SECONDS)
            way = next((w for w in self.route.ways() if self._reachable(w)), None)
            if way is None:
                self._publish()
                continue
            self.route.worked(way)
            try:
                ratelimit.client().delete(f"{settings.REDIS_PREFIX}down:{self.host}")
            except Exception:
                pass
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
    venue = "kalshi" if url.startswith(KALSHI) else "poly" if url.startswith(POLY_HOSTS) else None
    net = link(url)
    attempt = unreachable = 0
    via, tried = None, set()
    while True:
        if not net.wait(stop):
            raise VenueError(f"{url}: stopped", status)
        if venue and not ratelimit.acquire(stop, venue):
            raise VenueError(f"{url}: stopped", status)
        via = via or net.route.current()
        if venue == "poly" and method == "GET":
            # Polymarket's answers sit in Cloudflare's cache for up to five
            # minutes (max-age=300), each address on its own clock: a value
            # that changes every request asks Polymarket itself instead.
            params = dict(params or {}, _=f"{time.time():.3f}")
        try:
            response = session(via).request(method, url, params=params, json=body,
                                            timeout=timeout)
            status = response.status_code
            _notice(url, response.headers)
            if status < 400:
                return response.json()
            if status == 429 and venue:
                ratelimit.throttled(venue)
            elif status != 429 and status < 500:
                raise VenueError(f"{url}: HTTP {status} {response.text[:120]}", status)
        except UNREACHABLE as exc:
            # The other way out first, if there is one not tried yet.
            tried.add(via)
            via = net.route.failed(via, tried)
            if via is not None:
                continue
            tried.clear()
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


_noticed = set()


def _notice(url, headers):
    """A venue's warning that an endpoint is going away, logged once per
    endpoint (check.py looks for these)."""
    found = {k: headers[k] for k in ("Deprecation", "Sunset", "Warning") if headers.get(k)}
    path = url.split("?")[0]
    if found and path not in _noticed:
        _noticed.add(path)
        log.warning("%s: deprecation notice from the venue: %s", path, found)


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


def poly_trades(market=None, cursor=None, stop=None):
    """One page of trades, newest first, as (trades, next cursor or None):
    every trade on Polymarket, or one market's (a condition id). Pass the
    cursor from the page before to go on from it. Each is a taker's order as
    filled, so a trade is counted once, with the taker's wallet. Only the
    fields poly.row needs are kept."""
    params = {"taker_only": "true"}
    params.update({"cursor": cursor} if cursor else {"limit": POLY_PAGE})
    if market:
        params["condition"] = market        # sent with every page, as the venue asks
    data = call("GET", DATA + "/v2/trades", params=params, timeout=60, attempts=4,
                stop=stop) or {}
    trades = [{k: t.get(k) for k in TRADE_FIELDS} for t in data.get("data") or []]
    return trades, (data.get("pagination") or {}).get("next_cursor") or None

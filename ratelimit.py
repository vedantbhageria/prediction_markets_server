"""One request budget per venue, shared by every program through Redis.

discover.py, live.py and backfill.py each take a token before every Kalshi
request, from a single bucket kept in Redis. However many of them run, the
total stays inside KALSHI_REQUESTS_PER_MIN:

    bucket      refills at (budget - size) / 60 per second and holds `size`
                tokens, so no 60-second window can ever see more than `budget`;
                and size + refill stays within KALSHI_MAX_PER_SECOND, so no
                second can either (Kalshi limits per second: a burst past it
                is a 429, and a 429 halves everyone's budget)
    priority    a caller may only take a token while the bucket holds enough:
                discover and hot need 1, warm needs a quarter of the bucket
                spare, backfill half -- so backfill only runs on what the
                others leave
    429         one "too many requests" halves the budget for everyone at once;
                it grows back 10% per quiet 10 minutes

If Redis is unreachable each process falls back to its own bucket with a third
of the budget, so the guarantee holds while it is away.

Polymarket has a bucket of its own, the same in every way, sized by
POLY_REQUESTS_PER_MIN and POLY_MAX_PER_SECOND; each call names its venue.
"""

import logging
import threading
import time
from contextlib import contextmanager

import redis

import settings

log = logging.getLogger("ratelimit")

# Share of the bucket a priority may draw down.
SHARE = {"discover": 1.0, "hot": 1.0, "warm": 0.75, "api": 0.75, "backfill": 0.5}
KEY = settings.REDIS_PREFIX + "kalshi:"
MIN_FACTOR = 0.1
FALLBACK_SHARE = 1 / 3      # of the budget, per process, while Redis is away

# Redis' own clock, so processes with drifting clocks still agree.
_TAKE = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6
local budget, share, source = tonumber(ARGV[1]), tonumber(ARGV[2]), ARGV[3]
local f = tonumber(redis.call('GET', KEYS[2]) or '1')
local f_at = tonumber(redis.call('GET', KEYS[3]) or '0')
if f < 1 and now - f_at >= 600 then
  f = math.min(1, f * 1.1)
  redis.call('SET', KEYS[2], tostring(f))
  redis.call('SET', KEYS[3], tostring(now))
end
local b = budget * f
local ps = tonumber(ARGV[4])
local size = math.max(1, math.min(math.max(2, b / 30), ps - b / 60))
local rate = (b > size) and (b - size) / 60 or b / 120
rate = math.max(0.01, math.min(rate, ps - size))
local tokens = tonumber(redis.call('HGET', KEYS[1], 't') or tostring(size))
local ts = tonumber(redis.call('HGET', KEYS[1], 'ts') or tostring(now))
tokens = math.min(size, tokens + math.max(0, now - ts) * rate)
local need = 1 + (1 - share) * (size - 1)
local wait = 0
if tokens < need then
  wait = (need - tokens) / rate
else
  tokens = tokens - 1
  local m = KEYS[4] .. source .. ':' .. math.floor(now / 60)
  redis.call('INCR', m)
  redis.call('EXPIRE', m, 7200)
end
redis.call('HSET', KEYS[1], 't', tostring(tokens), 'ts', tostring(now))
redis.call('EXPIRE', KEYS[1], 3600)
return tostring(wait)
"""

_THROTTLED = """
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1e6
local f = tonumber(redis.call('GET', KEYS[2]) or '1')
local f_at = tonumber(redis.call('GET', KEYS[3]) or '0')
-- Several requests in flight all see the same 429; halve once for the burst.
if now - f_at > 5 then
  f = math.max(tonumber(ARGV[1]), f / 2)
  redis.call('SET', KEYS[2], tostring(f))
end
redis.call('SET', KEYS[3], tostring(now))
redis.call('HSET', KEYS[1], 't', '0', 'ts', tostring(now))
redis.call('INCR', KEYS[4] .. 'throttled:' .. math.floor(now / 60))
return tostring(f)
"""

def prefix(venue):
    return f"{settings.REDIS_PREFIX}{venue}:"


def _keys(venue):
    k = prefix(venue)
    return [k + "bucket", k + "factor", k + "factor_at", k + "used:"]


def limits(venue):
    """(requests a minute, most in any second) for this venue."""
    if venue == "poly":
        return settings.POLY_REQUESTS_PER_MIN, settings.POLY_MAX_PER_SECOND
    return settings.KALSHI_REQUESTS_PER_MIN, settings.KALSHI_MAX_PER_SECOND


_local = threading.local()
_lock = threading.Lock()
_client = None
_scripts = {}
_down_until = 0.0
_fallbacks = {}


def _fallback(venue):
    return _fallbacks.setdefault(venue, {"tokens": 0.0, "ts": time.monotonic(),
                                         "factor": 1.0, "factor_at": 0.0})

DEFAULT = "discover"        # each program sets its own


def shape(budget, per_second):
    """(size, refill per second) of the bucket for `budget` a minute: no
    60-second window above the budget, no second above `per_second`. The same
    sums as the Lua script."""
    size = max(1.0, min(max(2.0, budget / 30), per_second - budget / 60))
    rate = (budget - size) / 60 if budget > size else budget / 120
    return size, max(0.01, min(rate, per_second - size))


def effective_budget(venue="kalshi"):
    """Requests a minute the bucket really allows now: the budget after any
    429 cut, and within the per-second cap."""
    budget, per_second = limits(venue)
    size, rate = shape(budget * factor(venue), per_second)
    return size + rate * 60


def client():
    global _client
    if _client is None:
        _client = redis.Redis.from_url(settings.REDIS_URL, socket_timeout=2,
                                       socket_connect_timeout=2, decode_responses=True)
    return _client


def _script(name, body):
    if name not in _scripts:
        _scripts[name] = client().register_script(body)
    return _scripts[name]


@contextmanager
def using(priority):
    """Requests made inside this block, on this thread, use `priority`."""
    before = getattr(_local, "priority", None)
    _local.priority = priority
    try:
        yield
    finally:
        _local.priority = before


def current():
    return getattr(_local, "priority", None) or DEFAULT


def _redis_ok():
    return time.monotonic() >= _down_until


def _redis_failed(exc):
    global _down_until
    if _redis_ok():
        log.warning("redis unreachable (%s); using a local third of each budget for 30s", exc)
    _down_until = time.monotonic() + 30


def _take_local(share, venue):
    """The same bucket, in-process, with a third of the budget."""
    with _lock:
        now = time.monotonic()
        f = _fallback(venue)
        if f["factor"] < 1 and now - f["factor_at"] >= 600:
            f["factor"], f["factor_at"] = min(1.0, f["factor"] * 1.1), now
        budget, per_second = limits(venue)
        size, rate = shape(budget * FALLBACK_SHARE * f["factor"], per_second * FALLBACK_SHARE)
        f["tokens"] = min(size, f["tokens"] + (now - f["ts"]) * rate)
        f["ts"] = now
        need = 1 + (1 - share) * (size - 1)
        if f["tokens"] < need:
            return (need - f["tokens"]) / rate
        f["tokens"] -= 1
        return 0.0


def acquire(stop=None, venue="kalshi"):
    """Block until this thread may send one request to `venue`.

    Returns False only if `stop` was set while waiting.
    """
    priority = current()
    share = SHARE[priority]
    budget, per_second = limits(venue)
    while True:
        wait = None
        if _redis_ok():
            try:
                wait = float(_script("take", _TAKE)(
                    keys=_keys(venue), args=[budget, share, priority, per_second]))
            except redis.RedisError as exc:
                _redis_failed(exc)
        if wait is None:
            wait = _take_local(share, venue)
        if wait <= 0:
            return True
        # Short naps: a higher priority may take the token first, and that is
        # the point.
        nap = min(max(wait, 0.01), 1.0)
        if stop is not None:
            if stop.wait(nap):
                return False
        else:
            time.sleep(nap)


def throttled(venue="kalshi"):
    """The venue answered 429: halve its budget for every program."""
    log.warning("%s said too many requests; halving its request budget", venue)
    if _redis_ok():
        try:
            _script("throttled", _THROTTLED)(keys=_keys(venue), args=[MIN_FACTOR])
        except redis.RedisError as exc:
            _redis_failed(exc)
    with _lock:
        now = time.monotonic()
        f = _fallback(venue)
        if now - f["factor_at"] > 5:
            f["factor"] = max(MIN_FACTOR, f["factor"] / 2)
        f["factor_at"], f["tokens"] = now, 0.0


def factor(venue="kalshi"):
    """The share of the budget in force right now (1.0 = no throttling)."""
    try:
        return float(client().get(prefix(venue) + "factor") or 1)
    except redis.RedisError:
        return _fallback(venue)["factor"]


def used(source, minutes, venue="kalshi"):
    """Requests `source` made in the last `minutes` whole minutes, or None."""
    try:
        now = int(client().time()[0]) // 60
        keys = [f"{prefix(venue)}used:{source}:{m}" for m in range(now - minutes, now)]
        return sum(int(v or 0) for v in client().mget(keys))
    except redis.RedisError:
        return None


def per_minute(source, since_minute, until_minute, venue="kalshi"):
    """{minute: requests} for `source`, by Redis minute number. For tests."""
    keys = [f"{prefix(venue)}used:{source}:{m}" for m in range(since_minute, until_minute + 1)]
    return {m: int(v or 0) for m, v in zip(range(since_minute, until_minute + 1),
                                           client().mget(keys))}

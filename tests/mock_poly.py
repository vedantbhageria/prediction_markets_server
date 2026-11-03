"""A stand-in for Polymarket's public APIs, for testing the collectors end to end.

    python tests/mock_poly.py --port 18400 --start 1793600000 [--scale 1]

One server answers for all three of Polymarket's hosts -- point
POLY_GAMMA_URL, POLY_CLOB_URL and POLY_DATA_URL at it:

    gamma   /events/keyset, /markets          the listing, with volumes
    data    /v2/trades                        the feed, or one market's trades
            (/trades, v1, answers 410 as the real one does since 2 Nov 2026)
    clob    /prices-history                   per-minute midpoints

Some 2,000 made-up markets trade deterministically from (seed, market,
minute), so a test can work out every trade -- and every candle -- exactly.

Made to be awkward in the ways the real one is:
  - every answer comes from a copy running 3 to 20 s behind (a different
    amount each time), so two requests a second apart can disagree about
    what has happened
  - trade lists come a page of at most MAX_PAGE, then by cursor; v1's
    parameters (market, offset) are refused
  - total volume runs LISTING_LAG behind the trades, and "stale" markets never
    update their total at all -- only their 24-hour volume moves
  - markets start during the run, and some closed hours ago

and it can be told to misbehave through GET /_control?mode= on this port:
  normal, down (connections dropped), 429 (most requests refused), slow.
GET /_stats returns the requests it has seen; GET /_truth?market=&from=&to=
returns every trade of a market in a window, to check against.
"""

import argparse
import base64
import bisect
import json
import random
import threading
import time
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SEED = 20261102
LISTING_LAG = 30            # total volume trails the trades by this much
LAG_MIN, LAG_MAX = 3, 20    # how far behind each answering copy runs
MAX_PAGE = 1000

# kind: (chance a minute has trades, most trades in such a minute, count)
KINDS = {"hot": (1.0, 20, 60), "huge": (1.0, 50, 3), "warm": (0.3, 3, 300),
         "stale": (0.3, 3, 50), "cold": (0.01, 2, 700), "never": (0.0, 0, 500),
         "new": (0.5, 4, 20), "closed": (0.2, 3, 150)}


def mix(a, b):
    """A well-spread 32-bit hash of two integers (splitmix-style)."""
    x = (a * 0x9E3779B97F4A7C15 + b * 0xBF58476D1CE4E5B9 + SEED) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 30
    x = (x * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 27
    x = (x * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    return (x ^ (x >> 31)) & 0xFFFFFFFF


def order(t):
    """The venue's order: newest first, ties by hash."""
    return (-t["timestamp"], t["transaction_hash"])


class Universe:
    def __init__(self, t0, scale=1, history_hours=36):
        r = random.Random(SEED)
        self.t0 = t0
        self.markets = []
        i = 0
        for kind, (_, _, count) in KINDS.items():
            for _ in range(count * scale):
                start = t0 - r.uniform(0.2, history_hours * 1.3) * 3600
                end, closed = t0 + r.uniform(2, 30) * 86400, False
                if kind == "new":
                    start = t0 + r.uniform(120, 1200)
                if kind == "huge":
                    start = t0 - history_hours * 3600 * 1.2
                if kind == "closed":
                    end, closed = t0 - r.uniform(1, 20) * 3600, True
                    start = min(start, end - 3600)
                cid = "0x" + format(r.getrandbits(256), "064x")
                self.markets.append({
                    "i": i, "kind": kind, "cid": cid, "start": int(start), "end": int(end),
                    "closed": closed, "yes": str(r.getrandbits(250)), "no": str(r.getrandbits(250)),
                    "event": 1000 + i // 3, "question": f"Will made-up thing #{i} ({kind}) happen?"})
                i += 1
        self.by_cid = {m["cid"]: m for m in self.markets}
        self.trading = [m for m in self.markets if KINDS[m["kind"]][0] > 0]
        self._cum = {}          # cid -> (minutes with trades, cumulative notional)
        self._cum_lock = threading.Lock()

    # -- trades --------------------------------------------------------------

    def price(self, m, minute):
        """The yes price drifting minute by minute, in (0.02, 0.98)."""
        x = mix(m["i"], minute // 30) / 2 ** 32
        y = mix(m["i"] + 7, minute // 30 + 1) / 2 ** 32
        f = (minute % 30) / 30
        return round(0.02 + 0.96 * (x * (1 - f) + y * f), 4)

    def live(self, m, minute):
        t = minute * 60
        return m["start"] - 59 <= t and (not m["closed"] or t < m["end"])

    @lru_cache(maxsize=400_000)
    def minute_trades(self, i, minute):
        """Every trade of market i in this minute, oldest first, as the venue
        would list them."""
        m = self.markets[i]
        chance, most, _ = KINDS[m["kind"]]
        if not chance or not self.live(m, minute) or mix(i, minute) % 10_000 >= chance * 10_000:
            return ()
        r = random.Random(mix(i, minute) * 1_000_003 + minute)
        out = []
        for _ in range(r.randint(1, most)):
            ts = minute * 60 + r.randrange(60)
            if ts < m["start"] or (m["closed"] and ts >= m["end"]):
                continue
            yes = r.random() < 0.6
            p = min(0.999, max(0.001, self.price(m, minute) + r.uniform(-0.01, 0.01)))
            price = round(p if yes else 1 - p, 3)
            out.append({
                "proxy_wallet": "0x" + format(r.getrandbits(160), "040x"),
                "side": "BUY" if r.random() < 0.55 else "SELL",
                "token_id": m["yes"] if yes else m["no"], "condition_id": m["cid"],
                "size": round(r.uniform(1, 500), 2), "price": price, "timestamp": ts,
                "title": m["question"], "slug": f"thing-{i}", "outcome": "Yes" if yes else "No",
                "outcome_index": 0 if yes else 1,
                "transaction_hash": "0x" + format(r.getrandbits(256), "064x"),
                "name": f"trader{r.randrange(500)}", "pseudonym": "Made-Up-Trader"})
        out.sort(key=lambda t: t["timestamp"])
        return tuple(out)

    @lru_cache(maxsize=20_000)
    def feed_minute(self, minute):
        """Every trade on the venue in this minute, newest first."""
        out = []
        for m in self.trading:
            out += self.minute_trades(m["i"], minute)
        out.sort(key=order)
        return tuple(out)

    def page(self, m, cutoff, anchor, limit):
        """A page of the feed (m None) or of one market's trades, newest first:
        up to `cutoff` for the first page, else strictly after `anchor` (the
        (time, hash) of the last trade of the page before). Returns (trades,
        more)."""
        floor = (min(x["start"] for x in self.markets) if m is None else m["start"] - 60) // 60
        out, minute = [], (anchor[0] if anchor else cutoff) // 60
        while len(out) <= limit and minute >= floor:
            trades = (self.feed_minute(minute) if m is None
                      else sorted(self.minute_trades(m["i"], minute), key=order))
            out += [t for t in trades
                    if (order(t) > (-anchor[0], anchor[1]) if anchor else t["timestamp"] <= cutoff)]
            minute -= 1
        return out[:limit], len(out) > limit

    def truth(self, m, lo, hi):
        """Every trade of the market with lo <= ts < hi, oldest first."""
        out = []
        for minute in range(max(lo, m["start"] - 60) // 60, hi // 60 + 1):
            out += [t for t in self.minute_trades(m["i"], minute) if lo <= t["timestamp"] < hi]
        return out

    # -- volumes -----------------------------------------------------------------

    def notional_to(self, m, t):
        """USDC traded in the market before time t."""
        with self._cum_lock:
            minutes, cum, upto = self._cum.get(m["cid"], ([], [], m["start"] // 60 - 1))
            target = int(t) // 60
            for minute in range(upto + 1, target):
                trades = self.minute_trades(m["i"], minute)
                if trades:
                    minutes.append(minute)
                    cum.append((cum[-1] if cum else 0) + sum(x["size"] * x["price"] for x in trades))
            self._cum[m["cid"]] = (minutes, cum, max(upto, target - 1))
            k = bisect.bisect_left(minutes, target)
            return cum[k - 1] if k else 0.0

    def listing(self, m, now):
        if m["start"] > now:
            return None
        seen = now - LISTING_LAG
        total = self.notional_to(m, seen)
        day = total - self.notional_to(m, seen - 86400)
        if m["kind"] == "stale":
            total = round(self.notional_to(m, m["start"] + 3600), 4)    # never updated
        iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))
        return {"conditionId": m["cid"], "question": m["question"], "slug": f"thing-{m['i']}",
                "clobTokenIds": json.dumps([m["yes"], m["no"]]), "outcomes": '["Yes", "No"]',
                "startDate": iso(m["start"]), "createdAt": iso(m["start"]),
                "endDate": iso(m["end"]), "closed": m["closed"],
                "closedTime": iso(m["end"]) if m["closed"] else None,
                "volume": f"{total:.6f}", "volume24hr": round(day, 6),
                "liquidity": "1000", "description": f"Resolves on made-up thing #{m['i']}."}

    def midpoints(self, m, lo, hi):
        return [{"t": minute * 60, "p": self.price(m, minute)}
                for minute in range(lo // 60, hi // 60 + 1)
                if lo <= minute * 60 <= hi and minute * 60 >= m["start"]]


class Handler(BaseHTTPRequestHandler):
    universe = None
    mode = "normal"
    stats = []
    lock = threading.Lock()
    protocol_version = "HTTP/1.1"

    def log_message(self, *args):
        pass

    def send(self, code, body):
        data = json.dumps(body).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        url = urlparse(self.path)
        q = {k: v[-1] for k, v in parse_qs(url.query).items()}
        path, u = url.path, Handler.universe
        if path == "/_control":
            Handler.mode = q.get("mode", "normal")
            return self.send(200, {"mode": Handler.mode})
        if path == "/_stats":
            with Handler.lock:
                return self.send(200, Handler.stats[-int(q.get("last", 100000)):])
        if path == "/_truth":
            m = u.by_cid[q["market"]]
            return self.send(200, u.truth(m, int(float(q["from"])), int(float(q["to"]))))
        if path == "/_markets":
            return self.send(200, [{k: m[k] for k in ("cid", "kind", "start", "end", "closed",
                                                      "yes", "no")} for m in u.markets])
        if Handler.mode == "down":
            self.close_connection = True
            self.connection.shutdown(2)
            return
        if Handler.mode == "slow":
            time.sleep(20)
        with Handler.lock:
            Handler.stats.append({"at": time.time(), "path": path})
        if Handler.mode == "429" and random.random() < 0.8:
            return self.send(429, {"error": "too many requests"})
        now = time.time()
        cutoff = int(now - random.uniform(LAG_MIN, LAG_MAX))
        if path == "/trades":
            return self.send(410, {"error": "Data API v1 has been retired",
                                   "migration": "https://data-api.polymarket.com/v2/"})
        if path == "/v2/trades":
            bad = [k for k in ("market", "offset", "takerOnly") if k in q]
            if bad:
                return self.send(400, {"error": f"unknown parameter {bad[0]}"})
            if q.get("cursor"):
                c = json.loads(base64.urlsafe_b64decode(q["cursor"]))
                anchor, limit = (c["ts"], c["h"]), c["l"]
            else:
                anchor, limit = None, int(q.get("limit", 100))
                if not 0 <= limit <= MAX_PAGE:
                    return self.send(400, {"error": f"limit must be between 0 and {MAX_PAGE}"})
            m = None
            if q.get("condition"):
                m = u.by_cid.get(q["condition"])
                if m is None:
                    return self.send(200, {"data": [], "pagination": {
                        "limit": limit, "offset": 0, "has_more": False, "next_cursor": None}})
            rows, more = u.page(m, cutoff, anchor, limit)
            cursor = None
            if more and rows:
                cursor = base64.urlsafe_b64encode(json.dumps(
                    {"ts": rows[-1]["timestamp"], "h": rows[-1]["transaction_hash"], "l": limit}
                ).encode()).decode()
            return self.send(200, {"data": rows, "pagination": {
                "limit": limit, "offset": 0, "has_more": more, "next_cursor": cursor}})
        if path == "/events/keyset":
            closed = q.get("closed") == "true"
            end_min = q.get("end_date_min")
            end_min = (time.mktime(time.strptime(end_min, "%Y-%m-%dT%H:%M:%SZ")) - time.timezone
                       if end_min else None)
            events = {}
            for m in u.markets:
                if m["closed"] != closed or (end_min and m["end"] < end_min):
                    continue
                row = u.listing(m, now)
                if row:
                    events.setdefault(m["event"], []).append(row)
            ids = sorted(events)
            after = int(q.get("after_cursor") or 0)
            page = [e for e in ids if e > after][:int(q.get("limit", 100))]
            body = {"events": [{"id": e, "slug": f"event-{e}", "title": f"Made-up event {e}",
                                "tags": [{"slug": "made-up"}, {"slug": f"group-{e % 7}"}],
                                "markets": events[e]} for e in page]}
            body["next_cursor"] = str(page[-1]) if len(page) == int(q.get("limit", 100)) else None
            return self.send(200, body)
        if path == "/markets":
            ids = parse_qs(url.query).get("condition_ids", [])
            return self.send(200, [row for cid in ids if cid in u.by_cid
                                   for row in [u.listing(u.by_cid[cid], now)] if row])
        if path == "/prices-history":
            m = next((m for m in u.markets if q.get("market") in (m["yes"], m["no"])), None)
            if not m:
                return self.send(200, {"history": []})
            return self.send(200, {"history": u.midpoints(m, int(q["startTs"]), int(q["endTs"]))})
        return self.send(404, {"error": "not found"})


def serve(port, start, scale=1, history_hours=36):
    Handler.universe = Universe(start, scale, history_hours)
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18400)
    parser.add_argument("--start", type=float, default=time.time())
    parser.add_argument("--scale", type=int, default=1)
    parser.add_argument("--history-hours", type=float, default=36)
    args = parser.parse_args()
    serve(args.port, args.start, args.scale, args.history_hours)

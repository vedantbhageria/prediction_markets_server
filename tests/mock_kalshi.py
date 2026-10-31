"""A stand-in for Kalshi's public API, for testing the collectors end to end.

    python tests/mock_kalshi.py --port 18080 --state /tmp/pmtest/markets.json

Serves the endpoints the collectors use -- /events, /markets, /series and
/markets/candlesticks -- over about a thousand made-up markets whose trades
are generated deterministically from (seed, market, minute), so the test can
work out exactly what every candle should be.

Made to be awkward in the ways the real one is, or might be:
  - a few trades only show up in candles up to 45 s after they happen
  - the market listing's volume runs 20 s behind the trades
  - minutes with quotes but no trade come back as candles with no price
  - more than 100 markets or 10,000 market-minutes per request is a 400
  - more than 20 requests in a second is a 429

and it can be told to misbehave on purpose, through GET /_control?mode=:
  normal   as above
  down     every connection is dropped without an answer (the network is out)
  429      most requests get "too many requests"
  slow     every answer takes 20 s (the network is crawling)

GET /_stats returns every request it has seen, for checking the budget.
"""

import argparse
import json
import random
import threading
import time
from collections import deque
from functools import lru_cache
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

SEED = 20261031
PREFIX = "/trade-api/v2"
LISTING_LAG = 20            # the listing's volume trails the trades by this much
LIMIT_PER_SECOND = 20

# kind: (chance of any trade in a minute, most trades in such a minute)
KINDS = {"hot": (0.9, 6), "warm": (0.06, 3), "cold": (0.003, 2), "never": (0.0, 0),
         "new_hot": (0.6, 4), "new_warm": (0.08, 2), "closing": (0.3, 3),
         "closed": (0.12, 3)}


# ---------------------------------------------------------------------------
# The made-up market universe
# ---------------------------------------------------------------------------

def build(t0):
    """Markets and events around start time t0, the same for the same t0."""
    r = random.Random(SEED)
    plan = ([("hot", 30), ("warm", 120), ("cold", 500), ("never", 300)],
            [("new_hot", 10), ("new_warm", 10)], [("closing", 25)], [("closed", 80)])
    markets = []
    for group in plan:
        for kind, count in group:
            for _ in range(count):
                if kind.startswith("new"):
                    opened, closes = t0 + r.uniform(60, 20 * 60), t0 + 5 * 86400
                elif kind == "closing":
                    opened, closes = t0 - r.uniform(5, 20) * 3600, t0 + r.uniform(4, 22) * 60
                elif kind == "closed":
                    opened = t0 - r.uniform(2, 4) * 86400
                    closes = t0 - r.uniform(2, 40) * 3600
                else:
                    opened, closes = t0 - r.uniform(2, 30) * 3600, t0 + 10 * 86400
                # a few open markets reach back past the test's 2-day backfill window
                if kind == "warm" and r.random() < 0.1:
                    opened = t0 - 3 * 86400
                markets.append({"kind": kind, "open": opened, "close": closes,
                                "base": r.uniform(0.1, 0.9)})
    # Events of one to three markets that share a close time.
    events, i = [], 0
    while i < len(markets):
        size = min(r.randint(1, 3), len(markets) - i)
        head = markets[i]
        ev = {"event_ticker": f"MOCK{len(events):04d}", "series_ticker": f"MSER{len(events) % 7}",
              "close": head["close"], "markets": []}
        for j in range(i, i + size):
            m = markets[j]
            m["close"] = head["close"]
            m["ticker"] = f"{ev['event_ticker']}-M{j:04d}"
            m["index"] = j
            m["event_ticker"] = ev["event_ticker"]
            ev["markets"].append(j)
        events.append(ev)
        i += size
    return markets, events


@lru_cache(maxsize=1_000_000)
def _minute_trades(index, kind, base, minute):
    """[(ts, price, size, delay)] for one market's minute, always the same."""
    chance, most = KINDS[kind]
    if not chance:
        return ()
    h = hash((SEED, index, minute)) & 0xFFFFFFFF
    if (h % 100_000) / 100_000 >= chance:
        return ()
    r = random.Random(h)
    out = []
    for _ in range(r.randint(1, most)):
        ts = minute + r.uniform(0, 59.999)
        drift = 0.08 * ((minute // 60) % 17 - 8) / 8
        price = round(min(0.99, max(0.01, base + drift + r.uniform(-0.03, 0.03))), 2)
        delay = r.uniform(0, 45) if r.random() < 0.1 else 0.0
        out.append((ts, price, r.randint(1, 50), delay))
    out.sort()
    return tuple(out)


def trades(m, minute):
    """Trades in one minute of market m, inside its open-to-close life."""
    if minute + 60 <= m["open"] or minute >= m["close"]:
        return ()
    return tuple(t for t in _minute_trades(m["index"], m["kind"], m["base"], minute)
                 if m["open"] <= t[0] < m["close"])


_cum_lock = threading.Lock()
_cum = {}       # index -> (first minute, [volume before each minute])


def volume_at(m, t):
    """Total size of the trades in market m up to and including time t."""
    first = int(m["open"]) // 60 * 60
    if t < first:
        return 0
    upto = int(min(t, m["close"])) // 60 * 60
    with _cum_lock:
        start, cum = _cum.setdefault(m["index"], (first, [0]))
        while start + (len(cum) - 1) * 60 < upto:
            minute = start + (len(cum) - 1) * 60
            cum.append(cum[-1] + sum(x[2] for x in trades(m, minute)))
        before = cum[(upto - start) // 60]
    return before + sum(x[2] for x in trades(m, upto) if x[0] <= t)


def candle(m, minute, now=None):
    """The candle Kalshi would give for this minute: (o, h, l, c, volume), or
    None without a trade. `now` hides trades not visible yet."""
    got = [x for x in trades(m, minute) if now is None or x[0] + x[3] <= now]
    if not got:
        return None
    prices = [x[1] for x in got]
    return prices[0], max(prices), min(prices), prices[-1], sum(x[2] for x in got)


# ---------------------------------------------------------------------------
# The API
# ---------------------------------------------------------------------------

class Kalshi:
    def __init__(self, t0):
        self.t0 = t0
        self.markets, self.events = build(t0)
        self.by_ticker = {m["ticker"]: m for m in self.markets}
        self.mode = "normal"
        self.log = []           # (time, path, status)
        self.recent = deque()
        self.lock = threading.Lock()

    def status(self, m, now):
        if now < m["open"]:
            return "initialized"
        if now < m["close"]:
            return "active"
        return "settled" if now >= m["close"] + 120 else "closed"

    def payload(self, m, now):
        status = self.status(m, now)
        iso = lambda t: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t))  # noqa: E731
        return {"ticker": m["ticker"], "event_ticker": m["event_ticker"],
                "title": f"Mock market {m['index']} ({m['kind']})", "status": status,
                "result": "yes" if status == "settled" else "",
                "open_time": iso(m["open"]), "close_time": iso(m["close"]),
                "strike_type": "greater", "floor_strike": 100 + m["index"], "cap_strike": None,
                "volume_fp": f"{volume_at(m, now - LISTING_LAG):.2f}",
                "open_interest_fp": "100.00"}

    def events_page(self, q, now):
        status = q.get("status", ["open"])[0]
        min_close = float(q.get("min_close_ts", [0])[0] or 0)
        limit = min(int(q.get("limit", [200])[0]), 200)
        offset = int(q.get("cursor", ["0"])[0] or 0)

        def keep(ev):
            if status == "open":
                return now < ev["close"]
            if ev["close"] < min_close:
                return False
            if status == "closed":
                return ev["close"] <= now < ev["close"] + 120
            return now >= ev["close"] + 120             # settled

        chosen = [ev for ev in self.events if keep(ev)]
        page = chosen[offset:offset + limit]
        out = [{"event_ticker": ev["event_ticker"], "series_ticker": ev["series_ticker"],
                "title": f"Mock event {ev['event_ticker']}", "sub_title": "",
                "category": "Mock", "mutually_exclusive": False, "strike_period": "",
                "markets": [self.payload(self.markets[j], now) for j in ev["markets"]]}
               for ev in page]
        nxt = str(offset + limit) if offset + limit < len(chosen) else ""
        return 200, {"events": out, "cursor": nxt}

    def candlesticks(self, q, now):
        tickers = [t for t in q.get("market_tickers", [""])[0].split(",") if t]
        start, end = int(q["start_ts"][0]), int(q["end_ts"][0])
        if len(tickers) > 100:
            return 400, {"error": {"code": "bad_request", "details": "too many tickers"}}
        asked = len(tickers) * ((end - start) // 60 + 1)
        if asked > 10_000:
            return 400, {"error": {"code": "bad_request",
                                   "details": f"requested candlesticks across all markets: {asked},"
                                              " max candlesticks: 10000"}}
        out = []
        for ticker in tickers:
            m = self.by_ticker.get(ticker)
            if m is None:
                continue
            sticks = []
            first = -(-start // 60) * 60
            for close_ts in range(first, end + 1, 60):
                minute = close_ts - 60
                if close_ts > now + 60 or minute + 60 <= m["open"] or minute >= m["close"]:
                    continue
                c = candle(m, minute, now)
                if c:
                    o, h, l, cl, v = c
                    sticks.append({"end_period_ts": close_ts, "volume_fp": f"{v:.2f}",
                                   "open_interest_fp": "100.00",
                                   "price": {"open_dollars": f"{o:.4f}", "high_dollars": f"{h:.4f}",
                                             "low_dollars": f"{l:.4f}", "close_dollars": f"{cl:.4f}"},
                                   "yes_bid": {"close_dollars": "0.4000"},
                                   "yes_ask": {"close_dollars": "0.4200"}})
                elif hash((SEED, "quote", m["index"], minute)) % 5 == 0:
                    sticks.append({"end_period_ts": close_ts, "volume_fp": "0.00",
                                   "open_interest_fp": "100.00",
                                   "price": {"previous_dollars": "0.4100"},
                                   "yes_bid": {"close_dollars": "0.4000", "open_dollars": "0.4000",
                                               "high_dollars": "0.4000", "low_dollars": "0.4000"},
                                   "yes_ask": {"close_dollars": "0.4200", "open_dollars": "0.4200",
                                               "high_dollars": "0.4200", "low_dollars": "0.4200"}})
            out.append({"market_ticker": ticker, "candlesticks": sticks})
        return 200, {"markets": out}

    def answer(self, path, q):
        now = time.time()
        if path == PREFIX + "/events":
            return self.events_page(q, now)
        if path == PREFIX + "/markets":
            tickers = q.get("tickers", [""])[0].split(",")
            return 200, {"markets": [self.payload(self.by_ticker[t], now)
                                     for t in tickers if t in self.by_ticker]}
        if path == PREFIX + "/series":
            return 200, {"series": [{"ticker": f"MSER{i}", "category": "Mock",
                                     "title": f"Mock series {i}", "frequency": "daily"}
                                    for i in range(7)]}
        if path == PREFIX + "/markets/candlesticks":
            return self.candlesticks(q, now)
        return 404, {"error": "not found"}


def serve(port, state):
    t0 = time.time()
    k = Kalshi(t0)
    with open(state, "w", encoding="utf-8") as f:
        json.dump({"t0": t0}, f)

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def reply(self, status, body):
            data = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            url = urlparse(self.path)
            q = parse_qs(url.query)
            if url.path == "/_control":
                k.mode = q.get("mode", ["normal"])[0]
                return self.reply(200, {"mode": k.mode})
            if url.path == "/_stats":
                with k.lock:
                    return self.reply(200, {"requests": k.log, "mode": k.mode})
            now = time.time()
            mode = k.mode
            if mode == "down":
                with k.lock:
                    k.log.append((now, url.path, 0))
                self.close_connection = True
                return
            if mode == "slow":
                time.sleep(20)
            with k.lock:
                k.recent.append(now)
                while k.recent and k.recent[0] < now - 1:
                    k.recent.popleft()
                busy = len(k.recent) > LIMIT_PER_SECOND
            if busy or (mode == "429" and random.random() < 0.7):
                status, body = 429, {"error": "too many requests"}
            else:
                try:
                    status, body = k.answer(url.path, q)
                except Exception as exc:          # a bug here should be loud
                    status, body = 500, {"error": f"{type(exc).__name__}: {exc}"}
            with k.lock:
                k.log.append((now, url.path, status))
            self.reply(status, body)

    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    server.daemon_threads = True
    print(f"mock kalshi on :{port}, t0={t0:.0f}, {len(k.markets)} markets", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--state", required=True)
    args = parser.parse_args()
    serve(args.port, args.state)

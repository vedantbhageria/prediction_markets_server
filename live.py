"""Live Kalshi candles: keeps the newest edge of every market's history current.

    python live.py              # run forever (the live service)
    python live.py --cycles 3   # three one-minute cycles, then exit

Every minute, once the previous minute has settled (LIVE_SETTLE_SECONDS):

    hot    the markets on activity.py's hot list are fetched from where their
           candles stop up to now, re-reading the last LIVE_HOT_OVERLAP
           seconds so trades Kalshi records late still land
    warm   then, until the next minute is due, every market that traded since
           its candles were last fetched (candles_fetched_to before
           volume_measured_at) -- which is also how anything missed while this
           was stopped, or while Kalshi or the network was down, gets caught up
    cold   markets whose volume did not move need nothing: activity.py has
           already moved their coverage forward

Nothing here keeps state of its own: what to fetch is read from
kalshi_tracking each cycle and every successful request moves it forward, so
a crash or restart at any point loses nothing. Every request draws on the
shared budget in ratelimit.py: hot at full priority, warm at a lower one.
"""

import argparse
import json
import logging
import signal
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait

import activity
import db
import fetch
import ratelimit
import settings

log = logging.getLogger("live")

WARM_BATCH = 5000           # warm markets planned per query


def hot_spans(hot, end):
    """Spans for the hot markets: from their coverage, less the overlap, to `end`."""
    if not hot:
        return []
    rows = db.query("SELECT ticker, candles_fetched_to FROM kalshi_tracking"
                    " WHERE ticker = ANY(%s)", (list(hot),))
    return [(r["ticker"],
             fetch.minute(r["candles_fetched_to"].timestamp() - settings.HOT_OVERLAP), end)
            for r in rows]


def warm_spans(end, exclude):
    """Markets that traded since their candles were last fetched, busiest first."""
    rows = db.query("""
        SELECT ticker, candles_fetched_to FROM kalshi_tracking
        WHERE candles_fetched_to < volume_measured_at
          AND candles_fetched_to < %s
          AND NOT (ticker = ANY(%s))
        ORDER BY activity_score DESC, ticker
        LIMIT %s""", (fetch.stamp(end), list(exclude), WARM_BATCH))
    return [(r["ticker"], fetch.minute(r["candles_fetched_to"].timestamp()), end) for r in rows]


def current_hot():
    hot = activity.load_hot()
    if hot is not None:
        return hot
    # No list in Redis (it restarted, or activity.py has not run yet): work it
    # out the same way, without the memory of who was hot before.
    cap, _ = activity.hot_cap()
    return activity.pick_hot(cap)


class Live:
    def __init__(self, stop):
        self.stop = stop
        self.pool = ThreadPoolExecutor(max_workers=settings.FETCH_WORKERS,
                                       thread_name_prefix="fetch")

    def _work(self, priority, request):
        with ratelimit.using(priority):
            return fetch.run(request, self.stop)

    def run_requests(self, priority, requests, deadline):
        """Run planned requests, a few at a time, until done or `deadline`.
        Returns [requests done, requests failed, candles written]."""
        done = failed = candles = 0
        pending = set()
        todo = iter(requests)
        exhausted = False
        while True:
            while (not exhausted and len(pending) < settings.FETCH_WORKERS * 2
                   and time.time() < deadline and not self.stop.is_set()):
                request = next(todo, None)
                if request is None:
                    exhausted = True
                    break
                pending.add(self.pool.submit(self._work, priority, request))
            if not pending:
                return [done, failed, candles]
            finished, pending = wait(pending, timeout=1, return_when=FIRST_COMPLETED)
            for f in finished:
                n = f.result()
                if n is None:
                    failed += 1
                else:
                    done += 1
                    candles += n

    def cycle(self):
        """One minute's work. Returns the time the next one is due."""
        started = time.time()
        end = fetch.minute(started - settings.SETTLE_SECONDS)
        deadline = end + 60 + settings.SETTLE_SECONDS

        hot = current_hot()
        h = self.run_requests("hot", fetch.plan(hot_spans(hot, end)), deadline)

        w, warm_markets, exclude = [0, 0, 0], 0, set(hot)
        while time.time() < deadline - 5 and not self.stop.is_set():
            spans = warm_spans(end, exclude)
            if not spans:
                break
            warm_markets += len(spans)
            exclude.update(s[0] for s in spans)     # one try per market per cycle
            got = self.run_requests("warm", fetch.plan(spans), deadline)
            w = [a + b for a, b in zip(w, got)]

        log.info("%s  hot %s markets: %d requests (%d failed), %s candles"
                 " | warm %s markets: %d requests (%d failed), %s candles | %.0fs",
                 time.strftime("%H:%M", time.gmtime(end)), f"{len(hot):,}", h[0] + h[1], h[1],
                 f"{h[2]:,}", f"{warm_markets:,}", w[0] + w[1], w[1], f"{w[2]:,}",
                 time.time() - started)
        try:
            # For the API's /v1/status: how far behind live collection is.
            ratelimit.client().set(ratelimit.KEY + "live", json.dumps({
                "at": time.time(), "minute": end, "seconds": round(time.time() - started, 1),
                "hot": {"markets": len(hot), "requests": h[0] + h[1], "failed": h[1],
                        "candles": h[2]},
                "warm": {"markets": warm_markets, "requests": w[0] + w[1], "failed": w[1],
                         "candles": w[2]}}), ex=3600)
        except Exception:
            pass
        return deadline

    def run(self, cycles=None):
        n = 0
        while not self.stop.is_set():
            try:
                deadline = self.cycle()
            except Exception as exc:
                # The database or Redis went away mid-cycle. The bookkeeping
                # has whatever succeeded, so just go again next minute.
                log.warning("cycle failed: %s: %s", type(exc).__name__, exc)
                deadline = fetch.minute(time.time()) + 60 + settings.SETTLE_SECONDS
            n += 1
            if cycles and n >= cycles:
                break
            self.stop.wait(max(0.0, deadline - time.time()))
        self.pool.shutdown(wait=True, cancel_futures=True)
        log.info("stopped")


def main():
    parser = argparse.ArgumentParser(description="Live Kalshi one-minute candles.")
    parser.add_argument("--cycles", type=int, help="stop after this many one-minute cycles")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    ratelimit.DEFAULT = "hot"
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    Live(stop).run(args.cycles)


if __name__ == "__main__":
    main()

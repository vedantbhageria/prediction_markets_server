"""Live Polymarket trades: every trade on the venue, as it happens.

    python trades.py                # run forever (the trades service)
    python trades.py --cycles 3     # three feed reads, then exit

Two loops, each in its own thread:

    feed     every POLY_FEED_SECONDS, the feed of every Polymarket trade is
             read back to where the last read reached, and stored. A few
             requests a minute cover every market on the venue, however many
             there are. If the feed has moved on further than one read goes
             (POLY_FEED_MOST trades, ~1.5 hours) since the last, the gap is
             recorded and the feed starts again from what it reached.
    catch-up markets that traded where their coverage does not reach -- inside
             a gap, or while this was stopped -- are read one by one, busiest
             first, at a lower priority than the feed.

Candles are made from the stored trades by aggregate.py, a process of its own.
History before a market's coverage is backfill.py's (--venue poly).

Like live.py, nothing here keeps state of its own: the feed's coverage is in
poly_feed and each market's in poly_tracking, so a crash or restart at any
point loses nothing -- at worst a longer gap to catch up.
"""

import argparse
import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import poly
import ratelimit
import settings
import store

log = logging.getLogger("trades")

CATCH_UP_BATCH = 200        # markets picked per round
CATCH_UP_WORKERS = 4


def feed_loop(stop, cycles=None):
    n, read, new, last_report = 0, 0, 0, time.time()
    while not stop.is_set():
        started = time.time()
        try:
            with ratelimit.using("hot"):
                got, seen, gap = poly.poll_feed(stop)
            read, new = read + seen, new + got
            if gap:
                log.warning("feed gap: it no longer reaches back to %s; started again from %s."
                            " Markets that traded in between are caught up one by one",
                            time.strftime("%H:%M:%S", time.gmtime(gap[0])),
                            time.strftime("%H:%M:%S", time.gmtime(gap[1])))
        except Exception as exc:
            log.warning("feed read failed: %s: %s", type(exc).__name__, exc)
        n += 1
        if time.time() - last_report >= 60:
            state = poly.feed_state()
            log.info("feed: %s trades read, %s new in %.0fs; covered to %s (%.0fs behind)",
                     f"{read:,}", f"{new:,}", time.time() - last_report,
                     time.strftime("%H:%M:%S", time.gmtime(state[1])) if state else "-",
                     time.time() - state[1] if state else 0)
            read, new, last_report = 0, 0, time.time()
        if cycles and n >= cycles:
            return
        stop.wait(max(0.0, started + settings.POLY_FEED_SECONDS - time.time()))


def catch_up_loop(stop):
    pool = ThreadPoolExecutor(max_workers=CATCH_UP_WORKERS, thread_name_prefix="catch-up")

    def one(cid):
        with ratelimit.using("warm"):
            return None if stop.is_set() else poly.fetch_market(cid, stop)

    while not stop.is_set():
        try:
            todo = poly.behind(CATCH_UP_BATCH)
        except Exception as exc:
            log.warning("catch-up query failed: %s: %s", type(exc).__name__, exc)
            todo = []
        if not todo:
            stop.wait(15)
            continue
        started = time.time()
        results = list(pool.map(one, todo))
        ok = [r for r in results if r is not None]
        log.info("caught up %s markets (%s failed): %s new trades, %.0fs", f"{len(ok):,}",
                 f"{len(results) - len(ok):,}", f"{sum(r[0] for r in ok):,}",
                 time.time() - started)
        if not ok:
            stop.wait(30)
    pool.shutdown(wait=True, cancel_futures=True)


def main():
    parser = argparse.ArgumentParser(description="Live Polymarket trades.")
    parser.add_argument("--cycles", type=int, help="stop after this many feed reads")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    ratelimit.DEFAULT = "hot"
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    store.ensure_schema()
    if args.cycles:
        feed_loop(stop, args.cycles)
        return
    catcher = threading.Thread(target=catch_up_loop, args=(stop,), name="catch-up", daemon=True)
    catcher.start()
    feed_loop(stop)
    catcher.join(timeout=60)
    log.info("stopped")


if __name__ == "__main__":
    main()

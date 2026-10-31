"""The live collector. Runs forever; meant to sit under systemd.

    python live.py                  # collect into PostgreSQL
    python live.py --dry-run        # no database: print candles for a few markets
    python live.py --no-prune       # never delete expired markets

Three things happen side by side:

  markets store  on start, and every MARKET_SYNC_MINUTES after, the open
                 markets are re-read from both venues and anything that closed
                 more than MARKET_RETENTION_DAYS ago is deleted. On start the
                 store is also filled with markets that closed inside that
                 window, so a stop of any length leaves no gap.
  live candles   each venue's busiest open markets are polled every
                 LIVE_POLL_SECONDS and folded into one-minute candles
  backfill       a market's one-minute history is fetched the first time it is
                 collected, which on a fresh start means all of them

Every knob is in settings.py.
"""

import argparse
import logging
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import catalogue
import settings
import store
import venues
from backfill import BACKFILL
from candles import CandleBuilder

log = logging.getLogger("live")

# Every stored candle is stamped with the start of the period it covers.
STAMP = {"kalshi": "start", "poly": "start"}
PENDING_MAX = 200_000       # candles held in memory while the database is away


class Collector(threading.Thread):
    """Polls one venue and writes its candles."""

    def __init__(self, venue, stop, dry=False):
        super().__init__(name=venue, daemon=True)
        self.venue, self.stop, self.dry = venue, stop, dry
        # Heartbeat = width: every candle is written, moved or not.
        self.builder = CandleBuilder(settings.CANDLE_SECONDS,
                                     settings.CANDLE_SECONDS, STAMP[venue])
        self.lock = threading.Lock()
        self.markets = {}       # key -> token
        self.pending = []
        self.written = 0

    def track(self, markets):
        """Swap in a new set of markets. Returns the keys that are new."""
        with self.lock:
            fresh = [k for k in markets if k not in self.markets]
            gone = [k for k in self.markets if k not in markets]
            self.markets = dict(markets)
            for key in gone:
                self.builder.forget(key)
        return fresh

    def samples(self, markets):
        """[(key, quote, trade, running volume, open interest)] right now."""
        if self.venue == "kalshi":
            out = []
            for m in venues.kalshi_markets(list(markets)):
                if m.get("status") not in ("active", "open"):
                    continue    # closed since the last sync; nothing left to price
                out.append((m["ticker"],) + venues.kalshi_sample(m))
            return out
        mids = venues.poly_midpoints(list(markets.values()))
        # Polymarket's mid carries no size, so its candles have no volume --
        # the same as its minute history.
        return [(key, mids[token], None, None, None)
                for key, token in markets.items() if token in mids]

    def run(self):
        poll = settings.POLL_SECONDS
        due = (time.time() // poll + 1) * poll
        while not self.stop.wait(max(0.0, due - time.time())):
            due += poll
            if due < time.time():           # a slow cycle; skip the missed slots
                due = (time.time() // poll + 1) * poll
            with self.lock:
                markets = dict(self.markets)
            if not markets:
                continue
            try:
                got = self.samples(markets)
            except Exception as exc:
                log.warning("%s poll failed: %s: %s", self.venue, type(exc).__name__, exc)
                continue
            now = time.time()
            with self.lock:
                for key, quote, trade, volume, oi in got:
                    self.builder.add(key, now, quote, trade, volume, oi)
                candles = self.builder.flush(now)
            if candles:
                self.write(candles)
        self.write(self.builder.flush(time.time() + settings.CANDLE_SECONDS))

    def write(self, candles):
        if self.dry:
            for key, ts, o, h, l, c, volume, oi in candles:
                print(f"{self.venue:<6} {ts:%H:%M:%S}  {key[:44]:<44}"
                      f"  o={o:.4f} h={h:.4f} l={l:.4f} c={c:.4f}"
                      f"  vol={'-' if volume is None else f'{volume:g}'}", flush=True)
            self.written += len(candles)
            return
        self.pending += candles
        try:
            store.write_candles(self.venue, self.pending)
        except Exception as exc:
            # Keep them for the next cycle; a database restart should not cost
            # more than it has to.
            del self.pending[:-PENDING_MAX]
            log.warning("%s: %d candles waiting on the database (%s)",
                        self.venue, len(self.pending), exc)
            return
        self.written += len(self.pending)
        self.pending = []


def dry_markets(venue, n):
    """A handful of busy markets straight from the API, for --dry-run."""
    if venue == "kalshi":
        found = []
        for page, _ in zip(venues.kalshi_events("open"), range(3)):
            for event in page:
                found += event.get("markets") or []
        found.sort(key=lambda m: -float(m.get("volume_24h_fp") or 0))
        return {m["ticker"]: m["ticker"] for m in found[:n]}
    found = []
    for page in venues.poly_events(100):
        for event in page:
            found += [m for m in event.get("markets") or [] if venues.yes_token(m)]
    found.sort(key=lambda m: -float(m.get("volume24hr") or 0))
    return {m["conditionId"]: venues.yes_token(m) for m in found[:n]}


def run_backfill(collector, key):
    try:
        BACKFILL[collector.venue](key)
    except Exception as exc:
        log.warning("%s: backfill of %s failed: %s: %s", collector.venue, key,
                    type(exc).__name__, exc)


def retrack(collectors, pool, backfilled):
    for venue, collector in collectors.items():
        fresh = collector.track(store.tracked(venue))
        todo = [k for k in fresh if (venue, k) not in backfilled]
        backfilled.update((venue, k) for k in todo)
        for key in todo:
            pool.submit(run_backfill, collector, key)
        if fresh:
            log.info("%s: collecting %d markets (%d new, backfilling %d)",
                     venue, len(collector.markets), len(fresh), len(todo))


def serve(stop, prune=True):

    store.ensure_schema()
    collectors = {v: Collector(v, stop) for v in settings.VENUES}
    pool = ThreadPoolExecutor(max_workers=settings.BACKFILL_WORKERS,
                              thread_name_prefix="backfill")
    backfilled = set()

    # Whatever the last run left in the catalogue is good enough to start
    # collecting on; the first sync takes a minute or two.
    retrack(collectors, pool, backfilled)
    for collector in collectors.values():
        collector.start()

    def prune_expired():
        if not (prune and settings.MARKET_RETENTION_DAYS > 0):
            return
        try:
            dropped = store.prune(settings.MARKET_RETENTION_DAYS)
            if dropped:
                log.info("pruned %s", ", ".join(f"{n:,} {k}" for k, n in dropped.items()))
        except Exception as exc:
            log.warning("prune failed: %s: %s", type(exc).__name__, exc)

    window = settings.MARKET_RETENTION_DAYS or 30
    closed_since = {v: time.time() - window * 86400 for v in collectors}
    series_at = 0.0
    first = True
    while not stop.is_set():
        # 1. What is open right now.
        for venue in collectors:
            started = time.time()
            try:
                if venue == "kalshi" and started - series_at > 86400:
                    catalogue.sync_series()
                    series_at = started
                n = catalogue.SYNC[venue](stop)
                log.info("%s: open markets synced, %d seen in %.0fs",
                         venue, n, time.time() - started)
            except Exception as exc:
                log.warning("%s: sync failed: %s: %s", venue, type(exc).__name__, exc)
        if stop.is_set():
            break
        # 2. Drop what has aged out, and start collecting anything new.
        prune_expired()
        try:
            retrack(collectors, pool, backfilled)
        except Exception as exc:
            log.warning("retrack failed: %s: %s", type(exc).__name__, exc)

        # 3. Markets that closed since the last look. On start that is the
        # whole retention window, which also covers any time spent stopped;
        # after that, Kalshi is re-read since the previous pass. (Polymarket
        # closes are picked up by the open sync re-reading what left its list.)
        for venue in collectors:
            if venue == "poly" and not first:
                continue
            started = time.time()
            try:
                n = catalogue.IMPORT_CLOSED[venue](stop, closed_since[venue])
                closed_since[venue] = started - 3600
                log.info("%s: closed markets imported, %d seen in %.0fs",
                         venue, n, time.time() - started)
            except Exception as exc:
                log.warning("%s: closed import failed: %s: %s", venue,
                            type(exc).__name__, exc)
        if first:
            prune_expired()
        first = False

        log.info("candles written so far: %s", ", ".join(
            f"{v} {c.written:,}" for v, c in collectors.items()))
        stop.wait(settings.MARKET_SYNC_MINUTES * 60)

    pool.shutdown(wait=False, cancel_futures=True)
    for collector in collectors.values():
        collector.join(timeout=15)


def dry_run(stop, n, seconds):
    collectors = {v: Collector(v, stop, dry=True) for v in settings.VENUES}
    for venue, collector in collectors.items():
        collector.track(dry_markets(venue, n))
        print(f"{venue}: watching {len(collector.markets)} markets", flush=True)
        collector.start()
    stop.wait(seconds)
    stop.set()
    for collector in collectors.values():
        collector.join(timeout=15)
        print(f"{collector.venue}: {collector.written} candles", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="print candles for a few markets; touches no database")
    parser.add_argument("--markets", type=int, default=10, help="with --dry-run")
    parser.add_argument("--seconds", type=float, default=60, help="with --dry-run")
    parser.add_argument("--no-prune", action="store_true",
                        help="keep expired markets instead of deleting them")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    if args.dry_run:
        dry_run(stop, args.markets, args.seconds)
    else:
        serve(stop, prune=not args.no_prune)


if __name__ == "__main__":
    main()

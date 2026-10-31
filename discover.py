"""Market discovery: keep the markets store current, pass after pass.

    python discover.py              # run forever (the discovery service)
    python discover.py --once       # one pass, then exit
    python discover.py --no-prune   # never delete expired markets

Every MARKET_SYNC_MINUTES, for each venue:

  1. open      everything open now is added or updated, and stored markets
               that left the open list are re-read for their final state
  2. closed    markets that closed since the last pass are imported -- on the
               very first pass, everything closed inside the retention window
  3. prune     markets closed longer ago than MARKET_RETENTION_DAYS are
               deleted, with their candles and any event left empty

Progress is saved to data/state/discovery.json as it goes, so a restart picks
up where the last run stopped:

  - a restart soon after a pass waits for the next scheduled one instead of
    re-reading everything immediately
  - the open sync and the closed-market import both resume from the last
    page they finished, inside the same scan; the closed import covers
    exactly the time the service was down -- no gap, no full re-read
  - while Kalshi cannot be reached, requests wait and resume the moment it
    can (see venues.Link), so an outage pauses a pass instead of ending it
  - Kalshi's series list is refreshed once a day, not on every start

Each pass is also a Kalshi scan: a row in kalshi_scans, and every market's
volume as read, in kalshi_observations. activity.py scores it once it is done.
Discovery never waits for that.
"""

import argparse
import json
import logging
import os
import signal
import threading
import time

import catalogue
import db
import paths
import ratelimit
import settings
import store

log = logging.getLogger("discovery")
STATE_FILE = paths.state("discovery.json")
OVERLAP = 3600          # each closed import re-reads the hour before the last one
ABANDON_AFTER = 2 * 3600    # an open sync this old is started again, not resumed


def load_state():
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except (FileNotFoundError, ValueError):
        return {}


def save_state(state):
    """Write atomically, so a crash mid-write never leaves a broken file."""
    tmp = STATE_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1), encoding="utf-8")
    os.replace(tmp, STATE_FILE)


def _window_start(now):
    return now - (settings.MARKET_RETENTION_DAYS or 30) * 86400


def _closed_import(venue, state, stop, scan=None):
    """Import markets closed since the last completed import, resuming an
    interrupted one where it stopped."""
    v = state.setdefault(venue, {})
    job = v.get("closed_import")
    if not job:
        now = time.time()
        since = max(v.get("closed_synced_at", 0) - OVERLAP, _window_start(now))
        job = v["closed_import"] = {"since": since, "started": now}
        save_state(state)
    else:
        log.info("%s: resuming closed import from %s", venue,
                 job.get("status") or "the last saved page")

    started = time.time()
    try:
        n = catalogue.IMPORT_CLOSED[venue](stop, job["since"], progress=job,
                                           checkpoint=lambda: save_state(state),
                                           scan=scan if venue == "kalshi" else None)
    except Exception:
        # A saved cursor the venue no longer accepts would fail every time;
        # start that import over on the next pass instead.
        job.pop("cursor", None)
        job.pop("status", None)
        save_state(state)
        raise
    if stop.is_set():
        return
    v["closed_synced_at"] = job["started"]
    v.pop("closed_import", None)
    save_state(state)
    log.info("%s: closed markets imported, %s rows written, %.0fs",
             venue, f"{n:,}", time.time() - started)


def _scan_started():
    """A new kalshi_scans row, or None if the database is not answering --
    the pass still runs, it just goes unscored."""
    try:
        return db.query("INSERT INTO kalshi_scans DEFAULT VALUES RETURNING scan_id")[0]["scan_id"]
    except Exception as exc:
        log.warning("could not open a scan: %s: %s", type(exc).__name__, exc)
        return None


def _scan_finished(scan):
    try:
        db.execute("UPDATE kalshi_scans SET finished_at = now() WHERE scan_id = %s", (scan,))
    except Exception as exc:
        # activity.py still scores it, once the next scan has started.
        log.warning("could not close scan %s: %s: %s", scan, type(exc).__name__, exc)


def _open_job(venue, state):
    """The open sync in progress for this venue -- resumed if one was cut
    short, with the scan it belongs to -- or a new one."""
    v = state.setdefault(venue, {})
    job = v.get("open_sync")
    if job and time.time() - job["started"] > ABANDON_AFTER:
        log.info("%s: giving up on the open sync from %.0f min ago; starting afresh",
                 venue, (time.time() - job["started"]) / 60)
        job = None
    if job:
        log.info("%s: resuming the open sync of scan %s from %s", venue, job.get("scan"),
                 "the last saved page" if job.get("cursor") else "the start")
    else:
        job = {"started": time.time(),
               "scan": _scan_started() if venue == "kalshi" else None}
    v["open_sync"] = job
    save_state(state)
    return job


def run_pass(stop, state, prune=True):
    now = time.time()
    scan = None
    if "kalshi" in settings.VENUES and now - state.get("series_synced_at", 0) > 86400:
        try:
            n = catalogue.sync_series()
            state["series_synced_at"] = now
            save_state(state)
            log.info("kalshi: series list synced, %s series", f"{n:,}")
        except Exception as exc:
            log.warning("kalshi: series sync failed: %s: %s", type(exc).__name__, exc)

    for venue in settings.VENUES:
        if stop.is_set():
            return
        started = time.time()
        job = _open_job(venue, state)
        if venue == "kalshi":
            scan = job["scan"]
        try:
            if venue == "kalshi":
                n = catalogue.SYNC[venue](stop, scan=scan, progress=job,
                                          checkpoint=lambda: save_state(state))
            else:
                n = catalogue.SYNC[venue](stop)
            if stop.is_set():
                return
            v = state[venue]
            v["open_synced_at"] = job["started"]
            v.pop("open_sync", None)
            save_state(state)
            log.info("%s: open markets synced, %s rows written, %.0fs",
                     venue, f"{n:,}", time.time() - started)
        except Exception as exc:
            # The job stays saved: the next pass carries on from its last page.
            log.warning("%s: open sync failed: %s: %s", venue, type(exc).__name__, exc)
        if stop.is_set():
            return
        try:
            _closed_import(venue, state, stop, scan)
        except Exception as exc:
            log.warning("%s: closed import failed: %s: %s", venue, type(exc).__name__, exc)

    # A scan whose open sync has not finished stays open, so the next pass
    # can complete it instead of starting another.
    if scan is not None and not stop.is_set() and "open_sync" not in state.get("kalshi", {}):
        _scan_finished(scan)

    if prune and settings.MARKET_RETENTION_DAYS > 0 and not stop.is_set():
        try:
            dropped = store.prune(settings.MARKET_RETENTION_DAYS)
            log.info("pruned %s", ", ".join(f"{n:,} {k}" for k, n in dropped.items())
                     if dropped else "nothing")
        except Exception as exc:
            log.warning("prune failed: %s: %s", type(exc).__name__, exc)


def run(stop, prune=True, after_pass=None):
    """Pass after pass until `stop` is set. `after_pass` runs after each one."""
    state = load_state()
    interval = settings.MARKET_SYNC_MINUTES * 60
    pending = any(state.get(v, {}).get("closed_import") or state.get(v, {}).get("open_sync")
                  for v in settings.VENUES)
    last = min((state.get(v, {}).get("open_synced_at", 0) for v in settings.VENUES), default=0)
    due = time.time() if (pending or time.time() - last >= interval) else last + interval
    if due > time.time():
        log.info("last pass finished %.0f min ago; next one in %.0f min",
                 (time.time() - last) / 60, (due - time.time()) / 60)

    while not stop.wait(max(0.0, due - time.time())):
        started = time.time()
        log.info("pass starting")
        run_pass(stop, state, prune)
        if stop.is_set():
            break
        if after_pass:
            try:
                after_pass()
            except Exception as exc:
                log.warning("after-pass step failed: %s: %s", type(exc).__name__, exc)
        took = time.time() - started
        log.info("pass finished in %.0fs", took)
        # Keep to the schedule; a pass that overran starts the next at once.
        due = max(started + interval, time.time())
    log.info("stopped")


def main():
    parser = argparse.ArgumentParser(description="Keep the markets store current.")
    parser.add_argument("--once", action="store_true", help="one pass, then exit")
    parser.add_argument("--no-prune", action="store_true",
                        help="keep expired markets instead of deleting them")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(name)s %(message)s", datefmt="%H:%M:%S")
    ratelimit.DEFAULT = "discover"
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())

    store.ensure_schema()
    if args.once:
        run_pass(stop, load_state(), prune=not args.no_prune)
    else:
        run(stop, prune=not args.no_prune)


if __name__ == "__main__":
    main()

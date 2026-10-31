"""Market discovery: keep the markets store current, pass after pass.

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
  - the closed-market import resumes from the last page it finished, and
    covers exactly the time the service was down -- no gap, no full re-read
  - Kalshi's series list is refreshed once a day, not on every start
"""

import json
import logging
import os
import time

import catalogue
import paths
import settings
import store

log = logging.getLogger("discovery")
STATE_FILE = paths.state("discovery.json")
OVERLAP = 3600          # each closed import re-reads the hour before the last one


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


def _closed_import(venue, state, stop):
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
                                           checkpoint=lambda: save_state(state))
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


def run_pass(stop, state, prune=True):
    now = time.time()
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
        try:
            n = catalogue.SYNC[venue](stop)
            if stop.is_set():
                return
            state.setdefault(venue, {})["open_synced_at"] = started
            save_state(state)
            log.info("%s: open markets synced, %s rows written, %.0fs",
                     venue, f"{n:,}", time.time() - started)
        except Exception as exc:
            log.warning("%s: open sync failed: %s: %s", venue, type(exc).__name__, exc)
        if stop.is_set():
            return
        try:
            _closed_import(venue, state, stop)
        except Exception as exc:
            log.warning("%s: closed import failed: %s: %s", venue, type(exc).__name__, exc)

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
    pending = any(state.get(v, {}).get("closed_import") for v in settings.VENUES)
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

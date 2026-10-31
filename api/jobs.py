"""Backfills asked for through the API (POST /v1/backfill), run inside it.

A job re-downloads one market's candles over a stretch of time -- by default
its whole life inside the store window -- and overwrites what is stored, with
the same fetcher and bookkeeping as live.py and backfill.py. Its requests use
the shared Kalshi budget at "warm" priority: after hot markets, before
backfill.py.

Jobs live in api_jobs, so they outlast the API: one cut short by a restart is
queued again and run from the start (fetching is idempotent).
"""

import logging
import threading
import time

import psycopg2

import db
import fetch
import ratelimit
import settings
from api.common import ApiError, iso

log = logging.getLogger("api.jobs")
ATTEMPTS = 3                # per request, ten seconds apart
_wake = threading.Event()


def ceil_minute(ts):
    return -(-int(ts) // 60) * 60


def span(market, start, end, now):
    """[lo, hi) to fetch for this market: by default from its open (or
    BACKFILL_DAYS ago, if later) to now, or to just after its close."""
    opened = market["open_time"].timestamp() if market["open_time"] else None
    closed = market["close_time"].timestamp() if market["close_time"] else None
    lo = start if start is not None else max(opened or 0, now - settings.BACKFILL_DAYS * 86400)
    if opened:
        lo = max(lo, opened)
    hi = end if end is not None else now
    if closed:
        hi = min(hi, closed + 120)
    # Only whole minutes that have settled.
    return fetch.minute(lo), min(ceil_minute(hi), fetch.minute(now - settings.SETTLE_SECONDS))


def describe(job):
    return {"job_id": job["job_id"], "ticker": job["ticker"], "status": job["status"],
            "from": iso(job["from_ts"]), "to": iso(job["to_ts"]),
            "requested_at": iso(job["requested_at"]), "started_at": iso(job["started_at"]),
            "finished_at": iso(job["finished_at"]),
            "progress": {"requests_done": job["requests_done"],
                         "requests_total": job["requests_total"]},
            "candles": job["candles"], "error": job["error"]}


def enqueue(key_id, ticker, start, end):
    rows = db.query("SELECT open_time, close_time FROM kalshi_markets WHERE ticker = %s",
                    (ticker,))
    if not rows:
        raise ApiError(404, "not_found", f"no market {ticker!r} in the store (it holds markets"
                       f" open now or closed in the last {settings.MARKET_RETENTION_DAYS} days)")
    lo, hi = span(rows[0], start, end, time.time())
    if hi <= lo:
        raise ApiError(400, "invalid_parameter", "nothing to fetch: that stretch is outside"
                       " the market's life, or not settled yet")
    try:
        job = db.query("""
            INSERT INTO api_jobs (key_id, ticker, from_ts, to_ts)
            VALUES (%s, %s, to_timestamp(%s), to_timestamp(%s)) RETURNING *""",
            (key_id, ticker, lo, hi))[0]
    except psycopg2.errors.UniqueViolation:
        open_job = db.query("SELECT job_id FROM api_jobs WHERE ticker = %s"
                            " AND status IN ('queued', 'running')", (ticker,))
        raise ApiError(409, "already_queued", f"a backfill of {ticker} is already queued or"
                       f" running: job {open_job[0]['job_id'] if open_job else '?'}") from None
    _wake.set()
    return job


def get(job_id):
    rows = db.query("SELECT * FROM api_jobs WHERE job_id = %s", (job_id,))
    if not rows:
        raise ApiError(404, "not_found", f"no backfill job {job_id}")
    return rows[0]


class Worker:
    """A few threads that take queued jobs one at a time."""

    def __init__(self, threads):
        self.stop = threading.Event()
        self.threads = [threading.Thread(target=self._loop, daemon=True, name=f"job-{i}")
                        for i in range(threads)]

    def start(self):
        try:
            n = db.execute("UPDATE api_jobs SET status = 'queued', started_at = NULL,"
                           " requests_done = 0, candles = 0 WHERE status = 'running'")
            if n:
                log.info("re-queued %d backfill jobs cut short by a restart", n)
        except Exception as exc:
            log.warning("could not re-queue unfinished jobs: %s", exc)
        for t in self.threads:
            t.start()

    def shutdown(self):
        self.stop.set()
        _wake.set()
        for t in self.threads:
            t.join(timeout=15)

    def _claim(self):
        rows = db.query("""
            UPDATE api_jobs SET status = 'running', started_at = now()
            WHERE job_id = (SELECT job_id FROM api_jobs WHERE status = 'queued'
                            ORDER BY job_id LIMIT 1 FOR UPDATE SKIP LOCKED)
            RETURNING *""")
        return rows[0] if rows else None

    def _loop(self):
        with ratelimit.using("warm"):
            while not self.stop.is_set():
                try:
                    job = self._claim()
                except Exception as exc:
                    log.warning("could not read the job queue: %s", exc)
                    job = None
                if job is None:
                    _wake.wait(5)
                    _wake.clear()
                    continue
                try:
                    self._run(job)
                except Exception as exc:
                    log.exception("job %s failed", job["job_id"])
                    self._finish(job, "failed", f"{type(exc).__name__}: {exc}")

    def _finish(self, job, status, error=None):
        db.execute("UPDATE api_jobs SET status = %s, finished_at = now(), error = %s"
                   " WHERE job_id = %s", (status, error, job["job_id"]))

    def _run(self, job):
        ticker = job["ticker"]
        lo, hi = int(job["from_ts"].timestamp()), int(job["to_ts"].timestamp())
        requests = fetch.plan([(ticker, lo, hi)])
        if not requests:
            return self._finish(job, "failed", "Kalshi rejected this ticker recently;"
                                " try again in an hour")
        db.execute("UPDATE api_jobs SET requests_total = %s WHERE job_id = %s",
                   (len(requests), job["job_id"]))
        log.info("job %s: %s, %s requests", job["job_id"], ticker, len(requests))
        for request in requests:
            n = None
            for _ in range(ATTEMPTS):
                n = fetch.run(request, self.stop)
                if n is not None or self.stop.wait(10):
                    break
            if self.stop.is_set():
                return          # left running: re-queued when the API starts again
            if n is None:
                why = ("Kalshi rejects this ticker" if fetch._bad.get(ticker, 0) > time.time()
                       else f"Kalshi did not answer after {ATTEMPTS} tries")
                return self._finish(job, "failed", why)
            db.execute("UPDATE api_jobs SET requests_done = requests_done + 1,"
                       " candles = candles + %s WHERE job_id = %s", (n, job["job_id"]))
        self._finish(job, "done")
        log.info("job %s: %s done", job["job_id"], ticker)

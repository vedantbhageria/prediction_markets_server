"""Backfills asked for through the API (POST /v1/backfill), run inside it.

A job re-downloads one market's candles over a stretch of time -- by default
its whole life inside the store window -- and overwrites what is stored, with
the same fetcher and bookkeeping as live.py and backfill.py. Its requests use
the shared Kalshi budget at "api" priority -- the same as warm: after hot
markets, before backfill.py -- counted on a line of their own.

Jobs live in api_jobs, so they outlast the API. Each API process runs a few
worker threads; a worker takes up to JOB_BATCH queued jobs at once and plans
them together, so jobs pushed together share requests (100 markets each)
instead of one request per market. While it runs them it renews their
heartbeat; a running job whose heartbeat goes stale -- its process died -- is
taken over by any worker and run from the start (fetching is idempotent).

A Polymarket market (a condition id, 0x...) is backfilled by reading its whole
trade list again (poly.fetch_market): one request for all but the busiest,
on Polymarket's budget. Its `candles` count is the trades that were new;
aggregate.py makes the candles from them within seconds.
"""

import logging
import threading
import time

import psycopg2
from psycopg2.extras import execute_values

import db
import fetch
import poly
import ratelimit
import settings
from api import config
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


def is_poly(ticker):
    return ticker.startswith("0x")


def enqueue(key_id, ticker, start, end):
    if is_poly(ticker):
        rows = db.query("""SELECT t.history_from AS open_time,
                                  CASE WHEN m.closed THEN m.end_date END AS close_time
                           FROM poly_markets m JOIN poly_tracking t USING (condition_id)
                           WHERE m.condition_id = %s""", (ticker,))
    else:
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
    """A few threads that take queued jobs in batches, and one that keeps the
    heartbeat of the jobs this process is running."""

    def __init__(self, threads):
        self.stop = threading.Event()
        self.mine = set()               # job ids running in this process
        self.mine_lock = threading.Lock()
        self.threads = [threading.Thread(target=self._loop, daemon=True, name=f"job-{i}")
                        for i in range(threads)]
        self.threads.append(threading.Thread(target=self._heartbeat, daemon=True,
                                             name="job-heartbeat"))

    def start(self):
        for t in self.threads:
            t.start()

    def shutdown(self):
        self.stop.set()
        _wake.set()
        for t in self.threads:
            t.join(timeout=15)

    def _heartbeat(self):
        while not self.stop.wait(config.JOB_LEASE / 4):
            with self.mine_lock:
                ids = list(self.mine)
            if ids:
                try:
                    db.execute("UPDATE api_jobs SET heartbeat_at = now()"
                               " WHERE job_id = ANY(%s) AND status = 'running'", (ids,))
                except Exception as exc:
                    log.warning("could not renew job heartbeats: %s", exc)

    def _claim(self):
        """Queued jobs, and running ones whose worker went quiet, oldest first."""
        return db.query("""
            WITH picked AS (
                SELECT job_id FROM api_jobs
                WHERE status = 'queued'
                   OR (status = 'running' AND COALESCE(heartbeat_at, started_at, requested_at)
                                              < now() - make_interval(secs => %s))
                ORDER BY job_id LIMIT %s FOR UPDATE SKIP LOCKED)
            UPDATE api_jobs j SET status = 'running', started_at = now(), heartbeat_at = now(),
                   requests_done = 0, candles = 0, requests_total = NULL
            FROM picked WHERE j.job_id = picked.job_id
            RETURNING j.*""", (config.JOB_LEASE, config.JOB_BATCH))

    def _loop(self):
        with ratelimit.using("api"):
            while not self.stop.is_set():
                try:
                    jobs = self._claim()
                except Exception as exc:
                    log.warning("could not read the job queue: %s", exc)
                    jobs = []
                if not jobs:
                    _wake.wait(5)
                    _wake.clear()
                    continue
                ids = {j["job_id"] for j in jobs}
                with self.mine_lock:
                    self.mine |= ids
                try:
                    self._run(jobs)
                except Exception as exc:
                    log.exception("jobs %s failed", sorted(ids))
                    for j in jobs:
                        self._finish(j, "failed", f"{type(exc).__name__}: {exc}",
                                     only_running=True)
                finally:
                    with self.mine_lock:
                        self.mine -= ids

    def _finish(self, job, status, error=None, candles=None, only_running=False):
        db.execute("UPDATE api_jobs SET status = %s, finished_at = now(), error = %s,"
                   " candles = COALESCE(%s, candles) WHERE job_id = %s"
                   + (" AND status = 'running'" if only_running else ""),
                   (status, error, candles, job["job_id"]))

    def _run_poly(self, jobs):
        """Each market's trade list read again, back to the job's start."""
        for job in jobs:
            db.execute("UPDATE api_jobs SET requests_total = 1 WHERE job_id = %s", (job["job_id"],))
            got = None
            for _ in range(ATTEMPTS):
                got = poly.fetch_market(job["ticker"], self.stop, job["from_ts"].timestamp() - 60)
                if got is not None or self.stop.wait(10):
                    break
            if self.stop.is_set():
                return
            if got is None:
                self._finish(job, "failed", f"Polymarket did not answer after {ATTEMPTS} tries")
                continue
            if not got[1]:
                poly.fill_mid(job["ticker"], self.stop)
            db.execute("UPDATE api_jobs SET requests_done = 1 WHERE job_id = %s", (job["job_id"],))
            self._finish(job, "done", None, got[0])

    def _run(self, jobs):
        self._run_poly([j for j in jobs if is_poly(j["ticker"])])
        jobs = [j for j in jobs if not is_poly(j["ticker"])]
        if not jobs:
            return
        by_ticker = {j["ticker"]: j for j in jobs}      # one open job per market
        requests = fetch.plan([(t, int(j["from_ts"].timestamp()), int(j["to_ts"].timestamp()))
                               for t, j in by_ticker.items()])
        total = {t: 0 for t in by_ticker}
        for tickers, _, _ in requests:
            for t in tickers:
                total[t] += 1
        for t, n in total.items():
            if not n:
                self._finish(by_ticker[t], "failed", "Kalshi rejected this ticker recently;"
                             " try again in an hour")
        with db.cursor(dict_rows=False) as cur:
            execute_values(cur, "UPDATE api_jobs j SET requests_total = v.n"
                                " FROM (VALUES %s) AS v(job_id, n) WHERE j.job_id = v.job_id",
                           [(by_ticker[t]["job_id"], n) for t, n in total.items() if n])
        log.info("jobs: %d markets in %d requests", sum(1 for n in total.values() if n),
                 len(requests))

        candles = {t: 0 for t in by_ticker}
        failed = {}
        for request in requests:
            got = {}
            n = None
            for _ in range(ATTEMPTS):
                n = fetch.run(request, self.stop, got)
                if n is not None or self.stop.wait(10):
                    break
            if self.stop.is_set():
                return          # left running: another worker takes them over
            for t in request[0]:
                if t in got:
                    candles[t] += got[t]
                else:
                    failed.setdefault(t, "Kalshi rejects this ticker"
                                      if fetch._bad.get(t, 0) > time.time()
                                      else f"Kalshi did not answer after {ATTEMPTS} tries")
            with db.cursor(dict_rows=False) as cur:
                execute_values(cur, "UPDATE api_jobs j SET requests_done = requests_done + 1,"
                                    " candles = v.c FROM (VALUES %s) AS v(job_id, c)"
                                    " WHERE j.job_id = v.job_id",
                               [(by_ticker[t]["job_id"], candles[t]) for t in request[0]])
        for t, job in by_ticker.items():
            if not total[t]:
                continue
            if t in failed:
                self._finish(job, "failed", failed[t], candles[t])
            else:
                self._finish(job, "done", None, candles[t])
        log.info("jobs: %d done, %d failed", len(by_ticker) - len(failed), len(failed))

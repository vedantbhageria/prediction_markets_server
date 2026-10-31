"""Every API request, written to api_requests for the viewer's /api-usage page.

A request only adds a line to an in-memory queue; one thread writes the queue
to the database every few seconds. So recording costs a request nothing, and
if the database is away the lines wait in memory (up to MAX_QUEUED, oldest
dropped first) and are written once it is back.
"""

import json
import logging
import threading
from collections import deque
from datetime import datetime, timezone

from psycopg2.extras import execute_values

import db

log = logging.getLogger("api.usage")
FLUSH_SECONDS = 3
MAX_QUEUED = 50_000
BATCH = 5_000
# Paths not worth recording: health checks and the docs pages.
SKIP = ("/health", "/docs", "/redoc", "/openapi.json", "/favicon.ico")

_queue = deque(maxlen=MAX_QUEUED)
COLUMNS = ("at", "key_id", "method", "path", "endpoint", "params", "tickers", "status",
           "error", "ms", "row_count", "bytes", "client_ip", "user_agent")


def _int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def record(request, response, ms):
    """Queue one finished request. Never raises."""
    try:
        path = request.url.path
        if path.startswith(SKIP):
            return
        state = request.state
        key = getattr(state, "key", None)
        route = request.scope.get("route")
        query = {k: v[:200] for k, v in request.query_params.items()}
        tickers = list(getattr(state, "tickers", None) or [])
        ticker = request.scope.get("path_params", {}).get("ticker")
        if ticker:
            tickers.append(ticker)
        if query.get("tickers"):
            tickers += [t.strip() for t in query["tickers"].split(",") if t.strip()]
        forwarded = request.headers.get("x-forwarded-for", "")
        ip = forwarded.split(",")[0].strip() or (request.client.host if request.client else None)
        _queue.append((
            datetime.now(timezone.utc), key["key_id"] if key else None, request.method, path[:300],
            getattr(route, "path", None), json.dumps(query) if query else None,
            list(dict.fromkeys(tickers))[:100] or None, response.status_code,
            response.headers.get("x-error-code"), round(ms, 2),
            _int(response.headers.get("x-row-count")), _int(response.headers.get("content-length")),
            ip, (request.headers.get("user-agent") or "")[:200] or None))
    except Exception as exc:        # recording must never break a request
        log.warning("could not record a request: %s: %s", type(exc).__name__, exc)


def _write(rows):
    with db.connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, f"INSERT INTO api_requests ({', '.join(COLUMNS)}) VALUES %s",
                           rows, template="(" + ", ".join(["%s"] * len(COLUMNS)) + ")",
                           page_size=1000)


class Writer:
    def __init__(self):
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self._loop, daemon=True, name="usage")
        self.failing = False

    def start(self):
        self.thread.start()

    def shutdown(self):
        self.stop.set()
        self.thread.join(timeout=10)
        self.flush()

    def flush(self):
        while _queue:
            rows = []
            while _queue and len(rows) < BATCH:
                rows.append(_queue.popleft())
            try:
                _write(rows)
            except Exception as exc:
                # Put them back, oldest first, and try again later.
                _queue.extendleft(reversed(rows))
                if not self.failing:
                    log.warning("could not write %d request records (will retry): %s: %s",
                                len(rows), type(exc).__name__, exc)
                self.failing = True
                return
            if self.failing:
                log.info("request records written again")
                self.failing = False

    def _loop(self):
        while not self.stop.wait(FLUSH_SECONDS):
            self.flush()

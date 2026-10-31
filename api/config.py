"""API settings, read from the environment / the project's .env.

The database and Redis settings are the ones every other program uses, so the
API always reads what the collectors write.
"""

import os

import settings


def _num(name, default):
    raw = os.environ.get(name, "").strip()
    return int(raw) if raw else default


API_HOST = os.environ.get("API_HOST", "127.0.0.1")
API_PORT = _num("API_PORT", 8000)

# Requests per key per minute, unless the key has its own limit; and how many
# backfills a key may ask for per minute.
RATE_PER_MIN = _num("API_RATE_PER_MIN", 600)
BACKFILL_PER_MIN = _num("API_BACKFILL_PER_MIN", 10)
# Requests with a missing or wrong key, from everyone together, per minute.
BAD_KEYS_PER_MIN = _num("API_BAD_KEYS_PER_MIN", 300)

MAX_ROWS = 10_000               # rows per page, any endpoint
DEFAULT_ROWS = 100              # ...when `limit` is not given (candles: MAX_ROWS)
STATEMENT_TIMEOUT = _num("API_STATEMENT_TIMEOUT_SECONDS", 30)
DB_POOL = _num("API_DB_POOL", 12)
JOB_WORKERS = _num("API_JOB_WORKERS", 2)

# Live collection runs this far behind the clock (a minute to settle, a cycle
# to fetch it), so "complete" coverage is judged up to here.
LIVE_LAG = settings.SETTLE_SECONDS + 120

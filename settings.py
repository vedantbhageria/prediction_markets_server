"""Every tunable, read from the environment / .env."""

import os

from db import load_env

load_env()


def _num(name, default, cast=int):
    raw = os.environ.get(name, "").strip()
    return cast(raw) if raw else default


def _list(name):
    return [part.strip() for part in os.environ.get(name, "").split(",") if part.strip()]


# --- venues -----------------------------------------------------------------
VENUES = _list("LIVE_VENUES") or ["kalshi", "poly"]
KALSHI_API_URL = (os.environ.get("KALSHI_API_URL", "").strip()
                  or "https://external-api.kalshi.com/trade-api/v2")
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
REDIS_PREFIX = os.environ.get("REDIS_PREFIX", "pm:")   # tests use their own
# While a venue cannot be reached, every request to it waits and one check
# tries to connect this often; the moment it works they all carry on.
NET_PROBE_SECONDS = _num("NET_PROBE_SECONDS", 30, float)

# --- markets store ----------------------------------------------------------
# The store holds markets that are still running or closed within this many
# days. Older ones are deleted with their candles on start and every sync, and
# on start the window is filled with markets that closed while the collector
# was not running.
MARKET_RETENTION_DAYS = _num("MARKET_RETENTION_DAYS", 30)
MARKET_SYNC_MINUTES = _num("MARKET_SYNC_MINUTES", 15, float)
MARKET_CATEGORIES = _list("MARKET_CATEGORIES")       # Kalshi only; empty = all

# --- request budget ---------------------------------------------------------
# Every Kalshi request from every program (discover, live, backfill) comes out
# of this one budget, shared through Redis. Kalshi publishes 20/s for signed-in
# Basic accounts and nothing for anonymous ones, so the default stays far below.
KALSHI_REQUESTS_PER_MIN = _num("KALSHI_REQUESTS_PER_MIN", 240)
# ...and never more than this in any one second, whatever the budget: Kalshi
# counts per second (20/s for signed-in Basic accounts).
KALSHI_MAX_PER_SECOND = _num("KALSHI_MAX_PER_SECOND", 15, float)

# --- activity score ----------------------------------------------------------
HALF_LIFE_MINUTES = _num("ACTIVITY_HALF_LIFE_MINUTES", 60, float)
HOT_ENTER = _num("HOT_ENTER", 1.0, float)      # contracts/min to become hot
HOT_EXIT = _num("HOT_EXIT", 0.5, float)        # ...and to stop being hot
# Part of the budget never planned for, so a busy minute cannot hit the limit.
BUDGET_HEADROOM = _num("BUDGET_HEADROOM", 0.10, float)

# --- live candles ------------------------------------------------------------
# A scan's read time is recorded this much earlier than the request, in case
# the venue's listing lags its trades a little. Costs nothing but a short delay.
OBSERVATION_LAG = _num("OBSERVATION_LAG_SECONDS", 120)
# A minute is fetched once it ended this long ago, and hot markets re-read the
# last HOT_OVERLAP seconds every time, so trades the venue records late land.
SETTLE_SECONDS = _num("LIVE_SETTLE_SECONDS", 60)
HOT_OVERLAP = _num("LIVE_HOT_OVERLAP_SECONDS", 120)
# A market first seen this soon after it opened has its few minutes of history
# fetched by live; older ones leave their history to backfill.py.
NEW_MARKET_HOURS = _num("NEW_MARKET_HOURS", 2, float)
FETCH_WORKERS = _num("LIVE_FETCH_WORKERS", 4)

# --- backfill ----------------------------------------------------------------
BACKFILL_DAYS = _num("BACKFILL_DAYS", 30)

"""Every tunable for the live collector, read from the environment / .env.

Volume thresholds follow what the batch scrapers already assumed: Kalshi
markets over 100 contracts, Polymarket markets over $10,000.
"""

import os

from db import load_env

load_env()


def _num(name, default, cast=int):
    raw = os.environ.get(name, "").strip()
    return cast(raw) if raw else default


def _list(name):
    return [part.strip() for part in os.environ.get(name, "").split(",") if part.strip()]


# --- candles ---------------------------------------------------------------
# Width of a stored candle. One minute: the same as both venues' own history,
# so live and backfilled rows are interchangeable.
CANDLE_SECONDS = _num("LIVE_CANDLE_SECONDS", 60)
# How often each venue is sampled. Several samples per candle are what give it
# a real high and low; one per candle would make open = high = low = close.
POLL_SECONDS = _num("LIVE_POLL_SECONDS", 5, float)

# --- which markets are collected live --------------------------------------
VENUES = _list("LIVE_VENUES") or ["kalshi", "poly"]
MAX_MARKETS = _num("LIVE_MAX_MARKETS", 500)          # per venue, busiest first
MIN_VOLUME = {"kalshi": _num("LIVE_MIN_VOLUME_KALSHI", 100, float),
              "poly": _num("LIVE_MIN_VOLUME_POLY", 10000, float)}
CATEGORIES = _list("LIVE_CATEGORIES")                # empty = every category

# --- markets store ----------------------------------------------------------
# The store holds markets that are still running or closed within this many
# days. Older ones are deleted with their candles on start and every sync, and
# on start the window is filled with markets that closed while the collector
# was not running.
MARKET_RETENTION_DAYS = _num("MARKET_RETENTION_DAYS", 30)
MARKET_SYNC_MINUTES = _num("MARKET_SYNC_MINUTES", 15, float)
MARKET_CATEGORIES = _list("MARKET_CATEGORIES")       # Kalshi only; empty = all

# --- backfill ---------------------------------------------------------------
BACKFILL_DAYS = _num("BACKFILL_DAYS", 30)
BACKFILL_WORKERS = _num("BACKFILL_WORKERS", 4)

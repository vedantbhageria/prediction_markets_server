"""Every file location in one place.

    data/
      perp_markets.jsonl   Kalshi's perpetual instruments (kalshi_perps.py)
      perps.parquet        their candles
      state/               discovery's resume state, monitor.py's minute files

Everything else lives in PostgreSQL.
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
STATE = Path(os.environ.get("PM_STATE_DIR") or DATA / "state")   # tests use their own

PERP_MARKETS = DATA / "perp_markets.jsonl"
PERP_CANDLES = DATA / "perps.parquet"

SCHEMA = ROOT / "schema.sql"


def ensure():
    """Create the directory tree if it isn't there yet."""
    for path in (DATA, STATE):
        path.mkdir(parents=True, exist_ok=True)


def state(name):
    """Path for a resume file, e.g. state('kalshi_candles.done')."""
    STATE.mkdir(parents=True, exist_ok=True)
    return STATE / name

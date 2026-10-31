"""Every file location in one place.

    data/
      markets.jsonl        Kalshi markets, combo markets removed  (the catalogue)
      markets_all.jsonl    raw listing, including KXMVE* combos   (archive)
      series.jsonl         Kalshi series and their categories
      perp_markets.jsonl   the 16 perpetual instruments
      poly_markets.jsonl   Polymarket markets
      perps.parquet        perp candles
      candles/kalshi/      prediction candles, part-*.parquet
      candles/poly/        Polymarket candles, part-*.parquet
      state/               resume bookkeeping: .done, .cursor, .loaded
"""

import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
STATE = Path(os.environ.get("PM_STATE_DIR") or DATA / "state")   # tests use their own
CANDLES = DATA / "candles"

# Catalogues
MARKETS = DATA / "markets.jsonl"
MARKETS_ALL = DATA / "markets_all.jsonl"
SERIES = DATA / "series.jsonl"
PERP_MARKETS = DATA / "perp_markets.jsonl"
POLY_MARKETS = DATA / "poly_markets.jsonl"

# Price history
KALSHI_CANDLES = CANDLES / "kalshi"
POLY_CANDLES = CANDLES / "poly"
PERP_CANDLES = DATA / "perps.parquet"

SCHEMA = ROOT / "schema.sql"


def ensure():
    """Create the directory tree if it isn't there yet."""
    for path in (DATA, STATE, CANDLES, KALSHI_CANDLES, POLY_CANDLES):
        path.mkdir(parents=True, exist_ok=True)


def state(name):
    """Path for a resume file, e.g. state('kalshi_candles.done')."""
    STATE.mkdir(parents=True, exist_ok=True)
    return STATE / name

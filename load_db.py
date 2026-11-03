"""Load Kalshi's perpetual futures into PostgreSQL.

    python kalshi_perps.py --days 7     # download them first
    python load_db.py                   # then load what was downloaded

Everything else is collected straight into the database by the running
programs (discover.py, live.py, trades.py, backfill.py); perpetuals are not,
so the viewer's "refresh perpetuals" button runs these two. Every insert is
ON CONFLICT DO NOTHING (markets are updated), so re-running is safe.

`num` and `insert` are also used by kalshi_perps.py, catalogue.py and the tests.
"""

import json

import pyarrow.parquet as pq
from psycopg2.extras import Json, execute_values

import paths
from db import connection


def rows_from_jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def num(value):
    """Kalshi sends numbers as strings; missing ones as None or ''."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def insert(cur, table, columns, rows, key=None):
    """Bulk insert.

    Candles are immutable, so a clash means we already have the row -- skip it.
    Dimension rows change (status, result, volume, and new columns like asset),
    so pass `key` to refresh them instead.
    """
    if not rows:
        return 0
    if key:
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != key)
        conflict = f"ON CONFLICT ({key}) DO UPDATE SET {sets}"
    else:
        conflict = "ON CONFLICT DO NOTHING"
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s {conflict}"
    execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def load_perp_markets(cur, path):
    rows = [(m["ticker"], num(m.get("contract_size")),
             num(m.get("tick_size")), m.get("status"), Json(m))
            for m in rows_from_jsonl(path)]
    columns = ["ticker", "contract_size", "tick_size", "status", "raw"]
    return insert(cur, "kalshi_perp_markets", columns, rows, key="ticker")


def load_perp_candles(cur, path):
    columns = ["ticker", "ts", "open", "high", "low", "close", "volume", "open_interest"]
    rows = [(c["ticker"], c["ts"], c["open"], c["high"], c["low"], c["close"],
             c["volume"], c["open_interest"]) for c in pq.read_table(path).to_pylist()]
    return insert(cur, "kalshi_perp_candles", columns, rows)


def main():
    with connection() as conn:
        with conn.cursor() as cur:
            # Candles have a foreign key into the markets, so markets first.
            for path, loader in ((paths.PERP_MARKETS, load_perp_markets),
                                 (paths.PERP_CANDLES, load_perp_candles)):
                if not path.exists():
                    print(f"{path.name:<22} skipped (not found)")
                    continue
                print(f"{path.name:<22} {loader(cur, path)} rows offered")


if __name__ == "__main__":
    main()

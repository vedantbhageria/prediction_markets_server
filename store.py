"""Database side of collection: schema setup, upserts, candles in, old rows out.

Every candle is one minute, stamped with the start of that minute, and only
minutes with a trade are stored.
"""

from datetime import datetime, timedelta, timezone

from psycopg2.extras import execute_values

import db
import paths
from db import connection

# Per venue: where candles live, what they key on, and which table owns the key.
VENUES = {
    "kalshi": {"table": "kalshi_candles", "key": "ticker", "dim": "kalshi_markets",
               "cols": ["open", "high", "low", "close", "volume", "open_interest"]},
    "poly": {"table": "poly_candles", "key": "condition_id", "dim": "poly_markets",
             "cols": ["open", "high", "low", "close", "volume"]},
}


TIMESCALE_SQL = paths.ROOT / "timescale.sql"
# (rollup, end offset, how often it refreshes)
ROLLUPS = [("kalshi_candles_5m", "5 minutes", "5 minutes"),
           ("kalshi_candles_15m", "15 minutes", "15 minutes"),
           ("kalshi_candles_1h", "1 hour", "30 minutes"),
           ("poly_candles_5m", "5 minutes", "5 minutes"),
           ("poly_candles_15m", "15 minutes", "15 minutes"),
           ("poly_candles_1h", "1 hour", "30 minutes")]


def has_timescale():
    return bool(db.query("SELECT 1 FROM pg_extension WHERE extname = 'timescaledb'"))


def _statements(sql):
    """Split a script into statements, keeping $$-quoted blocks whole."""
    out, current, quoted = [], [], False
    for line in sql.splitlines():
        if line.strip().startswith("--") and not quoted:
            continue
        current.append(line)
        quoted ^= line.count("$$") % 2 == 1
        if not quoted and line.rstrip().endswith(";"):
            out.append("\n".join(current).strip())
            current = []
    if "".join(current).strip():
        out.append("\n".join(current).strip())
    return out


def ensure_schema():
    """Tables, then the TimescaleDB layer if the extension is installed.

    The Timescale part runs statement by statement outside a transaction,
    because continuous aggregates cannot be created inside one.
    """
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute(paths.SCHEMA.read_text(encoding="utf-8"))
    if not has_timescale():
        return False

    pool = db.get_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            for statement in _statements(TIMESCALE_SQL.read_text(encoding="utf-8")):
                cur.execute(statement)

            # Replace the refresh jobs so a changed schedule takes effect. No
            # start offset: a refresh only re-reads stretches whose raw candles
            # changed, so covering all of history costs nothing extra -- and it
            # means deleting a market's candles also removes its rollups.
            for view, end, every in ROLLUPS:
                cur.execute("SELECT remove_continuous_aggregate_policy(%s, if_exists => true)",
                            (view,))
                cur.execute("SELECT add_continuous_aggregate_policy(%s,"
                            " start_offset => NULL, end_offset => %s::interval,"
                            " schedule_interval => %s::interval)", (view, end, every))
    finally:
        conn.autocommit = False
        pool.putconn(conn)
    return True


def upsert(cur, table, columns, rows, key, watch):
    """Insert rows, refreshing existing ones only if a `watch` column differs.

    The catalogue is re-read every few minutes and most of it has not moved;
    rewriting 100k unchanged rows each time would be pure table bloat.
    """
    rows = list({row[columns.index(key)]: row for row in rows}.values())
    if not rows:
        return 0
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != key)
    guard = " OR ".join(f"t.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in watch)
    execute_values(cur, f"INSERT INTO {table} AS t ({', '.join(columns)}) VALUES %s"
                        f" ON CONFLICT ({key}) DO UPDATE SET {sets} WHERE {guard}",
                   rows, page_size=500)
    return len(rows)


def write_candles(venue, candles, cur=None):
    """Insert or refresh candles. Returns the number of rows offered.

    Each candle is (key, ts, open, high, low, close, volume, open_interest);
    open_interest is ignored for Polymarket. A minute that is fetched again
    replaces the stored row, so trades the venue recorded late are not lost.
    Rows are filtered against the market table in the same statement: a market
    deleted mid-flight would otherwise fail the foreign key and take the whole
    batch down with it.
    """
    if not candles:
        return 0
    v = VENUES[venue]
    values = len(v["cols"])
    names = [v["key"], "ts"] + v["cols"]
    select = ", ".join(["v.k", "v.ts::timestamptz"] +
                       [f"v.c{i}::numeric" for i in range(values)])
    alias = ", ".join(["k", "ts"] + [f"c{i}" for i in range(values)])
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in v["cols"])
    changed = " OR ".join(f"t.{c} IS DISTINCT FROM EXCLUDED.{c}" for c in v["cols"])
    sql = (f"INSERT INTO {v['table']} AS t ({', '.join(names)})"
           f" SELECT {select} FROM (VALUES %s) AS v({alias})"
           f" WHERE EXISTS (SELECT 1 FROM {v['dim']} d WHERE d.{v['key']} = v.k)"
           f" ON CONFLICT ({v['key']}, ts) DO UPDATE SET {sets} WHERE {changed}")
    rows = [tuple(c[:2 + values]) for c in candles]
    if cur is not None:
        execute_values(cur, sql, rows, page_size=1000)
        return len(rows)
    with connection() as conn:
        with conn.cursor() as c:
            execute_values(c, sql, rows, page_size=1000)
    return len(rows)


def write_kalshi_fetch(candles, fetched):
    """Kalshi candles plus the stretch of time they were fetched for, in one
    transaction, so the bookkeeping can never claim minutes that were not
    written.

    `fetched` is [(ticker, lo, hi)]: every trade in [lo, hi) is now stored. It
    is merged into the market's [candles_fetched_from, candles_fetched_to) only
    where it touches or overlaps it -- a stretch with a hole between it and
    what was already covered is stored but not counted, so the hole stays
    visible to whoever fills it.
    """
    with connection() as conn:
        with conn.cursor() as cur:
            write_candles("kalshi", candles, cur)
            if fetched:
                execute_values(cur, """
                    UPDATE kalshi_tracking t SET
                      candles_fetched_to = CASE WHEN f.lo <= t.candles_fetched_to
                          THEN GREATEST(t.candles_fetched_to, f.hi) ELSE t.candles_fetched_to END,
                      candles_fetched_from = CASE WHEN f.hi >= t.candles_fetched_from
                          THEN LEAST(t.candles_fetched_from, f.lo) ELSE t.candles_fetched_from END
                    FROM (VALUES %s) AS f(ticker, lo, hi)
                    WHERE t.ticker = f.ticker""",
                    list(fetched), template="(%s, %s::timestamptz, %s::timestamptz)",
                    page_size=1000)
    return len(candles)


def prune(days):
    """Drop everything that closed more than `days` ago, candles included.

    Polymarket's end_date is nominal -- plenty of markets trade on past it --
    so there the market also has to be marked closed.
    """
    if days <= 0:
        return {}
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    steps = [
        ("kalshi candles", "DELETE FROM kalshi_candles WHERE ticker IN"
                           " (SELECT ticker FROM kalshi_markets WHERE close_time < %s)", True),
        ("kalshi markets", "DELETE FROM kalshi_markets WHERE close_time < %s", True),
        ("kalshi events", "DELETE FROM kalshi_events e WHERE NOT EXISTS"
                          " (SELECT 1 FROM kalshi_markets m WHERE m.event_ticker = e.event_ticker)", False),
        ("poly candles", "DELETE FROM poly_candles WHERE condition_id IN"
                         " (SELECT condition_id FROM poly_markets"
                         "  WHERE closed IS TRUE AND end_date < %s)", True),
        ("poly markets", "DELETE FROM poly_markets WHERE closed IS TRUE AND end_date < %s", True),
        ("poly events", "DELETE FROM poly_events e WHERE NOT EXISTS"
                        " (SELECT 1 FROM poly_markets p WHERE p.event_id = e.event_id)", False),
        # Observations activity.py has scored, or too old to be worth scoring.
        ("kalshi observations", "DELETE FROM kalshi_observations o USING kalshi_scans s"
                                " WHERE s.scan_id = o.scan_id AND (s.scored_at IS NOT NULL"
                                "  OR s.started_at < now() - interval '1 day')", False),
        ("kalshi scans", "DELETE FROM kalshi_scans WHERE started_at < now() - interval '7 days'",
         False),
        ("api requests", "DELETE FROM api_requests WHERE at < now() - interval '30 days'", False),
    ]
    counts = {}
    with connection() as conn:
        with conn.cursor() as cur:
            for label, sql, dated in steps:
                cur.execute(sql, (cutoff,) if dated else None)
                if cur.rowcount:
                    counts[label] = cur.rowcount
    return counts

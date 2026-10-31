"""Database side of the live collector: candles in, old rows out.

Live candles go into the same kalshi_candles / poly_candles tables the
viewer already reads, so nothing downstream has to know they exist. Every
candle is one minute, stamped with the start of that minute.
"""

from datetime import datetime, timedelta, timezone

from psycopg2.extras import execute_values

import db
import paths
import settings
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


def write_candles(venue, candles):
    """Insert candles. Returns the number of rows offered.

    Each candle is (key, ts, open, high, low, close, volume, open_interest
    [, filled]); open_interest is ignored for Polymarket and filled defaults
    to false. Rows are filtered against the market table in the same
    statement: a market deleted from the manage page mid-flight would
    otherwise fail the foreign key and take the whole batch down with it.
    """
    if not candles:
        return 0
    v = VENUES[venue]
    names = [v["key"], "ts"] + v["cols"] + ["filled"]
    values = len(v["cols"])
    select = ", ".join(["v.k", "v.ts::timestamptz"] +
                       [f"v.c{i}::numeric" for i in range(values)] + ["v.filled::boolean"])
    alias = ", ".join(["k", "ts"] + [f"c{i}" for i in range(values)] + ["filled"])
    sql = (f"INSERT INTO {v['table']} ({', '.join(names)})"
           f" SELECT {select} FROM (VALUES %s) AS v({alias})"
           f" WHERE EXISTS (SELECT 1 FROM {v['dim']} d WHERE d.{v['key']} = v.k)"
           " ON CONFLICT DO NOTHING")
    rows = [tuple(c[:2 + values]) + (bool(c[8]) if len(c) > 8 else False,)
            for c in candles]
    with connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


def tracked(venue):
    """The markets to collect live: open, liquid enough, busiest first.

    Returns {key: token}. The token is what Polymarket prices are quoted by;
    for Kalshi it is just the ticker again.
    """
    cats = settings.CATEGORIES
    cat = " AND e.category = ANY(%s)" if cats else ""
    args = (settings.MIN_VOLUME[venue],) + ((cats,) if cats else ()) + (settings.MAX_MARKETS,)
    if venue == "kalshi":
        rows = db.query(f"""
            SELECT m.ticker AS k, m.ticker AS token
            FROM kalshi_markets m JOIN kalshi_events e USING (event_ticker)
            WHERE m.close_time > now() AND COALESCE(m.result, '') = ''
              AND COALESCE(m.volume, 0) >= %s{cat}
            ORDER BY (m.raw->>'volume_24h_fp')::numeric DESC NULLS LAST,
                     m.volume DESC NULLS LAST, m.ticker
            LIMIT %s""", args)
    else:
        rows = db.query(f"""
            SELECT p.condition_id AS k, p.yes_token_id AS token
            FROM poly_markets p LEFT JOIN poly_events e USING (event_id)
            WHERE p.closed IS NOT TRUE AND p.yes_token_id IS NOT NULL
              AND COALESCE(p.volume, 0) >= %s{cat}
            ORDER BY (p.raw->>'volume24hr')::numeric DESC NULLS LAST,
                     p.volume DESC NULLS LAST, p.condition_id
            LIMIT %s""", args)
    return {r["k"]: r["token"] for r in rows}


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
    ]
    counts = {}
    with connection() as conn:
        with conn.cursor() as cur:
            for label, sql, dated in steps:
                cur.execute(sql, (cutoff,) if dated else None)
                if cur.rowcount:
                    counts[label] = cur.rowcount
    return counts

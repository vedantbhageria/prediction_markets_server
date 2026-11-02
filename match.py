"""Records and vectors for finding similar markets, within and across venues.

    python match.py                     # run forever: every MATCH_MINUTES, both steps
    python match.py --once              # both steps once
    python match.py --build --refresh   # remake every record (after rule changes)
    python match.py --report            # what the records say

  1. records   every stored market as one record in `propositions`
               (propositions.py): what is asked, about whom, compared how,
               against what number, by when. Kalshi's are mostly its own
               fields; Polymarket's mostly read from the wording.
  2. embed     each distinct template, once, with a small off-the-shelf model
               (BAAI/bge-small-en-v1.5: 384 numbers) on the CPU, into
               `templates` (vector.sql), whose HNSW index answers "nearest to
               this one" in milliseconds. Every strike of a ladder shares its
               market's template, so it shares one vector.

Nothing is matched here: the API finds a market's nearest markets when asked
(api/similar.py), with the limits and filters chosen there. Until pgvector is
enabled, vectors are kept as plain arrays in `template_vectors` and move
across by themselves once it is.
"""

import argparse
import logging
import os
import signal
import threading
import time

from psycopg2.extras import Json, execute_values

import db
import propositions as P
import store
from db import connection

log = logging.getLogger("match")

BATCH = 5000
MODEL = "BAAI/bge-small-en-v1.5"
MATCH_MINUTES = float(os.environ.get("MATCH_MINUTES", "10"))
COLUMNS = ["venue", "market_id", "group_id", "kind", "subject", "entity", "comparator", "low",
           "high", "unit", "when_at", "when_kind", "placeholder", "template", "topic", "sources"]

KALSHI_TODO = """
    SELECT m.ticker, m.raw AS market, e.raw AS event, e.category
    FROM kalshi_markets m JOIN kalshi_events e USING (event_ticker)
    WHERE m.ticker > %(after)s {missing}
    ORDER BY m.ticker LIMIT %(n)s"""
POLY_TODO = """
    SELECT m.condition_id, m.raw AS market, m.event_id, e.tags
    FROM poly_markets m LEFT JOIN poly_events e USING (event_id)
    WHERE m.condition_id > %(after)s {missing}
    ORDER BY m.condition_id LIMIT %(n)s"""
MISSING = {"kalshi": "AND NOT EXISTS (SELECT 1 FROM propositions p"
                     " WHERE p.venue = 'kalshi' AND p.market_id = m.ticker)",
           "poly": "AND NOT EXISTS (SELECT 1 FROM propositions p"
                   " WHERE p.venue = 'poly' AND p.market_id = m.condition_id)"}


def _row(r):
    return (r["venue"], r["market_id"], r["group_id"], r["kind"], r["subject"], r["entity"],
            r["comparator"], r["low"], r["high"], r["unit"], r["when"], r["when_kind"],
            bool(r["placeholder"]), r["template"], r["topic"] or None, Json(r["sources"]))


def _write(rows):
    if not rows:
        return
    sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in COLUMNS[2:])
    with connection() as conn:
        with conn.cursor() as cur:
            execute_values(cur, f"INSERT INTO propositions ({', '.join(COLUMNS)}) VALUES %s"
                                f" ON CONFLICT (venue, market_id) DO UPDATE SET {sets}, made_at = now()",
                           rows, page_size=1000)


def build(venue, refresh=False):
    """Records for this venue's markets: the missing ones, or all of them."""
    sql = (KALSHI_TODO if venue == "kalshi" else POLY_TODO).format(
        missing="" if refresh else MISSING[venue])
    after, made, failed, started = "", 0, 0, time.time()
    while True:
        batch = db.query(sql, {"after": after, "n": BATCH})
        if not batch:
            break
        rows = []
        for b in batch:
            try:
                if venue == "kalshi":
                    event = dict(b["event"] or {}, category=b["category"])
                    rec = P.from_kalshi(b["market"], event)
                else:
                    event = {"id": b["event_id"], "tags": [{"label": t} for t in b["tags"] or []]}
                    rec = P.from_poly(b["market"], event)
                rows.append(_row(rec))
            except Exception as exc:          # one odd payload must not stop the rest
                failed += 1
                if failed <= 5:
                    log.warning("%s %s: %s: %s", venue, b.get("ticker") or b.get("condition_id"),
                                type(exc).__name__, exc)
        _write(rows)
        made += len(rows)
        after = batch[-1]["ticker" if venue == "kalshi" else "condition_id"]
        if made % 100_000 < BATCH:
            log.info("%s: %s records so far, %.0fs", venue, f"{made:,}", time.time() - started)
    if made:
        log.info("%s: %s records made (%s failed) in %.0fs", venue, f"{made:,}", f"{failed:,}",
                 time.time() - started)
    return made


# ---------------------------------------------------------------------------
# Embeddings
# ---------------------------------------------------------------------------

_model = None


def model():
    global _model
    if _model is None:
        from fastembed import TextEmbedding
        _model = TextEmbedding(MODEL, threads=os.cpu_count())
    return _model


def embed(stop=None):
    """A vector for every template that has none. Returns how many were made."""
    vector = store.has_vector()
    table = "templates" if vector else "template_vectors"
    # Open markets' questions first: those are the ones searched from.
    todo = [r["template"] for r in db.query(f"""
        SELECT p.template,
               bool_or(COALESCE(k.close_time > now(), pm.closed IS NOT TRUE, false)) AS open
        FROM propositions p
        LEFT JOIN kalshi_markets k ON p.venue = 'kalshi' AND k.ticker = p.market_id
        LEFT JOIN poly_markets pm ON p.venue = 'poly' AND pm.condition_id = p.market_id
        WHERE p.template IS NOT NULL AND NOT p.placeholder
          AND NOT EXISTS (SELECT 1 FROM {table} v WHERE v.template = p.template)
        GROUP BY p.template ORDER BY open DESC""")]
    made, started = 0, time.time()
    for i in range(0, len(todo), 2000):
        if stop is not None and stop.is_set():
            break
        chunk = todo[i:i + 2000]
        vecs = [[float(x) for x in v] for v in model().embed(chunk, batch_size=256)]
        with connection() as conn:
            with conn.cursor() as cur:
                if vector:
                    execute_values(cur, "INSERT INTO templates (template, model, embedding) VALUES %s"
                                        " ON CONFLICT (template) DO NOTHING",
                                   [(t, MODEL, "[" + ",".join(f"{x:.7g}" for x in v) + "]")
                                    for t, v in zip(chunk, vecs)],
                                   template="(%s, %s, %s::vector)", page_size=500)
                else:
                    execute_values(cur, "INSERT INTO template_vectors (template, model, vec) VALUES %s"
                                        " ON CONFLICT DO NOTHING",
                                   [(t, MODEL, v) for t, v in zip(chunk, vecs)], page_size=500)
        made += len(chunk)
        log.info("embedded %s of %s templates, %.0f/s", f"{made:,}", f"{len(todo):,}",
                 made / max(time.time() - started, 1e-9))
    return made


def run_once(stop=None):
    for venue in ("kalshi", "poly"):
        build(venue)
    embed(stop)


def report():
    for r in db.query("""
            SELECT venue, count(*) AS n,
                   count(*) FILTER (WHERE placeholder) AS placeholders,
                   count(DISTINCT template) AS templates
            FROM propositions GROUP BY venue ORDER BY venue"""):
        print(f"{r['venue']:<7} {r['n']:>10,} records, {r['templates']:>9,} distinct templates,"
              f" {r['placeholders']:,} placeholders")
    table = "templates" if store.has_vector() else "template_vectors"
    n = db.query(f"SELECT count(*) AS n FROM {table}")[0]["n"]
    print(f"\n{n:,} templates embedded ({table})")
    print("\nby kind:")
    for r in db.query("SELECT venue, kind, count(*) AS n FROM propositions"
                      " GROUP BY 1, 2 ORDER BY 1, 3 DESC"):
        print(f"  {r['venue']:<7} {str(r['kind']):<10} {r['n']:>10,}")
    print("\nwhere each field came from:")
    for r in db.query("""
            SELECT venue, key AS field, value #>> '{}' AS source, count(*) AS n
            FROM propositions, jsonb_each(sources)
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"""):
        print(f"  {r['venue']:<7} {r['field']:<12} {r['source']:<7} {r['n']:>10,}")


def main():
    parser = argparse.ArgumentParser(description="Records and vectors for similar markets.")
    parser.add_argument("--build", action="store_true", help="make the missing records")
    parser.add_argument("--refresh", action="store_true", help="with --build: remake all of them")
    parser.add_argument("--venue", choices=["kalshi", "poly", "all"], default="all")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--once", action="store_true", help="both steps once, then exit")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s",
                        datefmt="%H:%M:%S")
    store.ensure_schema()
    if args.report:
        report()
    elif args.build:
        for venue in (["kalshi", "poly"] if args.venue == "all" else [args.venue]):
            build(venue, args.refresh)
    elif args.once:
        run_once()
    else:
        stop = threading.Event()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stop.set())
        while not stop.is_set():
            started = time.time()
            try:
                run_once(stop)
            except Exception as exc:
                log.warning("round failed: %s: %s", type(exc).__name__, exc)
            stop.wait(max(60.0, started + MATCH_MINUTES * 60 - time.time()))
        log.info("stopped")


if __name__ == "__main__":
    main()

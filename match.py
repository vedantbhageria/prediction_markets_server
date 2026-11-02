"""Matching markets within and across venues.

    python match.py                     # run forever: every MATCH_MINUTES, all four steps
    python match.py --once              # the four steps once
    python match.py --build --refresh   # remake every record (after rule changes)
    python match.py --report            # what the records and matches say

The steps:

  1. records     every stored market as one record in `propositions`
                 (propositions.py): what is asked, about whom, compared how,
                 against what number, by when. Kalshi's are mostly its own
                 fields; Polymarket's mostly read from the wording.
  2. embed       each distinct template, once, with a small off-the-shelf
                 model (BAAI/bge-small-en-v1.5, 384 numbers) on the CPU.
  3. candidates  for every open market's template, the most similar
                 templates -- on the other venue and on its own -- by cosine
                 similarity, all in memory (a few seconds for 100,000s).
  4. label       each candidate pair of markets judged by relate.py from the
                 records' fields; pairs it rejects are dropped. Stored in
                 `matches`, replacing the last run's.

The embedding only proposes; the fields decide. See relate.py for why.
"""

import argparse
import collections
import logging
import os
import threading
import time

import numpy as np
from psycopg2.extras import Json, execute_values

import db
import propositions as P
import relate as R
import store
from db import connection

log = logging.getLogger("match")

BATCH = 5000
MODEL = "BAAI/bge-small-en-v1.5"
MATCH_MINUTES = float(os.environ.get("MATCH_MINUTES", "30"))
TOP_OTHER, TOP_SAME = 5, 3          # candidate templates per template
MIN_SCORE = 0.80                    # cosine similarity a candidate needs
PER_SIDE = 300                      # markets compared per template, each side
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
    log.info("%s: %s records made (%s failed) in %.0fs", venue, f"{made:,}", f"{failed:,}",
             time.time() - started)
    return made


# ---------------------------------------------------------------------------
# Embeddings and matches
# ---------------------------------------------------------------------------

_model = None


def model():
    global _model
    if _model is None:
        from fastembed import TextEmbedding
        _model = TextEmbedding(MODEL)
    return _model


def embed(stop=None):
    """A vector for every template that has none. Returns how many were made."""
    todo = [r["template"] for r in db.query("""
        SELECT DISTINCT p.template FROM propositions p
        WHERE p.template IS NOT NULL AND NOT p.placeholder
          AND NOT EXISTS (SELECT 1 FROM template_vectors v WHERE v.template = p.template)""")]
    made, started = 0, time.time()
    for i in range(0, len(todo), 2000):
        if stop is not None and stop.is_set():
            break
        chunk = todo[i:i + 2000]
        vecs = list(model().embed(chunk, batch_size=256))
        with connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, "INSERT INTO template_vectors (template, model, vec) VALUES %s"
                                    " ON CONFLICT DO NOTHING",
                               [(t, MODEL, [float(x) for x in v]) for t, v in zip(chunk, vecs)],
                               page_size=500)
        made += len(chunk)
        log.info("embedded %s of %s templates, %.0f/s", f"{made:,}", f"{len(todo):,}",
                 made / max(time.time() - started, 1e-9))
    return made


OPEN = {"kalshi": "SELECT ticker AS id FROM kalshi_markets WHERE close_time > now()",
        "poly": "SELECT condition_id AS id FROM poly_markets WHERE closed IS NOT TRUE"}


def _open_records():
    """{venue: {template: [record]}} for markets still open."""
    out = {}
    for venue, sql in OPEN.items():
        rows = db.query(f"""
            SELECT p.* FROM propositions p JOIN ({sql}) o ON o.id = p.market_id
            WHERE p.venue = %s AND NOT p.placeholder AND p.template IS NOT NULL""", (venue,))
        by_t = collections.defaultdict(list)
        for r in rows:
            r["when"] = r.pop("when_at")
            by_t[r["template"]].append(r)
        out[venue] = by_t
    return out


def _vectors(templates):
    rows = {r["template"]: r["vec"] for r in db.query(
        "SELECT template, vec FROM template_vectors WHERE template = ANY(%s)", (list(templates),))}
    keep = [t for t in templates if t in rows]
    if not keep:
        return keep, np.zeros((0, 384), dtype=np.float32)
    m = np.asarray([rows[t] for t in keep], dtype=np.float32)
    return keep, m / np.linalg.norm(m, axis=1, keepdims=True)


def _nearest(qm, qt, cm, ct, top, same):
    """[(query template, candidate template, score)]: each query's `top`
    nearest candidates at MIN_SCORE or above (not itself, when `same`)."""
    out = []
    if not len(qt) or not len(ct):
        return out
    k = min(top + (1 if same else 0), len(ct))
    for i in range(0, len(qt), 2048):
        s = qm[i:i + 2048] @ cm.T
        idx = np.argpartition(-s, k - 1, axis=1)[:, :k]
        for row, cols in enumerate(idx):
            for c in cols:
                score = float(s[row, c])
                if score >= MIN_SCORE and not (same and qt[i + row] == ct[c]):
                    out.append((qt[i + row], ct[c], score))
    return out


def find_matches():
    """Candidates and labels for every open market; replaces `matches`."""
    started = time.time()
    recs = _open_records()
    kt, km = _vectors(sorted(recs["kalshi"]))
    pt, pm = _vectors(sorted(recs["poly"]))
    pairs = []
    pairs += [("poly", a, "kalshi", b, s) for a, b, s in _nearest(pm, pt, km, kt, TOP_OTHER, False)]
    pairs += [("kalshi", a, "kalshi", b, s) for a, b, s in _nearest(km, kt, km, kt, TOP_SAME, True)]
    pairs += [("poly", a, "poly", b, s) for a, b, s in _nearest(pm, pt, pm, pt, TOP_SAME, True)]
    log.info("candidates: %s template pairs from %s Kalshi and %s Polymarket templates, %.0fs",
             f"{len(pairs):,}", f"{len(kt):,}", f"{len(pt):,}", time.time() - started)
    rows, labels = {}, collections.Counter()
    for va, ta, vb, tb, score in pairs:
        for a in recs[va][ta][:PER_SIDE]:
            for b in recs[vb][tb][:PER_SIDE]:
                if (va, a["market_id"]) == (vb, b["market_id"]) or \
                        (va == vb and a["group_id"] and a["group_id"] == b["group_id"]):
                    continue        # itself, or a sibling in the same event
                label, why = R.relate(a, b)
                if label is None:
                    continue
                x, y = sorted([(va, a["market_id"]), (vb, b["market_id"])])
                key = x + y
                if key not in rows or score > rows[key][6]:
                    rows[key] = (*x, *y, label, why, score)
    for r in rows.values():
        labels[r[4]] += 1
    with connection() as conn:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE matches")
            execute_values(cur, "INSERT INTO matches (a_venue, a_market, b_venue, b_market, label,"
                                " reason, score) VALUES %s", list(rows.values()), page_size=2000)
    log.info("matches: %s (%s), %.0fs", f"{len(rows):,}",
             ", ".join(f"{k} {v:,}" for k, v in labels.most_common()), time.time() - started)
    return labels


def run_once(stop=None):
    for venue in ("kalshi", "poly"):
        build(venue)
    embed(stop)
    if stop is None or not stop.is_set():
        find_matches()


def report():
    for r in db.query("""
            SELECT venue, count(*) AS n,
                   count(*) FILTER (WHERE placeholder) AS placeholders,
                   count(DISTINCT template) AS templates
            FROM propositions GROUP BY venue ORDER BY venue"""):
        print(f"{r['venue']:<7} {r['n']:>10,} records, {r['templates']:>9,} distinct templates,"
              f" {r['placeholders']:,} placeholders")
    print("\nby kind:")
    for r in db.query("SELECT venue, kind, count(*) AS n FROM propositions"
                      " GROUP BY 1, 2 ORDER BY 1, 3 DESC"):
        print(f"  {r['venue']:<7} {str(r['kind']):<10} {r['n']:>10,}")
    print("\nmatches:")
    for r in db.query("SELECT a_venue || '-' || b_venue AS pair, label, count(*) AS n FROM matches"
                      " GROUP BY 1, 2 ORDER BY 1, 3 DESC"):
        print(f"  {r['pair']:<14} {r['label']:<13} {r['n']:>9,}")
    print("\nwhere each field came from (share of records):")
    for r in db.query("""
            SELECT venue, key AS field, value #>> '{}' AS source, count(*) AS n,
                   round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY venue, key), 1) AS pct
            FROM propositions, jsonb_each(sources)
            GROUP BY 1, 2, 3 ORDER BY 1, 2, 3"""):
        print(f"  {r['venue']:<7} {r['field']:<12} {r['source']:<7} {r['n']:>10,}")


def main():
    parser = argparse.ArgumentParser(description="Matching markets within and across venues.")
    parser.add_argument("--build", action="store_true", help="make the missing records")
    parser.add_argument("--refresh", action="store_true", help="with --build: remake all of them")
    parser.add_argument("--venue", choices=["kalshi", "poly", "all"], default="all")
    parser.add_argument("--report", action="store_true")
    parser.add_argument("--once", action="store_true", help="all four steps once, then exit")
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
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: stop.set())
        while not stop.is_set():
            started = time.time()
            try:
                run_once(stop)
            except Exception as exc:
                log.warning("matching round failed: %s: %s", type(exc).__name__, exc)
            stop.wait(max(60.0, started + MATCH_MINUTES * 60 - time.time()))


if __name__ == "__main__":
    main()

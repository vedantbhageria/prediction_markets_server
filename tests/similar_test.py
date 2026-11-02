"""Similar markets, end to end: records, vectors (pgvector) and the search.

    .venv/bin/python tests/similar_test.py

Builds a small set of made-up markets on both venues in its own schema,
makes their records and vectors with match.py, and asks api/similar.py
questions whose answers are known: the nearest markets, every filter and
limit, the gates, and the relations. Needs pgvector enabled in the database.
Drops its schema when it passes.
"""

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = "pm_similartest"
os.environ.update(PGSCHEMA=SCHEMA, PGAPPNAME="pm-similartest")
sys.path.insert(0, str(ROOT))

import db                       # noqa: E402
import match                    # noqa: E402
import store                    # noqa: E402
from api import similar as S    # noqa: E402
from api.common import ApiError  # noqa: E402
from psycopg2.extras import Json  # noqa: E402

failures = []


def check(name, ok, detail=""):
    print(("PASS " if ok else "FAIL ") + name + (f"  ({detail})" if detail and not ok else ""))
    if not ok:
        failures.append(name)


def kalshi(ticker, event, title, sub, strike_type=None, floor=None, cap=None, custom=None,
           occ="2026-12-31T17:00:00Z", close="2027-01-01T00:00:00Z", category="Crypto"):
    m = {"ticker": ticker, "event_ticker": event, "title": title, "yes_sub_title": sub,
         "strike_type": strike_type, "floor_strike": floor, "cap_strike": cap,
         "custom_strike": custom, "occurrence_datetime": occ, "close_time": close}
    return m, {"event_ticker": event, "title": title, "category": category}


def poly(cid, event, question, group="", outcomes='["Yes", "No"]', end="2026-12-31T12:00:00Z",
         closed=False):
    return {"conditionId": cid, "question": question, "groupItemTitle": group,
            "outcomes": outcomes, "endDate": end, "closed": closed,
            "clobTokenIds": json.dumps([cid + "y", cid + "n"])}, event


def load():
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
            cur.execute(f"CREATE SCHEMA {SCHEMA}")
    db.close_pool()
    store.ensure_schema()
    ks = [
        kalshi("KXBTC-26DEC31-T70000", "KXBTC-26DEC31", "Bitcoin price on Dec 31, 2026?", "$70,000 or above",
               "greater", 69999.99),
        kalshi("KXBTC-26DEC31-T75000", "KXBTC-26DEC31", "Bitcoin price on Dec 31, 2026?", "$75,000 or above",
               "greater", 74999.99),
        kalshi("KXBTC-26DEC31-B70000", "KXBTC-26DEC31", "Bitcoin price on Dec 31, 2026?", "Below $70,000",
               "less", None, 70000),
        kalshi("KXETH-26DEC31-T4000", "KXETH-26DEC31", "Ethereum price on Dec 31, 2026?", "$4,000 or above",
               "greater", 3999.99),
        kalshi("KXBTC-26JUN30-T70000", "KXBTC-26JUN30", "Bitcoin price on Jun 30, 2026?", "$70,000 or above",
               "greater", 69999.99, occ="2026-06-30T17:00:00Z", close="2026-07-01T00:00:00Z"),
        kalshi("KXPRES-28-JV", "KXPRES-28", "Who will win the 2028 presidential election?", "JD Vance",
               "custom", custom={"Candidate": "JD Vance"}, occ="2028-11-07T00:00:00Z",
               close="2028-11-08T00:00:00Z", category="Elections"),
        kalshi("KXOLD-26JAN-T60000", "KXOLD-26JAN", "Bitcoin price on Jan 15, 2026?", "$60,000 or above",
               "greater", 59999.99, occ="2026-01-15T17:00:00Z", close="2026-01-16T00:00:00Z"),
    ]
    ps = [
        poly("0xa1", {"id": "e1", "tags": []}, "Will Bitcoin be above $70,000 on December 31, 2026?", "↑ 70,000"),
        poly("0xa2", {"id": "e1", "tags": []}, "Will Bitcoin be above $80,000 on December 31, 2026?", "↑ 80,000"),
        poly("0xb1", {"id": "e2", "tags": []}, "Will Ethereum be above $4,000 on December 31, 2026?", "↑ 4,000"),
        poly("0xc1", {"id": "e3", "tags": []}, "Will JD Vance win the 2028 US Presidential Election?",
             "JD Vance", end="2028-11-07T12:00:00Z"),
        poly("0xc2", {"id": "e3", "tags": []}, "Will E win the 2028 US Presidential Election?", "E",
             end="2028-11-07T12:00:00Z"),
        poly("0xd1", {"id": "e4", "tags": []}, "Will Bitcoin be above $70,000 on December 31, 2025?", "↑ 70,000",
             end="2025-12-31T12:00:00Z", closed=True),
    ]
    with db.connection() as conn:
        with conn.cursor() as cur:
            for m, e in ks:
                cur.execute("INSERT INTO kalshi_series (series_ticker) VALUES (%s) ON CONFLICT DO NOTHING",
                            (e["event_ticker"].split("-")[0],))
                cur.execute("INSERT INTO kalshi_events (event_ticker, series_ticker, title, category, raw)"
                            " VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                            (e["event_ticker"], e["event_ticker"].split("-")[0], e["title"],
                             e["category"], Json(e)))
                cur.execute("INSERT INTO kalshi_markets (ticker, event_ticker, title, close_time, raw)"
                            " VALUES (%s, %s, %s, %s, %s)",
                            (m["ticker"], m["event_ticker"], m["title"], m["close_time"], Json(m)))
            for m, e in ps:
                cur.execute("INSERT INTO poly_events (event_id, title, tags) VALUES (%s, %s, %s)"
                            " ON CONFLICT DO NOTHING", (e["id"], "event " + e["id"], []))
                cur.execute("INSERT INTO poly_markets (condition_id, event_id, question, yes_token_id,"
                            " end_date, closed, raw) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                            (m["conditionId"], e["id"], m["question"], m["conditionId"] + "y",
                             m["endDate"], m["closed"], Json(m)))
    match.run_once()


def ask(ticker, **kw):
    with db.cursor() as cur:
        rows, _ = S.similar(cur, ticker, **kw)
    return rows


def main():
    if not store.has_vector():
        sys.exit("pgvector is not enabled in this database:"
                 " sudo -u postgres psql -d prediction_markets -c 'CREATE EXTENSION vector;'")
    t0 = time.time()
    load()
    n = db.query("SELECT count(*) AS n FROM templates")[0]["n"]
    check("records and vectors made", n >= 6, f"{n} templates")

    kal = ask("0xa1", limit=20, max_cosine=0.5, venue="kalshi")
    check("Polymarket BTC 70k: the nearest Kalshi market is BTC 70k",
          kal and kal[0]["ticker"] == "KXBTC-26DEC31-T70000", [r["ticker"] for r in kal][:4])
    top = kal[0] if kal else {}
    rows = ask("0xa1", limit=20, max_cosine=0.5)
    ids = [r["ticker"] for r in rows]
    check("...labelled identical (above 69,999.99 = at least 70,000)", top.get("relation") == "identical",
          top.get("relation"))
    below = next((r for r in rows if r["ticker"] == "KXBTC-26DEC31-B70000"), None)
    check("the opposite side is found and labelled complement",
          below is not None and below["opposite"] and below["relation"] == "complement",
          below and below["relation"])
    eth = next((r for r in rows if r["ticker"] in ("KXETH-26DEC31-T4000", "0xb1")), None)
    check("Ethereum comes after every Bitcoin market on Dec 31",
          eth is not None and all(ids.index(t) < ids.index(eth["ticker"]) for t in
                                  ("KXBTC-26DEC31-T70000", "KXBTC-26DEC31-T75000") if t in ids), ids)
    check("the market itself is not in its own answer", "0xa1" not in ids)
    sib = next((r for r in rows if r["ticker"] == "0xa2"), None)
    check("its sibling on Polymarket is marked same_event", sib is not None and sib["same_event"])

    gated = [r["ticker"] for r in ask("0xa1", limit=20, max_cosine=0.5, gates={"subject"})]
    check("gate subject drops Ethereum", not any(t in gated for t in ("KXETH-26DEC31-T4000", "0xb1")), gated)
    only_k = ask("0xa1", limit=20, max_cosine=0.5, venue="kalshi")
    check("venue=kalshi gives only Kalshi", only_k and all(r["venue"] == "kalshi" for r in only_k))
    closed = ask("0xa1", limit=20, max_cosine=0.5, status="closed")
    check("status=closed gives only closed markets",
          closed and all(r["status"] == "closed" for r in closed), [r["ticker"] for r in closed])
    both = {r["ticker"] for r in ask("0xa1", limit=50, max_cosine=0.5, status="all")}
    check("status=all includes open and closed", {"0xd1", "KXBTC-26DEC31-T70000"} <= both, both)
    near_days = ask("0xa1", limit=20, max_cosine=0.5, max_days=30)
    check("max_days=30 drops June", all(r["ticker"] != "KXBTC-26JUN30-T70000" for r in near_days))
    near_level = ask("0xa1", limit=20, max_cosine=0.5, max_level=0.05)
    check("max_level=5% keeps 70k, drops 75k and 80k",
          {r["ticker"] for r in near_level} & {"KXBTC-26DEC31-T75000", "0xa2"} == set()
          and any(r["ticker"] == "KXBTC-26DEC31-T70000" for r in near_level))
    tight = ask("0xa1", limit=20, max_cosine=0.0001)
    check("max_cosine nearly 0 keeps only the same wording", all(r["text"] <= 0.0001 for r in tight))
    ordered = ask("0xa1", limit=20, max_cosine=0.5)
    check("rows are sorted by distance", [r["distance"] for r in ordered] == sorted(r["distance"] for r in ordered))

    pres = ask("KXPRES-28-JV", limit=10, max_cosine=0.5)
    check("Kalshi JD Vance finds Polymarket JD Vance as same_outcome",
          any(r["ticker"] == "0xc1" and r["relation"] == "same_outcome" for r in pres),
          [(r["ticker"], r["relation"]) for r in pres])
    check("placeholders are never returned", all(r["ticker"] != "0xc2" for r in pres))
    try:
        ask("0xc2")
        check("asking about a placeholder is refused", False)
    except ApiError as exc:
        check("asking about a placeholder is refused", exc.status == 400)
    try:
        ask("0xnope")
        check("an unknown market is a 404", False)
    except ApiError as exc:
        check("an unknown market is a 404", exc.status == 404)

    print(f"\n{len(failures)} failed, {time.time() - t0:.0f}s")
    if failures:
        sys.exit(1)
    db.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    print("PASSED: every check")


if __name__ == "__main__":
    main()

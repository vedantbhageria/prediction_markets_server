"""The viewer's market panels: title matching and the odds / expiry filter.
Run on the server:

    .venv/bin/python tests/panels_test.py          # about half a minute

Builds its own schema (pm_panels_test), writes made-up markets through
discovery's own code, and checks:

  1  kalshi_titles holds every title discovery writes, once; prune drops the
     titles no market carries any more
  2  (title matching's ranking is tests/similar_test.py's; here only that
     its feed answers, in 5)
  3  nearby by expiry: exactly the markets in the category closing inside the
     window, nearest first
  4  nearby by odds: a market's price is its newest candle when that is newer
     than the listing, else the listing's last price; only open markets inside
     the points band; expiry + odds together; other categories when asked
  5  the pages: a market page renders both panels without waiting for them,
     /matching and /nearby answer, and an old link asking for strike gets expiry
"""

import os
import random
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.environ.update(PGSCHEMA="pm_panels_test", REDIS_PREFIX="pmpaneltest:",
                  VIEWER_USER="", VIEWER_PASSWORD_HASH="")
sys.path.insert(0, str(ROOT))

import catalogue              # noqa: E402
import db                     # noqa: E402
import store                  # noqa: E402

failures = []
NOW = datetime.now(timezone.utc).replace(microsecond=0)


def check(ok, what):
    print(("ok   " if ok else "FAIL ") + what, flush=True)
    if not ok:
        failures.append(what)


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# Made-up markets
# ---------------------------------------------------------------------------

WORDS = ["Bitcoin", "Ethereum", "Solana", "gold", "oil", "the Fed", "inflation", "Tesla",
         "Apple", "Nvidia", "the Lakers", "the Yankees", "Chelsea", "Real Madrid", "Taylor Swift",
         "OpenAI", "SpaceX", "the Senate", "the House", "Texas", "Ohio", "Paris", "Tokyo"]
VERBS = ["reach", "beat", "win", "pass", "close above", "hit", "lose to", "announce", "cut"]


def universe():
    """Events with nested markets, in Kalshi's own shape."""
    r = random.Random(7)
    events, n = [], 0

    def market(title, category, close, volume, last=None, status=None, sub=None):
        nonlocal n
        n += 1
        m = {"ticker": f"TEST-{n:04d}", "title": title, "status": status or
             ("active" if close > NOW else "settled"), "result": "" if close > NOW else "yes",
             "open_time": iso(NOW - timedelta(days=5)), "close_time": iso(close),
             "strike_type": "greater", "volume_fp": f"{volume:.2f}",
             "open_interest_fp": "10.00", "yes_sub_title": sub or title}
        if last is not None:
            m["last_price_dollars"] = f"{last:.4f}"
        return m

    def event(i, category, markets):
        for m in markets:
            m["event_ticker"] = f"TESTEV-{i:03d}"
        events.append({"event_ticker": f"TESTEV-{i:03d}", "series_ticker": f"TESTSER{i % 9}",
                       "title": f"Event {i}", "sub_title": "", "category": category,
                       "mutually_exclusive": False, "strike_period": "", "markets": markets})

    # Price ladders: one title over many markets, as Kalshi does.
    for i, (title, size) in enumerate([("Bitcoin price on Nov 3, 2026 at 5pm EDT?", 30),
                                       ("Bitcoin price on Nov 4, 2026 at 5pm EDT?", 20),
                                       ("Ethereum price on Nov 3, 2026 at 5pm EDT?", 15)]):
        close = NOW + timedelta(days=2, hours=i)
        event(i, "Crypto", [market(title, "Crypto", close, 1000 + 37 * k + i,
                                   last=round(0.05 + 0.03 * k, 2), sub=f"${60000 + 500 * k}")
                            for k in range(size)])
    # Many distinct titles, some close to each other.
    titles = set()
    while len(titles) < 400:
        titles.add(f"Will {r.choice(WORDS)} {r.choice(VERBS)} {r.choice(WORDS)} by"
                   f" {r.choice(['Nov', 'Dec', 'Jan'])} {r.randint(1, 28)}?")
    for i, title in enumerate(sorted(titles), start=10):
        category = r.choice(["Crypto", "Politics", "Sports"])
        close = NOW + timedelta(minutes=r.randint(-2 * 1440, 3 * 1440))
        event(i, category, [market(title, category, close, r.randint(1, 10**6) + i / 1000,
                                   last=round(r.uniform(0.02, 0.98), 2))])
    return events


def write(events):
    rows_e, rows_m = catalogue._kalshi_rows(events, categories=[])
    catalogue._write_kalshi(rows_e, rows_m)


def setup():
    with db.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP SCHEMA IF EXISTS pm_panels_test CASCADE")
            cur.execute("CREATE SCHEMA pm_panels_test")
    store.ensure_schema()
    write(universe())
    # Every market tracked, read by the last scan 10 minutes ago.
    db.execute("""INSERT INTO kalshi_tracking (ticker, volume, volume_measured_at,
                      candles_fetched_from, candles_fetched_to)
                  SELECT ticker, volume, %s, %s, %s FROM kalshi_markets""",
               (NOW - timedelta(minutes=10), NOW - timedelta(days=5), NOW - timedelta(minutes=2)))


# ---------------------------------------------------------------------------

def test_titles():
    have = {r["title"] for r in db.query("SELECT title FROM kalshi_titles")}
    want = {r["title"] for r in db.query("SELECT DISTINCT title FROM kalshi_markets")}
    check(have == want and len(want) == 403,
          f"1 kalshi_titles = the distinct titles discovery wrote ({len(have)} of {len(want)})")
    write(universe())                       # a second pass changes nothing
    check(db.query("SELECT count(*) AS n FROM kalshi_titles")[0]["n"] == 403,
          "1 writing the same markets again adds no titles")
    # A market long closed, with a title of its own: prune takes both.
    db.execute("INSERT INTO kalshi_markets (ticker, event_ticker, title, close_time, raw)"
               " VALUES ('TEST-OLD', 'TESTEV-010', 'An orphan title', %s, '{}')",
               (NOW - timedelta(days=40),))
    db.execute("INSERT INTO kalshi_titles VALUES ('An orphan title')")
    dropped = store.prune(30)
    left = db.query("SELECT count(*) AS n FROM kalshi_titles"
                    " WHERE title = 'An orphan title'")[0]["n"]
    check(left == 0 and dropped.get("kalshi titles") == 1
          and db.query("SELECT count(*) AS n FROM kalshi_titles")[0]["n"] == 403,
          f"1 prune drops a title no market carries, and only that one: {dropped}")


def test_nearby(viewer):
    # Expiry: a busy single market in Crypto.
    anchor = db.query("""
        SELECT m.ticker, m.close_time, e.category FROM kalshi_markets m
        JOIN kalshi_events e USING (event_ticker)
        WHERE e.category = 'Crypto' AND m.event_ticker >= 'TESTEV-010'
        ORDER BY m.volume DESC LIMIT 1""")[0]
    for hours in (1, 6, 24):
        got = viewer.closing_near("kalshi_candles", anchor["ticker"], hours=hours, by="time")
        want = db.query("""
            SELECT m.ticker FROM kalshi_markets m JOIN kalshi_events e USING (event_ticker)
            WHERE e.category = %s AND m.ticker <> %s
              AND m.close_time BETWEEN %s AND %s
            ORDER BY abs(EXTRACT(EPOCH FROM m.close_time - %s)), m.volume DESC LIMIT 40""",
                        (anchor["category"], anchor["ticker"],
                         anchor["close_time"] - timedelta(hours=hours),
                         anchor["close_time"] + timedelta(hours=hours), anchor["close_time"]))
        check([r["id"] for r in got["rows"]] == [r["ticker"] for r in want]
              and (hours < 24 or len(want) > 5),
              f"3 same expiry ±{hours}h: {len(got['rows'])} markets, nearest first")

    # Odds. Fresh markets, one category of their own, prices set by hand.
    later = NOW + timedelta(days=1)
    events = []

    def m(ticker, last, close=later, category="Odds"):
        return {"ticker": ticker, "event_ticker": f"ODDS-{ticker}", "title": f"Odds market {ticker}",
                "status": "active" if close > NOW else "settled", "result": "",
                "open_time": iso(NOW - timedelta(days=1)), "close_time": iso(close),
                "volume_fp": "100.00", "open_interest_fp": "1.00",
                **({"last_price_dollars": f"{last:.4f}"} if last is not None else {}),
                "_category": category}
    plan = [m("PA", 0.50),                         # the anchor: listing only
            m("P1", 0.40), m("P2", 0.47), m("P3", 0.52), m("P4", None),
            m("P5", 0.50, close=NOW - timedelta(hours=2)),   # closed
            m("P6", 0.50, category="Other"),       # another category
            m("P7", 0.51),                         # its newer candle says 0.80
            m("P8", 0.58),                         # its newer candle says 0.52
            m("P9", 0.70)]                         # far off
    for x in plan:
        cat = x.pop("_category")
        events.append({"event_ticker": x["event_ticker"], "series_ticker": "ODDSSER",
                       "title": "odds", "category": cat, "markets": [x]})
    write(events)
    db.execute("""INSERT INTO kalshi_tracking (ticker, volume, volume_measured_at,
                      candles_fetched_from, candles_fetched_to)
                  SELECT ticker, volume, %s, %s, %s FROM kalshi_markets WHERE ticker LIKE 'P_'""",
               (NOW - timedelta(minutes=10), NOW - timedelta(days=1), NOW - timedelta(minutes=2)))
    for ticker, close, age in (("P1", 0.54, 2), ("P2", 0.20, 180), ("P4", 0.50, 30),
                               ("P7", 0.80, 3), ("P8", 0.52, 4)):
        ts = NOW.replace(second=0) - timedelta(minutes=age)
        store.write_candles("kalshi", [(ticker, ts, close, close, close, close, 5, 1)])
    # P1: candle (2 min) newer than the listing (10 min) -> 0.54
    # P2: candle three hours old -> the listing's 0.47
    # P4: no listing price -> its candle's 0.50, though older than the listing
    got = viewer.closing_near("kalshi_candles", "PA", by="price", points=5)
    prices = {r["id"]: r["last"] for r in got["rows"]}
    check(set(prices) == {"P1", "P2", "P3", "P4", "P8"},
          f"4 same odds ±5pt: open markets in the category inside the band: {sorted(prices)}")
    check(prices.get("P1") == 0.54 and prices.get("P2") == 0.47 and prices.get("P4") == 0.5
          and prices.get("P8") == 0.52,
          f"4 price = newest candle if newer than the listing, else the listing: {prices}")
    gaps = [abs(r["last"] - 0.5) for r in got["rows"]]
    check(got["rows"] and got["rows"][0]["id"] == "P4" and gaps == sorted(gaps),
          f"4 nearest odds first: {[r['id'] for r in got['rows']]}")
    wide = viewer.closing_near("kalshi_candles", "PA", by="price", points=5, same_category=False)
    check("P6" in {r["id"] for r in wide["rows"]}, "4 all categories brings in the other category")
    both = viewer.closing_near("kalshi_candles", "PA", by="price,time", hours=1, points=5)
    check({r["id"] for r in both["rows"]} == {"P1", "P2", "P3", "P4", "P8"}
          and both["by"] == "price,time", "4 expiry + odds")
    check(viewer.closing_near("kalshi_candles", "PA", by="strike")["by"] == "time",
          "4 an old strike link falls back to expiry")


def test_pages(viewer):
    viewer.LOGIN_USER = None
    c = viewer.app.test_client()
    page = c.get("/table/kalshi_candles?ticker=TEST-0001")
    text = page.get_data(as_text=True)
    check(page.status_code == 200 and "title matching" in text and "odds / expiry filter" in text
          and "loading" in text and "Not built yet" not in text and "same strike" not in text,
          "5 a market page renders both panels, loading, with no strike control")
    # No vectors in this schema: the feed says why instead of failing.
    rel = c.get("/matching?table=kalshi_candles&ticker=TEST-0001&venue=other").get_json()
    near = c.get("/nearby?table=kalshi_candles&ticker=PA&by=price&pts=5&cat=1").get_json()
    old = c.get("/nearby?table=kalshi_candles&ticker=PA&by=strike&pct=2").get_json()
    check(isinstance(rel["rows"], list) and near["rows"] and old["by"] == "time",
          f"5 /matching ({len(rel['rows'])}, {rel.get('error')}) and /nearby ({len(near['rows'])}) answer;"
          f" by=strike gives {old['by']}")
    check(c.get("/table/kalshi_markets?ticker=PA").status_code == 200,
          "5 a market's row in kalshi_markets renders too")


def main():
    setup()
    import viewer
    test_titles()
    test_nearby(viewer)
    test_pages(viewer)
    print()
    if failures:
        print(f"FAILED: {len(failures)} checks")
        sys.exit(1)
    print("PASSED: every check")
    db.execute("DROP SCHEMA IF EXISTS pm_panels_test CASCADE")       # kept only when a check fails


if __name__ == "__main__":
    main()

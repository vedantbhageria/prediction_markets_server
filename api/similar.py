"""The markets nearest to one market, on either venue, found when asked.

    GET /v1/markets/{ticker}/similar

1. The market's template vector (vector.sql) is looked up, and the HNSW
   index gives the nearest templates by cosine distance -- milliseconds over
   every template on both venues.
2. Every market behind those templates is read, on the venues and in the
   statuses asked for.
3. Each is measured against the market asked about:

       text      cosine distance of the two questions' templates (0 = same
                 wording; numbers, dates and the named outcome are masked,
                 so a ladder's strikes all share one template)
       days      days between the dates the two questions turn on
       level     relative gap between their thresholds, |a - b| / max(|a|, |b|)
                 (0.05 = 5%), where both have one in the same unit
       opposite  true where one asks "above" and the other "below"

   and those make one distance to sort by:

       distance = text + 0.10 * min(days, 730) / 365
                       + 0.50 * min(level, 1) + (0.05 if opposite)

4. The limits asked for drop what is too far: max_cosine on text, max_days
   on days, max_level on level. A limit applies only where both markets have
   that measure (a question with no threshold has no level).
5. Optional gates drop near-misses the measures cannot see: `subject` (the
   same subject or named outcome: "Bitcoin above X" and "Ethereum above X"
   are 0.08 apart in text) and `keywords` (the words that change a question,
   and the places and names in it, agree).

Each row also carries `relation` -- identical, complement, nested,
overlapping, same_outcome or related -- worked out from the two records'
fields there and then (relate.py), or null where the rules see none. It is a
reading aid, not a filter; nothing is stored.
"""

from api.common import ApiError
import relate as R

UP = {"greater", "greater_or_equal"}
DOWN = {"less", "less_or_equal"}
COLUMNS = ["ticker", "venue", "title", "status", "same_event", "kind", "subject", "entity",
           "comparator", "low", "high", "unit", "when", "when_kind", "text", "days", "level",
           "opposite", "distance", "relation"]
VENUE_SQL = {
    "kalshi": """SELECT p.*, m.title, (m.close_time > now()) AS open
                 FROM propositions p JOIN kalshi_markets m ON m.ticker = p.market_id
                 WHERE p.venue = 'kalshi'""",
    "poly": """SELECT p.*, m.question AS title, (m.closed IS NOT TRUE) AS open
               FROM propositions p JOIN poly_markets m ON m.condition_id = p.market_id
               WHERE p.venue = 'poly'""",
}
MAX_TEMPLATES = 1000        # nearest templates read (pgvector's ef_search ceiling)
MAX_MARKETS = 50_000        # markets measured per request


def _venue_of(ticker):
    return "poly" if ticker.startswith("0x") else "kalshi"


def _record(row):
    r = dict(row)
    r["when"] = r.pop("when_at", None)
    return r


def _days(a, b):
    sa, sb = R.span(a["when"]), R.span(b["when"])
    if sa is None or sb is None:
        return None
    return abs((sa[1] - sb[1]).days)


def _level(a, b):
    la, lb = a.get("low"), b.get("low")
    if la is None or lb is None or a.get("kind") != "threshold" or b.get("kind") != "threshold":
        return None
    ua, ub = a.get("unit"), b.get("unit")
    if ua and ub and ua != ub:
        return None
    top = max(abs(la), abs(lb))
    return 0.0 if top == 0 else abs(la - lb) / top


def _opposite(a, b):
    ca, cb = a.get("comparator"), b.get("comparator")
    return (ca in UP and cb in DOWN) or (ca in DOWN and cb in UP)


def distance(text, days, level, opposite):
    return (text + 0.10 * min(days or 0, 730) / 365 + 0.50 * min(level or 0, 1)
            + (0.05 if opposite else 0))


def similar(cur, ticker, limit=50, venue="all", status="open", max_cosine=0.3,
            max_days=None, max_level=None, gates=()):
    """(rows nearest first, the market asked about)."""
    home = _venue_of(ticker)
    cur.execute("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
    if not cur.fetchone():
        raise ApiError(503, "unavailable", "similar markets need pgvector, which is not enabled"
                                           " on this server yet")
    cur.execute(f"SELECT x.*, t.embedding::text AS embedding FROM ({VENUE_SQL[home]}"
                f" AND p.market_id = %s) x LEFT JOIN templates t USING (template)", (ticker,))
    me = cur.fetchone()
    if not me:
        raise ApiError(404, "not_found", f"no market {ticker!r} with a record yet (records are"
                       " made within ten minutes of a market being listed)")
    if me["placeholder"]:
        raise ApiError(400, "invalid_parameter", "that market is a placeholder the venue has"
                       " not named yet; it has nothing to match")
    if me["embedding"] is None:
        raise ApiError(404, "not_found", "that market's question has no vector yet (made within"
                       " ten minutes of the market being listed)")
    me = _record(me)

    k = min(MAX_TEMPLATES, max(200, limit * 20))
    cur.execute("SET LOCAL hnsw.ef_search = %s", (k,))
    cur.execute("""SELECT template, embedding <=> %s::vector AS d FROM templates
                   ORDER BY embedding <=> %s::vector LIMIT %s""",
                (me["embedding"], me["embedding"], k))
    near = {r["template"]: float(r["d"]) for r in cur.fetchall()
            if max_cosine is None or float(r["d"]) <= max_cosine}
    if not near:
        return [], me

    venues = ["kalshi", "poly"] if venue == "all" else [venue]
    where = {"open": " AND m_open", "closed": " AND NOT m_open", "all": ""}[status]
    candidates = []
    for v in venues:
        cur.execute(f"""SELECT * FROM ({VENUE_SQL[v]} AND p.template = ANY(%s)
                                       AND NOT p.placeholder) x
                        WHERE true {where.replace('m_open', 'x.open')}
                        LIMIT %s""", (list(near), MAX_MARKETS))
        candidates += [_record(r) for r in cur.fetchall()]

    rows = []
    for c in candidates:
        if c["venue"] == me["venue"] and c["market_id"] == me["market_id"]:
            continue
        text = near[c["template"]]
        days, level, opp = _days(me, c), _level(me, c), _opposite(me, c)
        if max_days is not None and days is not None and days > max_days:
            continue
        if max_level is not None and level is not None and level > max_level:
            continue
        if "subject" in gates:
            mine, theirs = me.get("subject") or me.get("entity"), c.get("subject") or c.get("entity")
            if mine and theirs and not R.same_thing(mine, theirs):
                continue
        if "keywords" in gates:
            _, crit, named = R.question_similarity(me, c)
            if not (crit and named):
                continue
        rows.append({
            "ticker": c["market_id"], "venue": c["venue"], "title": c["title"],
            "status": "open" if c["open"] else "closed",
            "same_event": c["venue"] == me["venue"] and bool(c["group_id"]) and c["group_id"] == me["group_id"],
            "kind": c["kind"], "subject": c["subject"], "entity": c["entity"],
            "comparator": c["comparator"], "low": c["low"], "high": c["high"], "unit": c["unit"],
            "when": c["when"], "when_kind": c["when_kind"],
            "text": round(text, 4), "days": days, "level": None if level is None else round(level, 4),
            "opposite": opp, "distance": round(distance(text, days, level, opp), 4),
            "relation": R.relate(me, c)[0]})
    rows.sort(key=lambda r: (r["distance"], r["ticker"]))
    return rows[:limit], me

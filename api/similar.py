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
                 (0.05 = 5%), where both have one in the same unit. Where the
                 market asked about has a threshold and the other has none
                 (or one in another unit) it counts as 1 in the distance -- a
                 question about something else ("Bitcoin kimchi premium hits
                 8%") must not beat the same question at another strike
       opposite  true where one asks "above" and the other "below"
       name      how far apart the two named outcomes are, where the market
                 asked about has one: 1 - trigram similarity of the two names
                 (0 = the same name; "Los Angeles R" and "Los Angeles Rams"
                 about 0.2), and 1 for a market with no name. The template
                 masks the name, so without it every candidate in a "who will
                 win" event looks equally near.

   and those make one distance to sort by, each part with its weight
   (WEIGHTS; every one can be set per request):

       distance = 1.00 * text + 0.10 * min(days, 730) / 365
                + 0.50 * min(level, 1) + 0.05 * opposite + 0.40 * name

   A weight of 0 leaves that part out: name 0 is the ranking from before
   names counted.

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
import re

import relate as R

UP = {"greater", "greater_or_equal"}
DOWN = {"less", "less_or_equal"}
COLUMNS = ["ticker", "venue", "title", "status", "same_event", "kind", "subject", "entity",
           "comparator", "low", "high", "unit", "when", "when_kind", "text", "days", "level",
           "opposite", "name", "distance", "relation"]
VENUE_SQL = {
    "kalshi": """SELECT p.*, m.title, (m.close_time > now()) AS open
                 FROM propositions p JOIN kalshi_markets m ON m.ticker = p.market_id
                 WHERE p.venue = 'kalshi'""",
    "poly": """SELECT p.*, m.question AS title, (m.closed IS NOT TRUE) AS open
               FROM propositions p JOIN poly_markets m ON m.condition_id = p.market_id
               WHERE p.venue = 'poly'""",
}
# How much each part counts in the distance (see the module's doc).
WEIGHTS = {"text": 1.0, "days": 0.10, "level": 0.50, "opposite": 0.05, "name": 0.40}
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


def _grams(name):
    """pg_trgm's trigrams of a name's words of two letters or more (a party
    suffix like "(R)" says nothing about who it is)."""
    out = set()
    for word in re.findall(r"[a-z0-9]+", (name or "").lower()):
        if len(word) > 1:
            word = "  " + word + " "
            out |= {word[i:i + 3] for i in range(len(word) - 2)}
    return out


def _abbreviates(a, b):
    """One name is the other cut short, word for word: "Los Angeles R" and
    "Los Angeles Rams", "South Dakota St." and "South Dakota State", "August
    Pfluger" and "August Pfluger (R)". Two words at least, so "Drake" does
    not match "Drake Bell"."""
    wa, wb = re.findall(r"[a-z0-9]+", a.lower()), re.findall(r"[a-z0-9]+", b.lower())
    short, long_ = (wa, wb) if len(wa) <= len(wb) else (wb, wa)
    return (len(short) >= 2 and long_[:len(short) - 1] == short[:-1]
            and long_[len(short) - 1].startswith(short[-1]))


def _name(a, b):
    """1 - trigram similarity of the two named outcomes (0 where one name
    abbreviates the other); 1 where only the market asked about has one;
    None where it has none (nothing to compare)."""
    if not a.get("entity"):
        return None
    if b.get("entity") and _abbreviates(a["entity"], b["entity"]):
        return 0.0
    ga, gb = _grams(a["entity"]), _grams(b.get("entity"))
    if not ga or not gb:
        return 1.0
    return 1 - len(ga & gb) / len(ga | gb)


def _level_cost(a, level):
    """The level that counts in the distance: the gap where both have a
    threshold, 1 where only the market asked about has one."""
    if level is not None:
        return level
    return 1.0 if a.get("kind") == "threshold" and a.get("low") is not None else 0.0


def distance(text, days, level, opposite, name=None, weights=None):
    w = WEIGHTS if weights is None else weights
    return (w["text"] * text + w["days"] * min(days or 0, 730) / 365
            + w["level"] * min(level or 0, 1) + w["opposite"] * (1 if opposite else 0)
            + w["name"] * (name or 0))


def similar(cur, ticker, limit=50, venue="all", status="open", max_cosine=0.3,
            max_days=None, max_level=None, gates=(), weights=None):
    """(rows nearest first, the market asked about). `weights` overrides any
    of WEIGHTS."""
    weights = dict(WEIGHTS, **(weights or {}))
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

    # Each venue x status slice has its own index (vector.sql), searched only
    # if asked for: filtering after one search over everything could leave
    # nothing (the nearest 200 all on the other venue, or all closed).
    venues = ["kalshi", "poly"] if venue == "all" else [venue]
    states = ["open", "closed"] if status == "all" else [status]
    k = min(MAX_TEMPLATES, max(200, limit * 20))
    cur.execute("SET LOCAL hnsw.ef_search = %s", (k,))
    near = {}
    for v in venues:
        for st in states:
            cur.execute(f"""SELECT template, embedding <=> %s::vector AS d FROM templates
                            WHERE {v}_{st} ORDER BY embedding <=> %s::vector LIMIT %s""",
                        (me["embedding"], me["embedding"], k))
            for r in cur.fetchall():
                d = float(r["d"])
                if max_cosine is None or d <= max_cosine:
                    near[r["template"]] = d
    if not near:
        return [], me

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
        days, level, opp, name = _days(me, c), _level(me, c), _opposite(me, c), _name(me, c)
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
            "opposite": opp, "name": None if name is None else round(name, 4),
            "distance": round(distance(text, days, _level_cost(me, level), opp, name, weights), 4),
            "relation": c})
    rows.sort(key=lambda r: (r["distance"], r["ticker"]))
    rows = rows[:limit]
    # The label is a reading aid and the slowest part: only for rows returned.
    for r in rows:
        r["relation"] = R.relate(me, r["relation"])[0]
    return rows, me

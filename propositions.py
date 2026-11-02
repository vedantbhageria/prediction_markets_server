"""Every market on both venues as one record: what is asked, about whom,
compared how, against what number, by when.

    record = from_kalshi(market_payload, event_payload)
    record = from_poly(market_payload, event_payload)

Each field is taken from the venue's own fields where it has them, and read
from the wording otherwise; `sources` says which, field by field ("native",
"regex", or later "model"). The record is what matching works on: a text
embedding of `template` finds markets about the same event, and the other
fields decide how two such markets relate (see match.py).

Fields
    venue, market_id, group_id    which market, and its event
    kind        threshold   a number against a level ("above $70k")
                outcome     one named outcome of a set ("Javier Milei")
                deadline    something happens before a date
                matchup     one side of a game
                total       a game's over/under
                spread      a game's handicap
                up_down     a price up or down over a short window
                event       a yes/no question with nothing to vary
    subject     what it is about, for a threshold ("bitcoin", "30-yr frm")
    entity      the named outcome, for an outcome or a matchup
    comparator  greater, greater_or_equal, less, less_or_equal, between,
                exactly, wins, is, happens
    low, high   the level(s), as plain numbers (70k -> 70000, 2.5% -> 2.5)
    unit        usd, percent, bps, cents, degrees_f, ... or a counted noun
    when        ISO date (or year) the question turns on
    when_kind   by (any time until), on (at that time), within (a period)
    placeholder True for outcomes the venue has not named yet ("Person AB")
    template    the question with its numbers, dates and named outcome
                replaced by slots -- one text for a whole ladder of strikes
"""

import json
import re
from datetime import datetime

MONTHS = {m: i + 1 for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august", "september",
     "october", "november", "december"])}
MONTHS.update({m[:3]: i for m, i in list(MONTHS.items())})
MONTHS["sept"] = 9
MONTH = r"(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|june?|july?|aug(?:ust)?|sept?(?:ember)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)"

# A number with optional currency, scale word and unit. The scale letters must
# end a word, so "$200 by" is not read as $200 billion.
NUMBER = re.compile(
    r"(?P<neg>[-−])?(?P<cur>\$|€|£)?\s?(?P<n>\d{1,3}(?:,\d{3})+(?:\.\d+)?|\d+(?:\.\d+)?)"
    r"\s?(?P<scale>k|m|mm|b|bn|t|thousand|million|billion|trillion)?(?![a-z])"
    r"\s?(?P<unit>%|percent|bps|basis points|¢|cents?|°\s?[fc]|degrees?)?", re.I)
SCALES = {"k": 1e3, "thousand": 1e3, "m": 1e6, "mm": 1e6, "million": 1e6, "b": 1e9, "bn": 1e9,
          "billion": 1e9, "t": 1e12, "trillion": 1e12}

DATE = re.compile(
    rf"(?P<md>{MONTH}\.?\s+(?P<d1>\d{{1,2}})(?:st|nd|rd|th)?(?:,?\s+(?P<y1>\d{{4}}))?)"
    rf"|(?P<dm>(?P<d2>\d{{1,2}})(?:st|nd|rd|th)?\s+{MONTH}(?:,?\s+(?P<y2>\d{{4}}))?)"
    rf"|(?P<iso>(?P<y3>\d{{4}})-(?P<m3>\d{{2}})-(?P<d3>\d{{2}}))"
    rf"|(?P<my>{MONTH}\s+(?P<y4>\d{{4}}))"
    rf"|(?P<q>q(?P<qn>[1-4])\s*(?P<y5>\d{{4}}))"
    rf"|(?P<y6>\b20\d\d\b)", re.I)
WHEN_WORD = re.compile(r"\b(by|before|on|at|in|during|after|through|end of)\s*(?:the\s+)?(?:end of\s+)?$", re.I)

# Comparator phrases, most specific first. Each names where the number is.
COMPARATORS = [
    ("rank", re.compile(r"\btop[\s-]?(?P<a>\d{1,3})\b(?!\s*%)")),
    ("between", re.compile(r"\bbetween\s+(?P<a>{n})\s+(?:and|to|-)\s+(?P<b>{n})")),
    ("greater_or_equal", re.compile(r"\b(?:at least|no less than|minimum of|≥|>=)\s*(?P<a>{n})")),
    ("greater_or_equal", re.compile(r"(?P<a>{n})\s*(?:\+|or more|or higher|or above|or greater|or over)")),
    ("less_or_equal", re.compile(r"\b(?:at most|no more than|maximum of|≤|<=)\s*(?P<a>{n})")),
    ("less_or_equal", re.compile(r"(?P<a>{n})\s*(?:or less|or fewer|or lower|or below|or under)")),
    ("greater", re.compile(r"\b(?:above|over|more than|greater than|higher than|exceeds?|exceeding|"
                           r"reach(?:es)?|hits?|surpass(?:es)?|breaks?|tops?(?=\s)|climbs? to|rises? to|"
                           r"goes? above|close above|be above|>|↑)\s*(?P<a>{n})")),
    ("less", re.compile(r"\b(?:below|under|less than|fewer than|lower than|dips? to|falls? to|"
                        r"drops? to|close below|be below|<|↓)\s*(?P<a>{n})")),
    ("between", re.compile(r"(?<![\d-])(?P<a>{n})\s*(?:-|–|to)\s*(?P<b>{n})(?![\d-])")),
    ("exactly", re.compile(r"\bexactly\s+(?P<a>{n})")),
]
_N = r"[-−]?[$€£]?\s?\d[\d,]*(?:\.\d+)?\s?(?:k|m|mm|b|bn|t|thousand|million|billion|trillion)?(?![a-z])\s?(?:%|percent|bps|¢|cents?|°\s?[fc])?"
COMPARATORS = [(kind, re.compile(p.pattern.replace("{n}", _N), re.I)) for kind, p in COMPARATORS]

UPDOWN = re.compile(r"^(?P<subject>.+?)\s+up or down\b", re.I)
SPREAD = re.compile(r"\bspread:?\s*(?P<team>.+?)\s*\((?P<line>[-+]?\d+(?:\.\d+)?)\)", re.I)
TOTAL = re.compile(r"\bo/u\s*(?P<line>\d+(?:\.\d+)?)", re.I)
VS = re.compile(r"^(?:[\w .&'-]+:\s*)?(?P<a>.+?)\s+(?:vs\.?|v\.?|@)\s+(?P<b>.+?)(?=\s*(?:[-–:(?]|$))", re.I)
EXACT_SCORE = re.compile(r"^exact score:", re.I)
YEAR_RANGE = re.compile(r"\b(?:19|20)\d\d\s*[-–/]\s*(?:19|20)?\d\d\b")
PLACEHOLDER = re.compile(
    r"^(?:[A-Z]{1,2}|(?:person|player|candidate|contestant|team|party|coach|artist|movie|song|"
    r"option|company|country|nominee|other|show|film|album|book|city|state|driver|horse|"
    r"fighter|club|nation|game)\s+[A-Z0-9]{1,3}|\"?(?:song|movie|artist)\s+[A-Z0-9]{1,3}\"?)$", re.I)
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
UNIT_WORDS = re.compile(
    r"\b(goals?|points?|yards?|touchdowns?|seats?|votes?|tweets?|posts?|mentions?|games?|wins?|"
    r"days?|hours?|minutes?|inches?|people|users?|deliveries|vehicles|subscribers|units|"
    r"homeruns?|home runs?|strikeouts?|rebounds?|assists?|kills?|maps?|sets?|laps?|medals?|"
    r"times|episodes|members|deaths|cases|jobs|barrels|ounces|tonnes|tons|sacks|"
    r"interceptions|receptions|catches|saves|shots|corners|cards|runs|hits|aces|birdies|"
    r"followers|downloads|streams|views)\b", re.I)
KALSHI_NUMERIC = {"greater", "greater_or_equal", "less", "less_or_equal", "between"}


# ---------------------------------------------------------------------------
# Small readers
# ---------------------------------------------------------------------------

def number(text):
    """(value, unit) from '$70k', '2.5%', '300,000', '1.2 billion', or (None, None)."""
    m = NUMBER.search(text or "")
    if not m:
        return None, None
    value = float(m.group("n").replace(",", ""))
    if m.group("scale"):
        value *= SCALES[m.group("scale").lower()]
    if m.group("neg"):
        value = -value
    unit = None
    u = (m.group("unit") or "").lower().replace(" ", "")
    if m.group("cur"):
        unit = {"$": "usd", "€": "eur", "£": "gbp"}[m.group("cur")]
    elif u in ("%", "percent"):
        unit = "percent"
    elif u in ("bps", "basispoints"):
        unit = "bps"
    elif u.startswith("¢") or u.startswith("cent"):
        unit = "cents"
    elif u.startswith("°f"):
        unit = "degrees_f"
    elif u.startswith("°c"):
        unit = "degrees_c"
    elif u.startswith("degree"):
        unit = "degrees"
    return value, unit


def _range(a_text, b_text):
    """Two ends of a range, sharing a scale or unit written once: '300-400k',
    '$10B and $15B', '2-3%'."""
    a, ua = number(a_text)
    b, ub = number(b_text)
    if a is None or b is None:
        return a, b, ua or ub
    sb = NUMBER.search(b_text)
    sa = NUMBER.search(a_text)
    if sb and sa and sb.group("scale") and not sa.group("scale"):
        a *= SCALES[sb.group("scale").lower()]
    return a, b, ua or ub


def date_of(text, default_year=None):
    """(iso date or year, the matched text, where it starts) for the first date
    in the text, or (None, None, None)."""
    def month(text):
        word = re.search(r"[a-z]+", text.lower()).group(0)
        return MONTHS.get(word) or MONTHS[word[:3]]

    for m in DATE.finditer(text or ""):
        g = m.groupdict()
        try:
            if g["md"]:
                mon, day, year = month(g["md"]), int(g["d1"]), g["y1"] or default_year
            elif g["dm"]:
                mon, day, year = month(g["dm"]), int(g["d2"]), g["y2"] or default_year
            elif g["iso"]:
                return f"{g['y3']}-{g['m3']}-{g['d3']}", m.group(0), m.start()
            elif g["my"]:
                return f"{g['y4']}-{month(g['my']):02d}", m.group(0), m.start()
            elif g["q"]:
                return f"{g['y5']}-Q{g['qn']}", m.group(0), m.start()
            else:
                return g["y6"], m.group(0), m.start()
            if not year:
                return f"--{mon:02d}-{day:02d}", m.group(0), m.start()
            datetime(int(year), mon, day)
            return f"{year}-{mon:02d}-{day:02d}", m.group(0), m.start()
        except (KeyError, ValueError, AttributeError):
            continue
    return None, None, None


def when_kind(text, start):
    """by / on / within, from the word before the date."""
    if start is None:
        return None
    w = WHEN_WORD.search(text[:start])
    if not w:
        return "on"
    word = w.group(1).lower()
    return {"by": "by", "before": "by", "through": "by", "on": "on", "at": "on",
            "in": "within", "during": "within", "end of": "on", "after": "after"}[word]


def masked(text, entity=None):
    """The question with its named outcome, dates, numbers and comparator
    words replaced by slots. Bare years stay: '2028 election' names the event."""
    t = YEAR_RANGE.sub(lambda x: x.group(0).replace("-", "/").replace("–", "/"), text or "")
    if entity and len(entity) > 1:
        t = re.sub(r"\b" + re.escape(entity) + r"\w*", " ⟨x⟩ ", t, flags=re.I)
    t = TOTAL.sub(" O/U ⟨n⟩ ", t)
    t = SPREAD.sub(lambda x: f"Spread: {x.group('team')} (⟨n⟩)", t)
    t = re.sub(DATE.pattern.replace(r"|(?P<y6>\b20\d\d\b)", ""), " ⟨date⟩ ", t, flags=re.I)
    for _, pat in COMPARATORS:
        t = pat.sub(" ⟨cmp⟩ ⟨n⟩ ", t)
    # Amounts (money, percentages, scaled numbers) are levels, not names;
    # plain numbers ("11th District", "30-Year", "Nebraska 03") stay.
    t = re.sub(r"[-−]?[$€£]\s?\d[\d,]*(?:\.\d+)?\s?(?:k|m|b|bn|t|thousand|million|billion|trillion)?(?![a-z])"
               r"|\d[\d,]*(?:\.\d+)?\s?(?:%|percent|bps|¢|°\s?[fc])"
               r"|\d[\d,]*(?:\.\d+)?\s?(?:k|m|bn|thousand|million|billion|trillion)(?![a-z])",
               "⟨n⟩", t, flags=re.I)
    t = re.sub(r"\s+([?:,.)])", r"\1", t)
    return re.sub(r"\s+", " ", t).strip()


def _record(venue, market_id, group_id):
    return {"venue": venue, "market_id": market_id, "group_id": group_id, "kind": None,
            "subject": None, "entity": None, "comparator": None, "low": None, "high": None,
            "unit": None, "when": None, "when_kind": None, "placeholder": False,
            "template": None, "topic": [], "sources": {}}


def _set(rec, source, **fields):
    for k, v in fields.items():
        if v is not None and rec.get(k) in (None, [], False):
            rec[k] = v
            rec["sources"][k] = source


def _subject(text):
    """What a threshold question is about: the words before its comparator,
    and the measure named just after its number ("Spotify ... 1B total
    users" -> "spotify total users"), without the question scaffolding."""
    t = re.sub(r"^(?:will|does|is|are|did|can)\s+(?:the\s+)?", "", text.strip(), flags=re.I)
    for _, pat in COMPARATORS:
        m = pat.search(t)
        if m:
            after = re.split(r"\b(?:in|by|on|at|during|for|before|after|this|next|one|of)\b|[?,(]",
                             t[m.end():], maxsplit=1, flags=re.I)[0]
            t = t[:m.start()] + " " + (after if len(after.split()) <= 5 else "")
            break
    t = re.sub(r"\b(?:price of|price|the|be|close|end|finish|trade|report|reach|hit|have|get|go|"
               r"of|at|on|in|by|a|an)\b", " ", t, flags=re.I)
    t = re.sub(r"[?:,]", " ", t)
    t = re.sub(r"\s+", " ", t).strip(" -")
    return t.lower() or None


def _unit_word(text):
    m = UNIT_WORDS.search(text or "")
    return m.group(1).lower() if m else None


# ---------------------------------------------------------------------------
# Kalshi
# ---------------------------------------------------------------------------

def from_kalshi(m, event):
    """Kalshi gives the comparator and levels (strike_type, floor_strike,
    cap_strike), the named outcome (custom_strike, yes_sub_title), the date
    (occurrence_datetime) and a generic question (the event title). Regex adds
    units, subjects and whether the date is 'by' or 'on'."""
    rec = _record("kalshi", m["ticker"], m.get("event_ticker"))
    title, sub = m.get("title") or "", (m.get("yes_sub_title") or "").strip()
    etitle = (event or {}).get("title") or title
    rec["topic"] = [c for c in [(event or {}).get("category")] if c]
    st = m.get("strike_type")
    custom = m.get("custom_strike") or {}
    named = {k: v for k, v in custom.items() if v and isinstance(v, str) and not UUID.match(v)}
    date_key = named.pop("Date", None)

    if st in KALSHI_NUMERIC:
        lo, hi = m.get("floor_strike"), m.get("cap_strike")
        cmp_ = st
        if st == "between" and lo is not None and lo == hi:
            cmp_ = "exactly"
        _set(rec, "native", kind="threshold", comparator=cmp_,
             low=lo if st in ("greater", "greater_or_equal", "between") else hi,
             high=hi if st == "between" else None)
        unit = number(sub)[1] or number(title)[1] or _unit_word(sub) or _unit_word(title)
        _set(rec, "regex", unit=unit, subject=_subject(etitle) or _subject(title))
    elif date_key or re.match(r"^(?:before|by|on|after)\s", sub, re.I):
        d, _, _ = date_of(date_key or sub)
        _set(rec, "native" if date_key else "regex", kind="deadline", comparator="happens",
             when=d, when_kind="on" if sub.lower().startswith("on ") else "by")
    elif named or (sub and sub.lower() not in ("yes", "no")):
        entity = next(iter(named.values()), None) or sub
        _set(rec, "native", kind="outcome", entity=entity, comparator="is")
    else:
        _set(rec, "native", kind="event", comparator="happens")

    # The date: the event's own moment, else what the wording says.
    occ = m.get("occurrence_datetime") or m.get("expected_expiration_time")
    if rec["when"] is None:
        d, _, start = date_of(title)
        if d:
            _set(rec, "regex", when=d, when_kind=when_kind(title, start))
        elif occ:
            _set(rec, "native", when=occ[:10], when_kind="on")
    rec["template"] = masked(etitle, rec["entity"])
    return rec


# ---------------------------------------------------------------------------
# Polymarket
# ---------------------------------------------------------------------------

def from_poly(m, event):
    """Polymarket gives the question, the varying part of a set of markets
    (groupItemTitle), the event's tags and whether its outcomes exclude each
    other (negRisk). Everything else is read from the question."""
    q = (m.get("question") or "").strip()
    g = (m.get("groupItemTitle") or "").strip()
    rec = _record("poly", m.get("conditionId"), str((event or {}).get("id") or ""))
    rec["topic"] = [t.get("label") for t in (event or {}).get("tags") or [] if t.get("label")]
    year_hint = (m.get("endDate") or "")[:4] or None
    try:
        outcomes = json.loads(m.get("outcomes") or "[]")
    except (TypeError, ValueError):
        outcomes = []
    clean = YEAR_RANGE.sub(lambda x: x.group(0).replace("-", "/").replace("–", "/"), q)

    if g and PLACEHOLDER.match(g.strip('"')):
        rec["placeholder"] = True
        rec["sources"]["placeholder"] = "regex"

    up = UPDOWN.match(q)
    sp = SPREAD.search(q)
    tot = TOTAL.search(q) or TOTAL.search(g)
    vs = VS.match(q)
    cmp_hit = None
    for kind, pat in COMPARATORS:
        hit = pat.search(clean)
        if hit and not EXACT_SCORE.match(q):
            cmp_hit = (kind, hit)
            break

    if up:
        _set(rec, "regex", kind="up_down", subject=up.group("subject").lower(), comparator="greater")
    elif sp:
        _set(rec, "regex", kind="spread", entity=sp.group("team"), comparator="greater",
             low=float(sp.group("line")))
    elif tot and (vs or "o/u" in q.lower()):
        _set(rec, "regex", kind="total", comparator="greater", low=float(tot.group("line")),
             unit=_unit_word(q))
    elif EXACT_SCORE.match(q):
        _set(rec, "regex", kind="outcome", entity=q.split(":", 1)[1].strip(" ?"), comparator="is")
    elif cmp_hit:
        kind, hit = cmp_hit
        if kind == "rank":
            _set(rec, "regex", kind="threshold", comparator="less_or_equal",
                 low=float(hit.group("a")), unit="rank", subject=_subject(clean[:hit.start()] or clean))
            kind = None
        elif kind == "between":
            lo, hi, unit = _range(hit.group("a"), hit.group("b"))
        else:
            lo, unit = number(hit.group("a"))
            hi = None
        if kind:
            _set(rec, "regex", kind="threshold", comparator=kind, low=lo, high=hi,
                 unit=unit or _unit_word(clean[hit.end():hit.end() + 40]) or _unit_word(clean),
                 subject=_subject(clean))
    elif vs:
        first = outcomes[0] if outcomes and outcomes[0].lower() not in ("yes", "no") else None
        _set(rec, "native" if first else "regex", kind="matchup",
             entity=first or vs.group("a"), comparator="wins")
    else:
        gd, _, _ = date_of(g, year_hint) if g else (None, None, None)
        if gd and len(g) < 30:
            # A set of markets that differ only by deadline: "Kraken IPO by ...?"
            _set(rec, "regex", kind="deadline", comparator="happens", when=gd,
                 when_kind=when_kind(q, date_of(q, year_hint)[2]) or "by")
        elif g and g.lower() not in ("yes", "no"):
            _set(rec, "native", kind="outcome", entity=g, comparator="is")
        else:
            w = re.match(r"^(?:will|is|does)\s+(.+?)\s+(?:win|be the next|be elected|become|be named|"
                         r"be the)\b", q, re.I)
            if w:
                _set(rec, "regex", kind="outcome", entity=w.group(1), comparator="is")
            else:
                _set(rec, "regex", kind="event", comparator="happens")

    if rec["when"] is None:
        d, _, start = date_of(clean, year_hint)
        if d:
            _set(rec, "regex", when=d, when_kind=when_kind(clean, start))
        elif m.get("endDate"):
            _set(rec, "native", when=m["endDate"][:10], when_kind="on")
    rec["template"] = masked(q, rec["entity"] if rec["kind"] in ("outcome", "matchup") else None)
    return rec


def tokens(m):
    """The Polymarket market's token ids, first outcome first."""
    try:
        return json.loads(m.get("clobTokenIds") or "[]")
    except ValueError:
        return []

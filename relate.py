"""How two markets relate, from their records (see propositions.py).

    label, why = relate(a, b)

The labels, strongest first:

    identical     the same question: same subject (or outcome), comparator,
                  level and timing -- one pays exactly when the other does
    complement    the same level, opposite sides: one pays exactly when the
                  other does not ("above 70k" / "below 70k")
    nested        one paying implies the other pays ("above 75k" -> "above
                  70k"; "before March" -> "before June")
    overlapping   ranges that share some values
    same_outcome  the same named outcome of the same question ("Javier
                  Milei" in "who wins Argentina's election")
    related       about the same thing, but asked differently (a different
                  date, "on" against "by", a different kind of question)
    None          not about the same thing

Embeddings only ever propose pairs; these rules decide. They need the
subjects (or outcomes) to agree, because an embedding scores "Bitcoin above
X" and "Ethereum above X" as nearly the same text.
"""

import re
import unicodedata
from datetime import date

# Names that mean the same thing, lower case, after normalising.
ALIASES = {
    "btc": "bitcoin", "xbt": "bitcoin", "eth": "ethereum", "ether": "ethereum", "sol": "solana",
    "xrp": "ripple", "doge": "dogecoin", "bnb": "binance coin", "ada": "cardano",
    "fed": "federal reserve", "fomc": "federal reserve", "the fed": "federal reserve",
    "gop": "republican", "republicans": "republican", "republican party": "republican",
    "democrats": "democratic", "democrat": "democratic", "democratic party": "democratic",
    "dems": "democratic", "us": "united states", "usa": "united states", "u.s.": "united states",
    "uk": "united kingdom", "nyc": "new york city", "spx": "s&p 500", "s&p": "s&p 500",
    "nasdaq-100": "nasdaq 100", "ndx": "nasdaq 100", "wti": "crude oil", "gdp": "gdp",
    "cpi": "inflation", "30-yr frm": "30-year mortgage rate", "30-year fixed-rate mortgage": "30-year mortgage rate",
}
STOP = {"the", "a", "an", "of", "in", "on", "by", "at", "to", "for", "will", "be", "is", "price",
        "inc", "inc.", "corp", "corporation", "company", "co", "ltd", "holdings", "party", "s"}
# Words that change what a question asks: two questions differing in any of
# these are different questions, however close their wording otherwise
# ("vice-presidential nominee" / "presidential nominee"). Matched on stems.
CRITICAL = {"vice", "not", "no", "out", "without", "first", "last", "relega", "promot", "lose",
            "loser", "resign", "impeac", "indict", "convic", "arrest", "die", "death", "ban",
            "reject", "nomine", "nomina", "primar", "runner", "second", "third", "most", "least",
            "visit", "buy", "acquir", "invade", "attack", "meet", "talk", "fire", "leave", "join",
            "senate", "house", "govern", "mayor", "presid", "minist", "chance", "speake",
            "emerge", "announ", "reusab", "dock", "launch", "orbit", "land", "confir", "elect",
            "hold", "host", "run", "return", "record", "sign", "pass", "end", "start",
            "all", "every", "monthl", "daily", "weekly", "active", "album", "single", "song"}
STEM_STOP = STOP | {"who", "what", "when", "where", "which", "how", "many", "much", "next", "any",
                    "and", "or", "with", "from", "this", "that", "than", "have", "has", "do",
                    "does", "did", "be", "been", "get", "go", "x", "n", "cmp", "date", "before",
                    "after", "until", "during", "end", "through", "year", "season"}
UP = {"greater", "greater_or_equal"}
DOWN = {"less", "less_or_equal"}


def norm(text):
    """Lower case, accents off, punctuation off, aliases applied."""
    if not text:
        return ""
    t = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    t = re.sub(r"[^\w\s&./-]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    return ALIASES.get(t, t)


def words(text):
    out = set()
    for w in norm(text).replace("/", " ").replace("-", " ").split():
        w = ALIASES.get(w, w)
        if w not in STOP and not re.fullmatch(r"[\d.,()]+", w):     # years and numbers say nothing
            out.update(w.split())
    return out


def stems(template, entity=None):
    """The content words of a question, as six-letter stems: slots, the named
    outcome, numbers and years left out."""
    t = re.sub(r"⟨[a-z]+⟩", " ", template or "")
    if entity:
        t = re.sub(re.escape(entity), " ", t, flags=re.I)
    out = set()
    for w in re.findall(r"[a-z][a-z'’]*", norm(t)):
        w = ALIASES.get(w, w).replace("’", "").replace("'", "")
        if w not in STEM_STOP and len(w) > 1:
            out.add(w[:6])
    return out


def names(template, entity=None):
    """Stems of the capitalised words after the first: the places, people and
    organisations a question names ("California", "SpaceX")."""
    t = re.sub(r"⟨[a-z]+⟩", " ", template or "")
    if entity:
        t = re.sub(re.escape(entity), " ", t, flags=re.I)
    found = re.findall(r"(?<=[\s(\"'])([A-Z][\w’'.-]+)", " " + t)
    out = set()
    for w in found[1:] if t.strip()[:1].isupper() else found:
        w = ALIASES.get(norm(w), norm(w)).replace("’", "").replace("'", "")
        if w and w not in STEM_STOP:
            out.update(x[:6] for x in w.split())
    return out


def question_similarity(a, b):
    """(share of content stems in common, do the critical words agree, do
    the named things agree). The
    named things must agree too: one question's names all appear in the
    other's words ("Claude" in "Anthropic release Claude"), so California
    never matches Alabama."""
    sa = stems(a.get("template"), a.get("entity"))
    sb = stems(b.get("template"), b.get("entity"))
    if not sa or not sb:
        return 0.0, True, True
    crit_a = {c for c in CRITICAL if any(w.startswith(c) for w in sa)}
    crit_b = {c for c in CRITICAL if any(w.startswith(c) for w in sb)}
    na = names(a.get("template"), a.get("entity"))
    nb = names(b.get("template"), b.get("entity"))
    named = na <= sb or nb <= sa
    return len(sa & sb) / len(sa | sb), crit_a == crit_b, named


def same_thing(a, b):
    """Do two subjects (or outcomes) name the same thing? Equal after
    normalising, or one's words all inside the other's ('bitcoin' and
    'bitcoin price on dec 31')."""
    if not a or not b:
        return False
    na, nb = norm(a), norm(b)
    if na == nb:
        return True
    wa, wb = words(a), words(b)
    if not wa or not wb:
        return False
    small, big = (wa, wb) if len(wa) <= len(wb) else (wb, wa)
    return small <= big and len(small) >= 1 and (len(small) >= 2 or len(big) <= 4)


def span(when):
    """(first day, last day) of a date, month, quarter or year, or None."""
    if not when or when.startswith("--"):
        return None
    try:
        if re.fullmatch(r"\d{4}", when):
            y = int(when)
            return date(y, 1, 1), date(y, 12, 31)
        if re.fullmatch(r"\d{4}-Q[1-4]", when):
            y, q = int(when[:4]), int(when[-1])
            ends = {1: date(y, 3, 31), 2: date(y, 6, 30), 3: date(y, 9, 30), 4: date(y, 12, 31)}
            return date(y, 3 * q - 2, 1), ends[q]
        if re.fullmatch(r"\d{4}-\d{2}", when):
            y, m = int(when[:4]), int(when[5:7])
            nxt = date(y + (m == 12), m % 12 + 1, 1)
            return date(y, m, 1), date.fromordinal(nxt.toordinal() - 1)
        d = date.fromisoformat(when[:10])
        return d, d
    except ValueError:
        return None


def timing(a, b):
    """'same', 'nested' (deadlines: one before the other), 'overlap' or
    'different'. A year against a day in it counts as the same: venues often
    state only the year for a question about its last day."""
    sa, sb = span(a["when"]), span(b["when"])
    ka, kb = a.get("when_kind") or "on", b.get("when_kind") or "on"
    if sa is None or sb is None:
        return "same" if a["when"] == b["when"] else "different"
    if ka == "by" and kb == "by":
        if sa[1] == sb[1]:
            return "same"
        return "nested"
    if ka != kb and "by" in (ka, kb):
        return "overlap" if sa[0] <= sb[1] and sb[0] <= sa[1] else "different"
    if sa == sb or (sa[0] <= sb[0] and sb[1] <= sa[1] and (sa[1] == sb[1])) or \
            (sb[0] <= sa[0] and sa[1] <= sb[1] and (sa[1] == sb[1])):
        return "same"
    return "overlap" if sa[0] <= sb[1] and sb[0] <= sa[1] else "different"


def _units_agree(a, b):
    ua, ub = a.get("unit"), b.get("unit")
    return ua is None or ub is None or ua == ub or {ua, ub} <= {"usd", None}


def _close(x, y):
    if x is None or y is None:
        return False
    if abs(x) == float("inf") or abs(y) == float("inf"):
        return x == y
    return abs(x - y) <= 1e-9 * max(1.0, abs(x), abs(y)) + 1e-9


def _thresholds(a, b, when):
    ca, cb = a["comparator"], b["comparator"]
    la, lb = a["low"], b["low"]
    if la is None or lb is None or not _units_agree(a, b):
        return "related", "levels or units not comparable"
    # "above 69,999.99" and "at least 70,000" are one question on whole cents.
    if ca in UP and cb in UP:
        if _close(la, lb) or abs(la - lb) <= 0.01:
            return ("identical" if when == "same" else "related",
                    "same direction and level" + ("" if when == "same" else ", different timing"))
        return ("nested" if when == "same" else "related", "same direction, different level")
    if ca in DOWN and cb in DOWN:
        if _close(la, lb) or abs(la - lb) <= 0.01:
            return ("identical" if when == "same" else "related", "same direction and level")
        return ("nested" if when == "same" else "related", "same direction, different level")
    if (ca in UP and cb in DOWN) or (ca in DOWN and cb in UP):
        exact = (ca == "greater" and cb == "less_or_equal") or (ca == "less_or_equal" and cb == "greater") \
            or (ca == "greater_or_equal" and cb == "less") or (ca == "less" and cb == "greater_or_equal")
        if _close(la, lb) and when == "same":
            return "complement", "same level, opposite sides" + ("" if exact else " (one includes the level itself)")
        return ("related" if when != "same" else "overlapping" if
                (ca in UP and la < lb) or (ca in DOWN and la > lb) else "related",
                "opposite sides, different levels")
    if ca in ("between", "exactly") or cb in ("between", "exactly"):
        ra = (la, a["high"] if ca == "between" else (float("inf") if ca in UP else la))
        rb = (lb, b["high"] if cb == "between" else (float("inf") if cb in UP else lb))
        if ca == "exactly" and cb == "exactly":
            ra = rb = None
        if ca in DOWN:
            ra = (float("-inf"), la)
        if cb in DOWN:
            rb = (float("-inf"), lb)
        if ra is None:
            pass
        elif None in ra or None in rb:
            return "related", "range not complete"
        elif _close(ra[0], rb[0]) and _close(ra[1], rb[1]):
            return ("identical" if when == "same" else "related", "same range")
        elif (ra[0] >= rb[0] and ra[1] <= rb[1]) or (rb[0] >= ra[0] and rb[1] <= ra[1]):
            return ("nested" if when == "same" else "related", "one range inside the other")
        elif ra[0] <= rb[1] and rb[0] <= ra[1]:
            return ("overlapping" if when == "same" else "related", "ranges share values")
        else:
            return "related", "ranges do not meet"
    if ca == "exactly" and cb == "exactly":
        if _close(la, lb):
            return ("identical" if when == "same" else "related", "same exact value")
        return "related", "different exact values (cannot both happen)"
    return "related", "comparators not comparable"


QUESTION_MIN = 0.34       # share of content words two questions must share
RELATED_MIN = 0.25


def relate(a, b):
    """(label, reason) for two records, or (None, reason). Strong labels need
    the two questions' wording to agree as well (question_similarity); where
    it does not, the pair is at most `related`."""
    if a.get("placeholder") or b.get("placeholder"):
        return None, "placeholder"
    q, crit, named = question_similarity(a, b)
    if not named:
        return None, "the questions name different things"
    label, why = _relate(a, b)
    if label is None:
        return None, why
    if label != "related" and (q < QUESTION_MIN or not crit):
        if q >= RELATED_MIN:
            return "related", f"{why}, but the questions are worded differently ({q:.2f}" +                 ("" if crit else ", a key word differs") + ")"
        return None, f"questions differ ({q:.2f})"
    if label == "related" and q < RELATED_MIN:
        return None, f"questions differ ({q:.2f})"
    return label, why


def _relate(a, b):
    ka, kb = a["kind"], b["kind"]
    when = timing(a, b)
    if ka == "threshold" and kb == "threshold":
        if not same_thing(a["subject"], b["subject"]):
            return None, "different subjects"
        return _thresholds(a, b, when)
    if ka in ("outcome", "matchup") and kb in ("outcome", "matchup"):
        if not same_thing(a["entity"], b["entity"]):
            return None, "different outcomes"
        # "Argentina, Uruguay, or Paraguay" is one outcome, not Paraguay alone.
        joint = re.compile(r",|\s(?:or|and|\+|&)\s")
        if (joint.search(a["entity"]) or joint.search(b["entity"])) and norm(a["entity"]) != norm(b["entity"]):
            return "related", "one outcome is a combination including the other"
        if when in ("same", "overlap"):
            return "same_outcome", "same named outcome, " + ("same timing" if when == "same" else "overlapping timing")
        return "related", "same named outcome, different timing"
    if ka == "deadline" and kb == "deadline":
        if when == "same":
            return "identical", "same deadline"
        if when == "nested":
            return "nested", "earlier deadline implies the later"
        return "related", "deadlines not comparable"
    if {ka, kb} <= {"deadline", "event"}:
        if when == "same":
            return "identical", "same question and date"
        return "related", "same question, different date"
    if ka == kb == "total" or ka == kb == "spread":
        if _close(a["low"], b["low"]) and when == "same":
            return "identical", "same line"
        return "related", "different line"
    if ka == kb == "up_down":
        return ("identical" if when == "same" else "related"), "same window" if when == "same" else "different window"
    # Different kinds of question: related only if about the same thing.
    if same_thing(a.get("subject") or a.get("entity"), b.get("subject") or b.get("entity")):
        return "related", f"{ka} against {kb}, same subject"
    return None, f"{ka} against {kb}"

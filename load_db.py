"""Load the scraped files into PostgreSQL.

Run the scrapers first, then:

    python load_db.py --schema     # create the tables (safe to re-run)
    python load_db.py              # load whatever files are present

Every insert is ON CONFLICT DO NOTHING, so re-running is safe and overlapping
scrapes do not duplicate rows.

On the server, markets and their candles are written by live.py, which follows
the series -> event -> market chain the schema enforces. This loader only
handles what live.py does not collect: the series list and perpetual futures.
The functions for the old market files are kept for the derivations other
modules import from here.
"""

import argparse
import json
import re
from datetime import date, datetime
from pathlib import Path

import pyarrow.parquet as pq
from psycopg2.extras import Json, execute_values

import paths
from db import connection


def rows_from_jsonl(path):
    with open(path, encoding="utf-8") as f:
        for line in f:
            yield json.loads(line)


def num(value):
    """Kalshi sends numbers as strings; missing ones as None or ''."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


# Which coin a ticker is about. Kalshi has no asset field, but the ticker
# format is fixed: an optional KX prefix followed by the symbol.
_ALIASES = {"BITCOIN": "BTC", "ETHEREUM": "ETH", "RIPPLE": "XRP",
            "SOLANA": "SOL", "SHIBA": "SHIB", "KSHIB": "SHIB", "DOGECOIN": "DOGE"}
_SYMBOLS = sorted(
    ["BITCOIN", "ETHEREUM", "DOGECOIN", "SOLANA", "RIPPLE", "SHIBA", "KSHIB",
     "AVAX", "DOGE", "HBAR", "LINK", "NEAR", "SHIB", "XLM", "ADA", "BCH", "BNB",
     "BTC", "DOT", "ETH", "HYPE", "LTC", "SOL", "SUI", "TON", "XRP", "ZEC"],
    key=len, reverse=True)  # longest first, so BITCOIN wins over BTC


# A shared taxonomy so the same question shape can be lined up across venues.
#   ABOVE   price at or above a strike        BELOW  price at or below a strike
#   RANGE   price between two strikes         ATH    new all-time high
#   UPDOWN  short-horizon direction call      OTHER  everything else
def kalshi_type(market):
    """Kalshi states this structurally, so read it rather than parse prose."""
    title = (market.get("title") or "").lower()
    strike_type = market.get("strike_type")

    if "up in next" in title or "down in next" in title:
        return "UPDOWN", num(market.get("floor_strike"))
    if strike_type in ("greater", "greater_or_equal"):
        return "ABOVE", num(market.get("floor_strike"))
    if strike_type == "less":
        return "BELOW", num(market.get("cap_strike"))
    if strike_type == "between":
        return "RANGE", num(market.get("floor_strike"))
    return "OTHER", None


# The (?![a-z]) matters: without it the "b" in "$1,000,000 by December" reads
# as a billions suffix.
_MONEY = re.compile(r"\$\s*([\d,]+(?:\.\d+)?)\s*([kmb](?![a-z]))?", re.I)


def poly_strike(text):
    """First dollar amount in the question. Handles $150k / $1.5m shorthand."""
    match = _MONEY.search(text or "")
    if not match:
        return None
    value = float(match.group(1).replace(",", ""))
    return value * {"k": 1e3, "m": 1e6, "b": 1e9}.get((match.group(2) or "").lower(), 1)


_MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], start=1)}
_MONTHS.update({m[:3]: i for m, i in list(_MONTHS.items())})
_DATED = re.compile(
    r"\b(?:on|by|before|through|until)?\s*"
    r"(" + "|".join(sorted(_MONTHS, key=len, reverse=True)) + r")\.?\s+"
    r"(\d{1,2})(?:\s*-\s*(\d{1,2}))?"
    r"(?:,?\s*(\d{4}))?", re.I)


def poly_deadline(question, end_date):
    """The date the question itself names, which is not always end_date.

    "Will Bitcoin reach $73,000 on August 20?" resolves on the 21st but is a
    question about the 20th -- and the 20th is what lines up with a Kalshi
    market. Returns None when the question names no date.
    """
    found = list(_DATED.finditer(question or ""))
    if not found:
        return None
    # Last date wins: "from August 14 to August 21" means the 21st.
    match = found[-1]
    month = _MONTHS[match.group(1).lower()]
    day = int(match.group(3) or match.group(2))

    fallback = None
    if end_date:
        try:
            fallback = datetime.fromisoformat(str(end_date).replace("Z", "+00:00"))
        except ValueError:
            fallback = None
    year = int(match.group(4)) if match.group(4) else (fallback.year if fallback else None)
    if year is None:
        return None

    try:
        stated = date(year, month, day)
    except ValueError:
        return None
    # No year in the text and the date lands long before resolution: the
    # question means next year's occurrence.
    if not match.group(4) and fallback and (fallback.date() - stated).days > 300:
        try:
            stated = date(year + 1, month, day)
        except ValueError:
            pass
    return stated


def poly_type(question):
    """Polymarket only has prose, so this classifies on wording."""
    q = (question or "").lower()
    strike = poly_strike(q)

    if "all time high" in q or "all-time high" in q or "ath" in q.split():
        return "ATH", strike
    if "up or down" in q:
        return "UPDOWN", strike
    if any(w in q for w in ("dip to", "fall to", "drop to", "below", "under")):
        return "BELOW", strike
    if any(w in q for w in ("reach", "hit", "above", "over", "exceed")):
        return "ABOVE", strike
    return "OTHER", strike


def asset_of(ticker):
    name = (ticker or "").upper()
    if name.startswith("KX"):
        name = name[2:]
    for symbol in _SYMBOLS:
        if name.startswith(symbol):
            return _ALIASES.get(symbol, symbol)
    return "OTHER"


def insert(cur, table, columns, rows, key=None):
    """Bulk insert.

    Candles are immutable, so a clash means we already have the row -- skip it.
    Dimension rows change (status, result, volume, and new columns like asset),
    so pass `key` to refresh them instead.
    """
    if not rows:
        return 0
    if key:
        sets = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c != key)
        conflict = f"ON CONFLICT ({key}) DO UPDATE SET {sets}"
    else:
        conflict = "ON CONFLICT DO NOTHING"
    sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s {conflict}"
    execute_values(cur, sql, rows, page_size=1000)
    return len(rows)


# One category vocabulary for both venues. Kalshi states a category per series;
# Polymarket only tags events, so the first tag that maps here wins.
CATEGORIES = {
    "crypto": "Crypto", "bitcoin": "Crypto", "ethereum": "Crypto",
    "politics": "Politics", "elections": "Politics", "trump": "Politics",
    "geopolitics": "World", "world": "World", "ukraine": "World", "nato": "World",
    "sports": "Sports", "nfl": "Sports", "nba": "Sports", "mlb": "Sports",
    "soccer": "Sports", "tennis": "Sports", "epl": "Sports", "ufc": "Sports",
    "economics": "Economics", "inflation": "Economics", "fed": "Economics",
    "finance": "Financials", "stocks": "Financials", "business": "Companies",
    "tech": "Science and Technology", "ai": "Science and Technology",
    "science": "Science and Technology", "space": "Science and Technology",
    "pop-culture": "Entertainment", "movies": "Entertainment",
    "entertainment": "Entertainment", "music": "Entertainment",
    "weather": "Climate and Weather", "climate": "Climate and Weather",
    "health": "Health", "commodities": "Commodities", "oil": "Commodities",
}


def category_from_tags(tags):
    for tag in tags or []:
        if tag in CATEGORIES:
            return CATEGORIES[tag]
    return (tags or [None])[0] and (tags[0] or "").replace("-", " ").title()


def series_categories():
    """series_ticker -> category, read once from the series file."""
    out = {}
    if paths.SERIES.exists():
        for row in rows_from_jsonl(paths.SERIES):
            out[row.get("ticker")] = row.get("category")
    return out


def load_poly_events(cur, path):
    """Polymarket's events -- the counterpart of a Kalshi series."""
    events = {}
    for m in rows_from_jsonl(path):
        eid = m.get("event_id")
        if not eid:
            continue
        embedded = (m.get("events") or [{}])[0]
        entry = events.setdefault(eid, {
            "slug": m.get("event_slug"),
            "title": embedded.get("title"),
            "tags": m.get("tags") or [],
            "count": 0,
        })
        entry["count"] += 1

    rows = [(eid, e["slug"], e["title"], category_from_tags(e["tags"]),
             e["tags"], e["count"]) for eid, e in events.items()]
    columns = ["event_id", "slug", "title", "category", "tags", "market_count"]
    return insert(cur, "poly_events", columns, rows, key="event_id")


def load_series(cur, path):
    rows = [(s["ticker"], s.get("category"), s.get("title"), s.get("frequency"))
            for s in rows_from_jsonl(path)]
    return insert(cur, "kalshi_series", ["series_ticker", "category", "title", "frequency"], rows,
                  key="series_ticker")


def load_markets(cur, path):
    categories = series_categories()
    rows = []
    for m in rows_from_jsonl(path):
        kind, strike = kalshi_type(m)
        rows.append((
            m["ticker"],
            m.get("series_ticker"),
            asset_of(m.get("series_ticker") or m["ticker"]),
            categories.get(m.get("series_ticker")),
            kind,
            strike,
            m.get("event_ticker"),
            m.get("title"),
            m.get("status"),
            m.get("result"),
            m.get("open_time"),
            m.get("close_time"),
            num(m.get("floor_strike")),
            num(m.get("cap_strike")),
            num(m.get("volume_fp")),
            num(m.get("open_interest_fp")),
            Json(m),
        ))
    columns = ["ticker", "series_ticker", "asset", "category",
               "prediction_type", "strike",
               "event_ticker", "title", "status",
               "result", "open_time", "close_time", "floor_strike", "cap_strike",
               "volume", "open_interest", "raw"]
    return insert(cur, "kalshi_markets", columns, rows, key="ticker")


def load_perp_markets(cur, path):
    rows = [(m["ticker"], num(m.get("contract_size")),
             num(m.get("tick_size")), m.get("status"), Json(m))
            for m in rows_from_jsonl(path)]
    columns = ["ticker", "contract_size", "tick_size", "status", "raw"]
    return insert(cur, "kalshi_perp_markets", columns, rows, key="ticker")


def load_poly_markets(cur, path):
    rows = []
    for m in rows_from_jsonl(path):
        tokens = m.get("clobTokenIds")
        try:
            yes_token = json.loads(tokens)[0] if tokens else None
        except (ValueError, IndexError):
            yes_token = None
        kind, strike = poly_type(m.get("question"))
        rows.append((
            m["conditionId"], m.get("slug"), m.get("question"), m.get("asset"),
            kind, strike, poly_deadline(m.get("question"), m.get("endDate")),
            m.get("event_slug"), m.get("event_id"), m.get("tags") or [],
            category_from_tags(m.get("tags")),
            yes_token, m.get("endDate"), m.get("closed"),
            num(m.get("volume")), num(m.get("liquidity")), Json(m),
        ))
    columns = ["condition_id", "slug", "question", "asset",
               "prediction_type", "strike", "stated_date", "event_slug",
               "event_id", "tags", "category",
               "yes_token_id", "end_date", "closed", "volume", "liquidity", "raw"]
    return insert(cur, "poly_markets", columns, rows, key="condition_id")


def load_poly_candles(cur, path):
    """Same part-file handling as load_candles, different key column."""
    loaded_path = paths.state(f"{path.name}.loaded")
    loaded = set(loaded_path.read_text().split()) if loaded_path.exists() else set()
    columns = ["condition_id", "ts", "open", "high", "low", "close", "volume"]

    total = 0
    for file in sorted(path.glob("part-*.parquet")):
        if file.name in loaded:
            continue
        try:
            data = pq.read_table(file).to_pylist()
        except Exception:
            continue  # still being written
        rows = [(c["condition_id"], c["ts"], c["open"], c["high"], c["low"],
                 c["close"], c["volume"]) for c in data]
        total += insert(cur, "poly_candles", columns, rows)
        with open(loaded_path, "a") as f:
            f.write(file.name + "\n")
    return total


def load_candles(cur, path, table):
    """Load one Parquet file, or a directory of part files from the streaming
    scraper.

    Part files never change once written, so a sidecar records which ones are
    already in the database. That keeps repeated runs cheap while a download is
    still in progress.
    """
    columns = ["ticker", "ts", "open", "high", "low", "close", "volume", "open_interest"]

    if not path.is_dir():
        data = pq.read_table(path).to_pylist()
        rows = [(c["ticker"], c["ts"], c["open"], c["high"], c["low"], c["close"],
                 c["volume"], c["open_interest"]) for c in data]
        return insert(cur, table, columns, rows)

    loaded_path = paths.state(f"{path.name}.loaded")
    loaded = set(loaded_path.read_text().split()) if loaded_path.exists() else set()

    total = 0
    for file in sorted(path.glob("part-*.parquet")):
        if file.name in loaded:
            continue
        try:
            data = pq.read_table(file).to_pylist()
        except Exception:
            continue  # still being written; pick it up next run

        rows = [(c["ticker"], c["ts"], c["open"], c["high"], c["low"], c["close"],
                 c["volume"], c["open_interest"]) for c in data]
        total += insert(cur, table, columns, rows)
        with open(loaded_path, "a") as f:
            f.write(file.name + "\n")
    return total


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", action="store_true", help="create tables and exit")
    args = parser.parse_args()

    with connection() as conn:
        with conn.cursor() as cur:
            if args.schema:
                cur.execute(paths.SCHEMA.read_text(encoding="utf-8"))
                print("schema created")
                return

            # Order matters: candles have foreign keys into the market tables.
            jobs = [
                (paths.SERIES, lambda p: load_series(cur, p)),
                (paths.PERP_MARKETS, lambda p: load_perp_markets(cur, p)),
                (paths.PERP_CANDLES, lambda p: load_candles(cur, p, "kalshi_perp_candles")),
            ]

            for path, loader in jobs:
                filename = path.name
                if not path.exists():
                    print(f"{filename:<22} skipped (not found)")
                    continue
                count = loader(path)
                print(f"{filename:<22} {count} rows offered")


if __name__ == "__main__":
    main()

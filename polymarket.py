"""Download Polymarket crypto markets and 1-minute candles.

Mirrors poll_predictions_stream.py: pass 1 lists the markets worth having,
pass 2 fetches their price history, flushing a Parquet part every --batch
markets so a crash never loses more than one batch.

Prices are 0-1 = implied probability, same as Kalshi. Candles come from
/prices-history (one request per market) rather than raw trades, which would
cost hundreds of requests per market. The trade-off is that volume is not
available per candle -- see the note on `volume` below.

    python poll_polymarket.py                      # past 7 days -> poly/
    python poll_polymarket.py --days 30 --min-volume 50000
    python poll_polymarket.py --list-only          # just the market list
"""

import argparse
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

import paths

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
CRYPTO_TAG = 21

SCHEMA = pa.schema([
    ("condition_id", pa.string()),
    ("asset", pa.string()),
    ("ts", pa.timestamp("us", tz="UTC")),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    # Always null: /prices-history carries no size. Kept so this table lines up
    # with the Kalshi candle tables.
    ("volume", pa.float64()),
])

# Polymarket tags its events, and the tag vocabulary includes the coin. This is
# the reliable signal: structured data rather than guessing from prose.
TAG_ASSETS = {
    "bitcoin": "BTC", "btc": "BTC", "bitcoin-cash": "BCH",
    "ethereum": "ETH", "eth": "ETH", "solana": "SOL", "sol": "SOL",
    "xrp": "XRP", "ripple": "XRP", "dogecoin": "DOGE", "doge": "DOGE",
    "cardano": "ADA", "chainlink": "LINK", "avalanche": "AVAX",
    "polkadot": "DOT", "litecoin": "LTC", "shiba-inu": "SHIB", "shiba": "SHIB",
    "hyperliquid": "HYPE", "sui": "SUI", "toncoin": "TON", "zcash": "ZEC",
    "stellar": "XLM", "near": "NEAR", "binance-coin": "BNB", "bnb": "BNB",
}

# Fallback only, for markets whose event carries no coin tag. Questions are free
# text, so this matches on words. Many crypto markets are about the sector, not
# one coin ("Kraken IPO", "Pump.fun airdrop"); those are OTHER on purpose.
COINS = {
    "BTC": ["bitcoin", "btc"], "ETH": ["ethereum", "ether", "eth"],
    "SOL": ["solana", "sol"], "XRP": ["xrp", "ripple"], "DOGE": ["dogecoin", "doge"],
    "BNB": ["binance coin", "bnb"], "ADA": ["cardano", "ada"],
    "LINK": ["chainlink", "link"], "AVAX": ["avalanche", "avax"],
    "DOT": ["polkadot", "dot"], "LTC": ["litecoin", "ltc"],
    "SHIB": ["shiba", "shib"], "HYPE": ["hyperliquid", "hype"],
    "SUI": ["sui"], "TON": ["toncoin", "ton"], "ZEC": ["zcash", "zec"],
    "XLM": ["stellar", "xlm"], "NEAR": ["near protocol"], "BCH": ["bitcoin cash", "bch"],
}


def get(session, url, params=None):
    """GET with retries."""
    for attempt in range(6):
        try:
            response = session.get(url, params=params, timeout=40)
            if response.status_code < 400:
                return response.json()
            if response.status_code != 429 and response.status_code < 500:
                raise RuntimeError(f"{url}: HTTP {response.status_code}")
        except requests.RequestException:
            if attempt == 5:
                raise
        time.sleep(2 ** attempt)
    raise RuntimeError(f"{url}: gave up")


def asset_of(*texts):
    blob = re.sub(r"[^a-z0-9 ]+", " ", " ".join(t for t in texts if t).lower())
    words = set(blob.split())
    best = None
    for coin, keys in COINS.items():
        for key in keys:
            hit = key in blob if " " in key else key in words
            if hit and (best is None or len(key) > best[1]):
                best = (coin, len(key))
    return best[0] if best else "OTHER"


def get_event_tags(session, event_ids, chunk=20):
    """{event_id: [tag slugs]}. Tags are not returned on /markets, only /events."""
    tags = {}
    ids = sorted(set(i for i in event_ids if i))
    for start in range(0, len(ids), chunk):
        batch = get(session, f"{GAMMA}/events", {"id": ids[start:start + chunk]})
        for event in batch or []:
            tags[str(event.get("id"))] = [t.get("slug") for t in (event.get("tags") or [])]
        time.sleep(0.2)
    return tags


def asset_from_tags(slugs):
    """Coin from the event's tags, or None if none of them name a coin."""
    found = {TAG_ASSETS[s] for s in slugs or [] if s in TAG_ASSETS}
    if not found:
        return None
    # Two coins tagged means the market is about a pair; label it as such
    # rather than silently picking one.
    return "-".join(sorted(found)) if len(found) > 1 else found.pop()


def _page(session, params):
    """Walk one filtered query until Gamma's offset cap or the data runs out."""
    out, offset = [], 0
    while offset < 1000:  # Gamma refuses deeper paging
        batch = get(session, f"{GAMMA}/markets", dict(params, limit=100, offset=offset))
        if not batch:
            break
        out += batch
        offset += 100
        time.sleep(0.15)
    return out


def get_markets(session, lo, min_volume, tag=None, forward_days=400):
    """Markets that ended inside the window or have not ended yet.

    Gamma stops paging at offset 1000, which is well short of every sector's
    markets. Slicing the query into end-date buckets keeps each slice under
    that ceiling: a day at a time across the lookback window, then widening
    buckets into the future where markets thin out.
    """
    start = datetime.fromtimestamp(lo, timezone.utc)
    today = datetime.now(timezone.utc)

    edges = []
    day = start
    while day < today:                      # recent past: one bucket per day
        edges.append((day, day + timedelta(days=1)))
        day += timedelta(days=1)
    ahead, step = today, 3
    while ahead < today + timedelta(days=forward_days):   # future: widening
        edges.append((ahead, ahead + timedelta(days=step)))
        ahead += timedelta(days=step)
        step = min(step * 2, 90)

    base = {"volume_num_min": min_volume, "order": "volumeNum", "ascending": "false"}
    if tag:
        base["tag_id"] = tag

    seen, markets = set(), []
    for lo_edge, hi_edge in edges:
        batch = _page(session, dict(
            base,
            end_date_min=lo_edge.strftime("%Y-%m-%dT%H:%M:%SZ"),
            end_date_max=hi_edge.strftime("%Y-%m-%dT%H:%M:%SZ"),
        ))
        for market in batch:
            cid = market.get("conditionId")
            if not cid or cid in seen:
                continue
            seen.add(cid)
            event = (market.get("events") or [{}])[0]
            market["event_id"] = str(event.get("id") or "")
            market["event_slug"] = event.get("slug")
            markets.append(market)
        print(f"  {lo_edge:%Y-%m-%d} -> {len(markets)} markets")

    # Tags come from a second call, then fall back to text for anything untagged.
    print(f"  fetching tags for {len(set(m['event_id'] for m in markets))} events")
    tags = get_event_tags(session, [m["event_id"] for m in markets])

    tagged = 0
    for market in markets:
        slugs = tags.get(market["event_id"], [])
        market["tags"] = slugs
        asset = asset_from_tags(slugs)
        if asset:
            tagged += 1
        else:
            asset = asset_of(market.get("question"), market.get("slug"))
        market["asset"] = asset

    print(f"  asset from tags: {tagged}/{len(markets)}, rest from text")
    return markets


def yes_token(market):
    """The Yes-side token id. Its price is the probability."""
    try:
        return json.loads(market["clobTokenIds"])[0]
    except (KeyError, ValueError, IndexError):
        return None


def get_candles(session, market, lo, hi):
    """1-minute candles from the price series.

    /prices-history returns roughly one point per minute, so most buckets hold
    a single point and open==high==low==close. Buckets with several trades get
    a real range.
    """
    token = yes_token(market)
    if not token:
        return []

    data = get(session, f"{CLOB}/prices-history",
               {"market": token, "startTs": lo, "endTs": hi, "fidelity": 1})

    buckets = {}
    for point in data.get("history") or []:
        minute = point["t"] - (point["t"] % 60)
        buckets.setdefault(minute, []).append(float(point["p"]))

    rows = []
    for minute in sorted(buckets):
        prices = buckets[minute]
        rows.append({
            "condition_id": market["conditionId"],
            "asset": market["asset"],
            "ts": datetime.fromtimestamp(minute, timezone.utc),
            "open": prices[0],
            "high": max(prices),
            "low": min(prices),
            "close": prices[-1],
            "volume": None,
        })
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--min-volume", type=float, default=10000)
    parser.add_argument("--out", default=str(paths.POLY_CANDLES),
                        help="output directory for Parquet parts")
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--list-only", action="store_true")
    parser.add_argument("--tag", default="crypto",
                        help="'crypto' for the crypto tag, or 'all' for every sector")
    args = parser.parse_args()

    paths.ensure()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    done_path = paths.state("poly_candles.done")
    done = set(done_path.read_text().split()) if done_path.exists() else set()

    now = datetime.now(timezone.utc)
    lo = int((now - timedelta(days=args.days)).timestamp())
    hi = int(now.timestamp())

    session = requests.Session()

    markets_path = paths.POLY_MARKETS
    if markets_path.exists() and done:
        markets = [json.loads(l) for l in open(markets_path, encoding="utf-8")]
        print(f"{len(markets)} markets from {markets_path}")
    else:
        tag = None if args.tag == "all" else CRYPTO_TAG
        print(f"scanning {args.tag} markets, volume >= {args.min_volume:,.0f}")
        markets = get_markets(session, lo, args.min_volume, tag=tag)
        with open(markets_path, "w", encoding="utf-8") as out:
            for market in markets:
                out.write(json.dumps(market) + "\n")
        print(f"\n{len(markets)} markets -> {markets_path}")

    if args.list_only:
        return

    todo = [m for m in markets if m["conditionId"] not in done]
    part = len(list(out_dir.glob("part-*.parquet")))
    print(f"{len(todo)} markets to fetch ({len(done)} done), resuming at part {part}")

    rows, batch_ids, total = [], [], 0
    for index, market in enumerate(todo, start=1):
        rows += get_candles(session, market, lo, hi)
        batch_ids.append(market["conditionId"])
        time.sleep(0.2)

        if len(batch_ids) >= args.batch:
            pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA),
                           out_dir / f"part-{part:05d}.parquet", compression="zstd")
            with open(done_path, "a") as f:
                f.write("\n".join(batch_ids) + "\n")
            total += len(rows)
            part += 1
            print(f"{index}/{len(todo)} markets, {total} candles (part {part})")
            rows, batch_ids = [], []

    if batch_ids:
        pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA),
                       out_dir / f"part-{part:05d}.parquet", compression="zstd")
        with open(done_path, "a") as f:
            f.write("\n".join(batch_ids) + "\n")
        total += len(rows)

    print(f"wrote {total} candles to {out_dir}/")


if __name__ == "__main__":
    main()

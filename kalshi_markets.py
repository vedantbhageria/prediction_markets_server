
import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

import paths

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
MAX_CANDLES = 4000  # the API rejects windows holding much more than this


def get(session, path, params=None, attempts=14):
    """GET with retries.

    Kalshi drops connections often, and this machine occasionally loses DNS
    for a stretch. Backoff caps at a minute and keeps trying for ~10 minutes
    rather than abandoning a long walk over a brief outage.
    """
    for attempt in range(attempts):
        try:
            response = session.get(BASE_URL + path, params=params, timeout=30)
            if response.status_code < 400:
                return response.json()
            if response.status_code != 429 and response.status_code < 500:
                raise RuntimeError(f"{path}: HTTP {response.status_code}")
        except requests.RequestException as exc:
            if attempt == attempts - 1:
                raise
            if attempt and attempt % 5 == 0:
                print(f"  retrying after {type(exc).__name__} ({attempt})")
        time.sleep(min(2 ** attempt, 60))
    raise RuntimeError(f"{path}: gave up")


def get_markets(session, series, lo, hi, min_volume):
    """Markets in one series that closed between lo and hi and traded enough."""
    params = {"limit": 1000, "series_ticker": series, "min_close_ts": lo, "max_close_ts": hi}
    markets = []
    cursor = None

    while True:
        if cursor:
            params["cursor"] = cursor
        data = get(session, "/markets", params)
        page = data.get("markets") or []
        if not page:
            break

        markets += [m for m in page if float(m.get("volume_fp") or 0) >= min_volume]

        cursor = data.get("cursor")
        if not cursor:
            break
        time.sleep(0.2)

    return markets


def get_all_markets(session, lo, hi, min_volume, out_path, resume=False):
    """Every market closing in [lo, hi], across all series, in one walk.

    Kalshi has 13k+ series, so asking series by series costs tens of thousands
    of requests. Paging /markets over a close-time window covers the same
    ground in a few hundred.

    Each page is appended to disk and the cursor saved beside it, so a dropped
    connection costs one page instead of the whole run. Re-run with --resume to
    carry on from the saved cursor.
    """
    out_path = Path(out_path)
    cursor_path = paths.state("markets.cursor")
    cursor = cursor_path.read_text().strip() if (resume and cursor_path.exists()) else None
    kept = sum(1 for _ in open(out_path, encoding="utf-8")) if (resume and out_path.exists()) else 0

    params = {"limit": 1000, "min_close_ts": lo, "max_close_ts": hi}
    seen, pages = set(), 0

    with open(out_path, "a" if resume else "w", encoding="utf-8") as out:
        while True:
            if cursor:
                params["cursor"] = cursor
            data = get(session, "/markets", params)
            page = data.get("markets") or []
            if not page:
                break

            for market in page:
                if float(market.get("volume_fp") or 0) >= min_volume:
                    # No series field on the payload; the ticker starts with it.
                    market["series_ticker"] = market["ticker"].split("-")[0]
                    out.write(json.dumps(market) + "\n")
                    kept += 1
            out.flush()

            cursor = data.get("cursor")
            if cursor:
                cursor_path.write_text(cursor)

            pages += 1
            if pages % 25 == 0:
                print(f"  {pages} pages this run, {kept} kept total")

            if not cursor or cursor in seen:
                break
            seen.add(cursor)
            time.sleep(0.15)

    return kept


def get_candles(session, series, ticker, lo, hi, interval):
    """Candles for one market. Prices are dollars 0-1 = implied probability.

    The API rejects a window holding more than ~5000 candles, so ask in chunks.
    """
    rows = []
    chunk = MAX_CANDLES * interval * 60
    start = lo

    while start < hi:
        end = min(start + chunk, hi)
        params = {"start_ts": start, "end_ts": end, "period_interval": interval}
        data = get(session, f"/series/{series}/markets/{ticker}/candlesticks", params)

        for candle in data.get("candlesticks") or []:
            price = candle.get("price") or {}
            bid = candle.get("yes_bid") or {}
            ask = candle.get("yes_ask") or {}

            if price.get("close_dollars") is not None:
                o, h, l, c = (float(price["open_dollars"]), float(price["high_dollars"]),
                              float(price["low_dollars"]), float(price["close_dollars"]))
            elif bid.get("close_dollars") is not None and ask.get("close_dollars") is not None:
                # Nobody traded this minute, but the book was quoted. Thin
                # markets are mostly these -- dropping them leaves a market
                # looking like it has ten candles in three weeks. Use the mid,
                # with volume 0 to mark that nothing changed hands.
                def mid(field):
                    return (float(bid[field]) + float(ask[field])) / 2
                o, h, l, c = mid("open_dollars"), mid("high_dollars"),                              mid("low_dollars"), mid("close_dollars")
            else:
                continue  # no trade and no quote

            rows.append({
                "ticker": ticker,
                "series": series,
                # Kalshi stamps a candle with the END of its period; stored
                # candles use the start, the same as Polymarket's.
                "ts": datetime.fromtimestamp(candle["end_period_ts"] - interval * 60,
                                             timezone.utc),
                "open": o, "high": h, "low": l, "close": c,
                "volume": float(candle.get("volume_fp") or 0),
                "open_interest": float(candle.get("open_interest_fp") or 0),
            })
        start = end

    return rows


def market_window(market, lo, hi):
    """Clamp the fetch window to the market's own life.

    These markets often live 15 minutes, so asking for the whole lookback
    period per market wastes requests and can exceed the candle limit.
    """
    def ts(field, default):
        value = market.get(field)
        if not value:
            return default
        return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())

    # +120s on the close: the last candle is stamped at the end of its minute,
    # so it can fall just after close_time and be missed otherwise.
    return max(ts("open_time", lo), lo), min(ts("close_time", hi) + 120, hi)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--min-volume", type=float, default=100,
                        help="skip markets that traded less than this")
    parser.add_argument("--forward", type=int, default=30,
                        help="also keep markets closing this many days ahead")
    parser.add_argument("--category", default="Crypto",
                        help="Kalshi category, or 'all' for every sector")
    parser.add_argument("--resume", action="store_true",
                        help="with --category all, continue from the saved cursor")
    args = parser.parse_args()

    paths.ensure()
    now = datetime.now(timezone.utc)
    lo = int((now - timedelta(days=args.days)).timestamp())
    # Markets are listed over a window that runs into the future, so open
    # markets are included alongside ones that already settled. Candles still
    # stop at `hi` -- the future has no price history.
    hi_markets = int((now + timedelta(days=args.forward)).timestamp())

    session = requests.Session()

    params = {} if args.category == "all" else {"category": args.category}
    series_list = get(session, "/series", params)["series"]
    print(f"{len(series_list)} series ({args.category})")

    with open(paths.SERIES, "w", encoding="utf-8") as out:
        for series in series_list:
            out.write(json.dumps(series) + "\n")

    # Pass 1: which markets are worth pulling candles for.
    if args.category == "all":
        # Writes markets.jsonl itself, page by page.
        total = get_all_markets(session, lo, hi_markets, args.min_volume,
                                paths.MARKETS_ALL, resume=args.resume)
        print(f"\n{total} markets over volume {args.min_volume} "
              f"-> {paths.MARKETS_ALL.name}")
        return
    else:
        wanted = sorted(s["ticker"] for s in series_list)
        markets = []
        for series in wanted:
            found = get_markets(session, series, lo, hi_markets, args.min_volume)
            markets += [(series, m) for m in found]
            if found:
                print(f"{series:<24} {len(found)}")

    with open(paths.MARKETS, "w", encoding="utf-8") as out:
        for series, market in markets:
            # The payload has no series field -- add it so the loader
            # doesn't have to parse it back out of the ticker.
            market["series_ticker"] = series
            out.write(json.dumps(market) + "\n")
    print(f"\n{len(markets)} markets over volume {args.min_volume} -> markets.jsonl")

if __name__ == "__main__":
    main()

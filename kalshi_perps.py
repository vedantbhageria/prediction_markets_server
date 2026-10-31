"""Download Kalshi perpetual futures candles from the past month to Parquet.

    python poll_perps.py
    python poll_perps.py --days 7 --interval 60
"""

import argparse
import json
import time
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import requests

import paths

BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
MAX_CANDLES = 4000  # the API rejects requests spanning much more than this


def get(session, path, params=None):
    """GET with retries -- Kalshi drops connections fairly often."""
    for attempt in range(8):
        try:
            response = session.get(BASE_URL + path, params=params, timeout=30)
            if response.status_code < 400:
                return response.json()
            if response.status_code != 429 and response.status_code < 500:
                raise RuntimeError(f"{path}: HTTP {response.status_code}")
        except requests.RequestException:
            if attempt == 7:
                raise
        time.sleep(2 ** attempt)
    raise RuntimeError(f"{path}: gave up")


def get_candles(session, ticker, lo, hi, interval):
    """Candles for one perp, in chunks small enough for the API to accept."""
    rows = []
    chunk = MAX_CANDLES * interval * 60
    start = lo

    while start < hi:
        end = min(start + chunk, hi)
        params = {"start_ts": start, "end_ts": end, "period_interval": interval}
        data = get(session, f"/margin/markets/{ticker}/candlesticks", params)

        for candle in data.get("candlesticks") or []:
            price = candle.get("price") or {}
            # Markets that are listed but not trading yet return nulls.
            num = lambda value: float(value) if value is not None else None
            rows.append({
                "ticker": ticker,
                # Stamped with the start of the period, like every stored candle.
                "ts": datetime.fromtimestamp(candle["end_period_ts"] - interval * 60, timezone.utc),
                "open": num(price.get("open")),
                "high": num(price.get("high")),
                "low": num(price.get("low")),
                "close": num(price.get("close")),
                "volume": num(candle.get("volume")),
                "open_interest": num(candle.get("open_interest")),
            })

        start = end
        time.sleep(0.2)

    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--interval", type=int, default=1, help="candle minutes")
    parser.add_argument("--out", default=str(paths.PERP_CANDLES))
    args = parser.parse_args()

    now = datetime.now(timezone.utc)
    lo = int((now - timedelta(days=args.days)).timestamp())
    hi = int(now.timestamp())

    session = requests.Session()
    markets = get(session, "/margin/markets")["markets"]
    tickers = sorted(m["ticker"] for m in markets)

    # contract_size lives here and nowhere else -- without it the candle
    # prices can't be converted back to the underlying.
    paths.ensure()
    with open(paths.PERP_MARKETS, "w", encoding="utf-8") as out:
        for market in markets:
            out.write(json.dumps(market) + "\n")

    all_rows = []
    for ticker in tickers:
        rows = get_candles(session, ticker, lo, hi, args.interval)
        all_rows += rows
        print(f"{ticker:<14} {len(rows)}")

    pq.write_table(pa.Table.from_pylist(all_rows), args.out, compression="zstd")
    print(f"wrote {len(all_rows)} candles to {args.out}")


if __name__ == "__main__":
    main()

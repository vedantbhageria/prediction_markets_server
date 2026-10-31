"""Step 2: fetch candles for markets in the catalogue.

One request per market, so this is the expensive half. It runs the requests
concurrently (the wait dominates, not the work), writes a Parquet part every
--batch markets, and records finished tickers so a restart resumes rather than
starting over.

Needs data/markets.jsonl -- build it with kalshi_markets.py first.

    python kalshi_candles.py --min-volume 10000     # a useful subset
    python kalshi_candles.py --workers 20           # push closer to the rate cap
    python kalshi_candles.py                        # re-run to resume
"""

import argparse
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import requests

import paths
# The fetching logic lives with the catalogue builder; only the writing differs.
from kalshi_markets import get_candles, market_window

from datetime import datetime, timedelta, timezone

# Explicit so every part file has an identical schema and the directory reads
# back as one dataset.
SCHEMA = pa.schema([
    ("ticker", pa.string()),
    ("series", pa.string()),
    ("ts", pa.timestamp("us", tz="UTC")),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("volume", pa.float64()),
    ("open_interest", pa.float64()),
])


def flush(rows, out_dir, part):
    """Write one part file. Empty batches still advance the counter."""
    if not rows:
        return
    path = out_dir / f"part-{part:05d}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path, compression="zstd")


# One requests.Session per worker thread; Sessions are not safe to share.
_local = threading.local()


def session_for_thread():
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


def fetch_one(job):
    """Candles for a single market. Runs on a worker thread.

    Returns (ticker, rows, error) so a failure is reported rather than
    silently dropping the market from the output.
    """
    series, market, lo, hi, interval = job
    try:
        start, end = market_window(market, lo, hi)
        rows = get_candles(session_for_thread(), series, market["ticker"],
                           start, end, interval)
        return market["ticker"], rows, None
    except Exception as exc:                     # network, HTTP, parse
        return market["ticker"], [], repr(exc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--interval", type=int, default=1, help="candle minutes")
    parser.add_argument("--out", default=str(paths.KALSHI_CANDLES),
                        help="output directory for Parquet parts")
    parser.add_argument("--batch", type=int, default=200,
                        help="flush to disk every N markets")
    parser.add_argument("--workers", type=int, default=16,
                        help="parallel requests; the wait dominates, not the work")
    parser.add_argument("--min-volume", type=float, default=0,
                        help="skip markets that traded less than this")
    parser.add_argument("--include-mve", action="store_true",
                        help="include Kalshi's auto-generated combo markets")
    args = parser.parse_args()

    paths.ensure()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    # Kept outside the directory, so the directory stays pure Parquet and
    # pyarrow can read it as a single dataset.
    done_path = paths.state("kalshi_candles.done")
    done = set(done_path.read_text().split()) if done_path.exists() else set()

    now = datetime.now(timezone.utc)
    lo = int((now - timedelta(days=args.days)).timestamp())
    hi = int(now.timestamp())

    skipped_mve = skipped_vol = 0
    markets = []
    for line in open(paths.MARKETS, encoding="utf-8"):
        market = json.loads(line)
        if market["ticker"] in done:
            continue
        # KXMVE* are auto-generated combination bets -- 95% of the catalogue
        # and not comparable to anything on Polymarket.
        if not args.include_mve and market["ticker"].startswith("KXMVE"):
            skipped_mve += 1
            continue
        if float(market.get("volume_fp") or 0) < args.min_volume:
            skipped_vol += 1
            continue
        markets.append((market["series_ticker"], market))

    part = len(list(out_dir.glob("part-*.parquet")))
    print(f"{len(markets)} markets to fetch ({len(done)} done, "
          f"{skipped_mve} combo, {skipped_vol} below volume), "
          f"part {part}, {args.workers} workers")

    jobs = [(series, market, lo, hi, args.interval) for series, market in markets]
    rows, batch_tickers, total, failed = [], [], 0, 0
    started = time.monotonic()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for done_count, (ticker, got, error) in enumerate(
                pool.map(fetch_one, jobs), start=1):
            if error:
                failed += 1
            rows += got
            batch_tickers.append(ticker)

            if len(batch_tickers) >= args.batch:
                flush(rows, out_dir, part)
                with open(done_path, "a") as f:
                    f.write("\n".join(batch_tickers) + "\n")
                total += len(rows)
                part += 1
                rate = done_count / max(time.monotonic() - started, 1e-6)
                left = (len(jobs) - done_count) / rate if rate else 0
                print(f"{done_count}/{len(jobs)} markets, {total} candles "
                      f"({rate:.1f}/s, ~{left/60:.0f} min left, {failed} failed)")
                rows, batch_tickers = [], []

    # Whatever is left over after the last full batch.
    if batch_tickers:
        flush(rows, out_dir, part)
        with open(done_path, "a") as f:
            f.write("\n".join(batch_tickers) + "\n")
        total += len(rows)

    print(f"wrote {total} candles to {out_dir}/  ({failed} markets failed)")


if __name__ == "__main__":
    main()

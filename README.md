# prediction_markets

Kalshi and Polymarket market data in PostgreSQL, with a browser viewer for
comparing the same question across both venues.

The whole design on one page: [`docs/architecture.html`](docs/architecture.html).

## Collection: four programs

```bash
python discover.py      # markets store, every 15 min        (service)
python activity.py      # scores each scan, picks hot list   (service)
python live.py          # one-minute candles, every minute   (service)
python backfill.py      # history and repairs                (by hand)
```

**discover.py** lists every open market every `MARKET_SYNC_MINUTES` (default
15), imports markets that closed since the last pass, and deletes markets that
closed more than `MARKET_RETENTION_DAYS` (default 30) ago. Each pass is also a
scan: every Kalshi market's volume, and when it was read, goes to
`kalshi_observations`. Progress is saved in `data/state/discovery.json`.

**activity.py** scores each finished scan inside PostgreSQL: a smoothed
contracts-per-minute rate with a one-hour half-life, seeded for new markets
with total volume / time open. A market whose volume did not move had no
trades, so its candles are marked complete without a request. The busiest
markets, as many as the request budget can fetch every minute, go on the hot
list in Redis.

**live.py** fetches the hot markets every minute, then every market that traded
since its last fetch. Only minutes with a trade are stored.

**backfill.py** fills history back to each market's open (or `BACKFILL_DAYS`):

```bash
python backfill.py --report            # coverage, no downloads
python backfill.py --verify            # candle volumes vs market volumes
python backfill.py --verify --repair   # re-download what does not match
python backfill.py --ticker T --refetch --from 2026-10-20 --to 2026-10-21
```

Every Kalshi request from every program comes out of one budget,
`KALSHI_REQUESTS_PER_MIN` (default 240), shared through Redis
(`ratelimit.py`): no 60 s window can exceed it, hot markets go first, backfill
gets what is left, and a 429 halves it for everyone.

```
discover.py    the pass schedule and its saved progress
activity.py    scores, the cold-market rule, the hot list
live.py        the one-minute loop
backfill.py    history, --report, --verify, --refetch
fetch.py       batch candle requests: plan, fetch, store, book-keep
ratelimit.py   the shared request budget
catalogue.py   one venue listing -> series / events / markets rows
venues.py      the Kalshi and Polymarket calls
store.py       schema setup, upserts, pruning, candle writes
settings.py    every tunable, read from .env
schema.sql     tables; timescale.sql adds hypertables and rollups
tests/         mock Kalshi, end-to-end run, activity and rate-limit tests
```

It creates the tables itself and **deletes markets that closed more than
`MARKET_RETENTION_DAYS` ago, candles included**. Do not point it at a database
holding history you want to keep without `--no-prune` or
`MARKET_RETENTION_DAYS=0`.

## Batch scrapers (the original pipeline)

```
kalshi_markets.py    step 1 -- build the market catalogue (cheap, ~200 requests)
kalshi_candles.py    step 2 -- candles for those markets (expensive, 1 request each)
kalshi_perps.py      perpetual futures: instruments + candles
polymarket.py        Polymarket markets and candles (both steps in one)

paths.py             every file location
db.py                PostgreSQL connection (reads .env)
schema.sql           tables
load_db.py           load the scraped files into PostgreSQL
viewer.py            web viewer at http://127.0.0.1:8080
start_postgres.ps1   start/stop/status for the local server

data/                see paths.py for what lives where
```

## Running it

```bash
python kalshi_markets.py --category all --list-only   # catalogue
python kalshi_candles.py --min-volume 10000           # candles for a subset
python polymarket.py --tag all --days 7               # the other venue
python load_db.py --schema                            # once
python load_db.py                                     # load whatever exists
python viewer.py                                      # browse
```

`.env` needs `PGPASSWORD`; copy `.env.example` and fill it in.

## Two things worth knowing

**Kalshi's catalogue is 95% combination markets.** `KXMVE*` tickers are
auto-generated multi-leg parlays -- 2.05M of 2.17M rows. They are archived in
`data/markets_all.jsonl` and excluded from `data/markets.jsonl`, which holds the
113k real markets.

**Polymarket candles are not real OHLC.** Its price history endpoint returns one
forward-filled point per minute rather than aggregated trades, so open, high,
low and close are almost always identical and volume is always null. Treat
`close` as the price. Kalshi's candles are genuine OHLCV.

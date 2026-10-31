# prediction_markets

Kalshi and Polymarket market data in PostgreSQL, with a browser viewer for
comparing the same question across both venues.

## Live collector

```bash
python live.py --dry-run     # no database: print 5-second candles for a few markets
python live.py               # run forever; see deploy/ for the systemd unit
```

```
live.py       the daemon: markets store, live candles, backfill
settings.py   every tunable, read from .env (listed in .env.example)
candles.py    price samples -> 5-second candles
venues.py     the Kalshi and Polymarket calls it makes
catalogue.py  keeps the catalogue of markets current
backfill.py   one-minute history for a market when it is first collected
store.py      candle writes, the per-ticker cap, pruning
```

`live.py` creates the tables itself and **deletes markets that closed more than
`MARKET_RETENTION_DAYS` ago, candles included**. Do not point it at a database
holding history you want to keep without `--no-prune` or
`MARKET_RETENTION_DAYS=0`.

Neither venue serves anything finer than a minute, so the 5-second candles are
built from polled quotes and begin when the collector does; backfilled history
is one-minute.

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

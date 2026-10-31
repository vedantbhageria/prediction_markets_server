# prediction_markets

Kalshi and Polymarket market data in PostgreSQL, with a browser viewer for
comparing the same question across both venues.

## Market discovery

```bash
python live.py --markets-only   # keep the markets store current; no candles
```

Every `MARKET_SYNC_MINUTES` (default 15) it lists every open market on both
venues, imports markets that closed since the last pass, and deletes markets
that closed more than `MARKET_RETENTION_DAYS` (default 30) ago. The first pass
imports everything closed within that window. Only the venues' own values are
stored; the columns for derived data stay empty for now.

Progress is saved in `data/state/discovery.json`, so a restart resumes an
interrupted import from its last page and only re-reads the time it was down.
`deploy/prediction-markets-discovery.service` runs it under systemd.

```
live.py        entry point (discovery, and later live candles)
discovery.py   the pass schedule and its saved progress
catalogue.py   one venue listing -> series / events / markets rows
venues.py      the Kalshi and Polymarket calls
store.py       schema setup, upserts, pruning, candle writes
settings.py    every tunable, read from .env (listed in .env.example)
schema.sql     tables; timescale.sql adds hypertables and rollups
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

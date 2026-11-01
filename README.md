# prediction_markets

Kalshi (and, later, Polymarket) market data in PostgreSQL on a 24/7 Ubuntu
server: every market open now or closed in the last 30 days, with one-minute
trade candles, a browser viewer and an HTTP API for Python.

- The whole design on one page: [`docs/architecture.html`](docs/architecture.html)
  (second tab: the API).
- The stress test of 1 Nov 2026 and what it fixed:
  [`docs/stress-test-2026-11-01.html`](docs/stress-test-2026-11-01.html).

## The six programs

| Program | What it does | Runs |
|---|---|---|
| `discover.py` | lists every open market, imports closed ones, deletes expired ones | always, a pass every 15 min |
| `activity.py` | scores each scan, publishes the hot list | always |
| `live.py` | one-minute candles: hot markets every minute, others as they trade | always |
| `backfill.py` | fills history; `--report`, `--verify`, `--refetch` | always; reruns every 15 min once done |
| `api/` | the HTTP API, 4 processes | always |
| `viewer.py` | the browser viewer, `/status` charts | always |

They run as systemd services, so they start on boot and restart if they crash:

```bash
sudo bash deploy/install-services.sh        # once: moves them from tmux to services
systemctl status prediction-markets-live    # any of the six
tail -f ~/logs/live.log                     # each writes ~/logs/<program>.log
sudo systemctl restart prediction-markets-api
```

**Don't restart them while the network refuses new connections** (Kalshi
unreachable in the logs): each keeps working on the connections it already has,
and a restart closes them until the network comes back. The install script
checks for this and refuses unless given `--force`.

## Collection

**discover.py** lists every open market every `MARKET_SYNC_MINUTES` (15),
imports markets that closed since the last pass, and deletes markets that
closed more than `MARKET_RETENTION_DAYS` (30) ago, candles included. Each pass
is also a scan: every market's volume, and when it was read, goes to
`kalshi_observations`. Progress is saved in `data/state/discovery.json`, so a
pass cut short resumes from its last page.

**activity.py** scores each finished scan inside PostgreSQL: a smoothed
contracts-per-minute rate with a one-hour half-life, seeded for new markets
with total volume / time open. A market whose volume did not move had no
trades, so its candles are marked complete without a request. The busiest
markets, as many as the budget can fetch every minute, go on the hot list in
Redis.

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
`KALSHI_REQUESTS_PER_MIN` (240), shared through Redis (`ratelimit.py`): no
60-second window above it, no second above `KALSHI_MAX_PER_SECOND` (15), hot
markets first, backfill on what is left, and a 429 halves it for everyone.
While Kalshi cannot be reached every request waits, checking every 30 s, and
carries on from where it was.

## The API

```bash
.venv/bin/uvicorn api.main:app --host 127.0.0.1 --port 8000 --workers 4
```

Public at `https://theonealpha.tail947e08.ts.net:8443` through Tailscale Funnel;
every request needs a key in `X-API-Key`. Interactive docs at `/docs`, a summary
in the API tab of `docs/architecture.html`.

```bash
python -m api.keys create laptop                # prints the key once
python -m api.keys create notebook --read-only  # cannot push backfills
python -m api.keys list
python -m api.keys revoke laptop
```

From Python, copy `pmapi.py` next to your notebook:

```python
from pmapi import Client
pm = Client("https://theonealpha.tail947e08.ts.net:8443", key="pm_...")

pm.markets(q="bitcoin", status="open")           # DataFrame
pm.candles("KXBTCD-...", "5m", start="2026-10-25")  # indexed by ts, forward-filled
pm.closes(["A", "B"], "1h")                      # one close column per market
pm.wait(pm.backfill("KXBTCD-..."))               # download it again, now
```

Candles come at `1m 5m 15m 1h 4h 1d`, forward-filled by default (only where
every trade is known to be stored) or `fill=raw`, as JSON, CSV or Parquet.
Pushed backfills run inside the API, batched so queued jobs share requests,
and survive restarts: a running job with no heartbeat for `API_JOB_LEASE_SECONDS`
is taken over by another process.

## The viewer

`viewer.py --host 100.106.150.105 --port 8080`, behind `VIEWER_USER` /
`VIEWER_PASSWORD_HASH`, public at `https://theonealpha.tail947e08.ts.net`.

- tables, filters, candle charts, manage, stats -- as before
- on a market's page: **related wording** (title similarity, with a slider) and
  **nearby markets** (same expiry, same odds, or both)
- **`/status`**: one page for everything happening now -- live ingestion,
  backfill progress, the Kalshi budget, discovery, the database and the API. Its history is one JSON line
  a minute in `data/state/monitor/`, kept 30 days.

## Tests

Run on the server, from the project folder. Each builds its own schema and Redis
prefix, never touches the real data, and drops its schema again when it passes.

```bash
.venv/bin/python tests/activity_test.py    # scoring rules, case by case      (seconds)
.venv/bin/python tests/ratelimit_test.py   # the shared budget, 3 processes   (3.5 min)
.venv/bin/python tests/panels_test.py      # related wording, nearby markets  (30 s)
.venv/bin/python tests/monitor_test.py     # the usage charts' minute files   (10 s)
.venv/bin/python tests/api_test.py         # every endpoint, keys, jobs       (2 min)
.venv/bin/python tests/live_test.py        # the collectors end to end, with faults (35 min)
.venv/bin/python tests/stress_test.py      # everything under load           (25 min)
```

`tests/mock_kalshi.py` is the simulated Kalshi they run against: trades known in
advance, so every stored candle can be checked.

## Files

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
monitor.py     the minute-by-minute record behind /status
viewer.py      the browser viewer
api/           the HTTP API; pmapi.py is its Python client
settings.py    every tunable, read from .env (see .env.example)
schema.sql     tables; timescale.sql adds hypertables and rollups
deploy/        systemd services and install-services.sh
tests/         the tests above and the simulated Kalshi
docs/          the architecture page and the stress-test report
```

`.env` needs `PGPASSWORD`; copy `.env.example`, which lists every setting.

The programs create the tables themselves and **delete markets that closed more
than `MARKET_RETENTION_DAYS` ago, candles included**. Do not point them at a
database holding history you want to keep without `--no-prune` or
`MARKET_RETENTION_DAYS=0`.

## The original batch scrapers

`kalshi_markets.py`, `kalshi_candles.py`, `kalshi_perps.py`, `polymarket.py` and
`load_db.py` are the run-by-hand pipeline this replaced; they still work.

## Two things worth knowing

**Kalshi's catalogue is mostly combination markets.** `KXMVE*` tickers are
auto-generated multi-leg parlays; discovery leaves them out.

**Polymarket candles are not real OHLC.** Its price history endpoint returns one
forward-filled point per minute rather than aggregated trades, so open, high,
low and close are almost always identical and volume is always null. Treat
`close` as the price. Kalshi's candles are genuine OHLCV.

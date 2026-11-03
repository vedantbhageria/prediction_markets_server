-- Kalshi and Polymarket market data. Safe to re-run.
--
-- Every fact lives in one table and the chains are enforced:
--
--   Kalshi:      kalshi_series -> kalshi_events -> kalshi_markets -> kalshi_candles
--   Polymarket:                   poly_events   -> poly_markets   -> poly_candles
--
-- A market's category, series and event details are read from its event, not
-- copied onto every market. Prices are NUMERIC, not float: the venues quote
-- exact decimal values and float would make equality comparisons unreliable.
-- The TimescaleDB layer on top of the candle tables is in timescale.sql.


-- ---------------------------------------------------------------------------
-- Kalshi
-- ---------------------------------------------------------------------------

-- A question template: rules, data source, timing. `frequency` says whether it
-- repeats (fifteen_min, hourly, daily, ...) or is asked once (one_off, custom).
CREATE TABLE IF NOT EXISTS kalshi_series (
    series_ticker TEXT PRIMARY KEY,
    category      TEXT,
    title         TEXT,
    frequency     TEXT
);

-- One instance of a series, e.g. "Bitcoin price on Oct 26 at 5pm". Dates stay
-- on the markets: a fifth of events hold markets that close at different times.
CREATE TABLE IF NOT EXISTS kalshi_events (
    event_ticker       TEXT PRIMARY KEY,
    series_ticker      TEXT NOT NULL REFERENCES kalshi_series (series_ticker),
    title              TEXT,
    sub_title          TEXT,
    category           TEXT,
    -- True when exactly one market in the event can resolve yes, so their
    -- prices should add up to about 100%.
    mutually_exclusive BOOLEAN,
    strike_period      TEXT,
    raw                JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS kalshi_events_series_idx ON kalshi_events (series_ticker);
CREATE INDEX IF NOT EXISTS kalshi_events_category_idx ON kalshi_events (category);

-- One yes/no contract. Everything but `strike` is Kalshi's own value; `raw`
-- is the payload exactly as Kalshi sent it.
CREATE TABLE IF NOT EXISTS kalshi_markets (
    ticker        TEXT PRIMARY KEY,
    event_ticker  TEXT NOT NULL REFERENCES kalshi_events (event_ticker) ON DELETE CASCADE,
    title         TEXT,
    status        TEXT,
    result        TEXT,
    open_time     TIMESTAMPTZ,
    close_time    TIMESTAMPTZ,
    -- How yes relates to the strikes: greater, greater_or_equal, less,
    -- less_or_equal, between, structured, custom -- or empty.
    strike_type   TEXT,
    floor_strike  NUMERIC(20, 6),
    cap_strike    NUMERIC(20, 6),
    -- The one level the question turns on: floor_strike, else cap_strike.
    -- Lets "same strike" be compared with Polymarket.
    strike        NUMERIC(20, 6),
    volume        NUMERIC(20, 4),
    open_interest NUMERIC(20, 4),
    raw           JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS kalshi_markets_event_idx ON kalshi_markets (event_ticker);
CREATE INDEX IF NOT EXISTS kalshi_markets_close_idx ON kalshi_markets (close_time);
CREATE INDEX IF NOT EXISTS kalshi_markets_strike_type_idx ON kalshi_markets (strike_type);

-- One row per market per minute that had at least one trade, stamped with
-- the start of the minute. Minutes without a trade have no row; they are
-- filled in when read, not stored.
CREATE TABLE IF NOT EXISTS kalshi_candles (
    ticker        TEXT NOT NULL REFERENCES kalshi_markets (ticker) ON DELETE CASCADE,
    ts            TIMESTAMPTZ NOT NULL,
    open          NUMERIC(12, 6),
    high          NUMERIC(12, 6),
    low           NUMERIC(12, 6),
    close         NUMERIC(12, 6),
    volume        NUMERIC(20, 4),
    open_interest NUMERIC(20, 4),
    PRIMARY KEY (ticker, ts)
);

CREATE INDEX IF NOT EXISTS kalshi_candles_ts_idx ON kalshi_candles (ts);

-- Live-collection bookkeeping, one row per market.
--
--   volume, volume_measured_at  the market's total volume at the last scan,
--                               and when it was read (see activity.py)
--   activity_score              smoothed trading rate, contracts per minute
--   candles_fetched_from/_to    every trade in [from, to) has its candle stored
--
-- No index on activity_score: it changes every scan, and an index on it would
-- turn each of those cheap in-page updates into a full rewrite. Picking the
-- busiest markets is a ~40 ms scan without one. fillfactor leaves room in each
-- page for those updates.
CREATE TABLE IF NOT EXISTS kalshi_tracking (
    ticker               TEXT PRIMARY KEY REFERENCES kalshi_markets (ticker) ON DELETE CASCADE,
    volume               NUMERIC(20, 4) NOT NULL,
    volume_measured_at   TIMESTAMPTZ NOT NULL,
    activity_score       DOUBLE PRECISION NOT NULL DEFAULT 0,
    candles_fetched_from TIMESTAMPTZ NOT NULL,
    candles_fetched_to   TIMESTAMPTZ NOT NULL
) WITH (fillfactor = 70);

-- One row per discovery scan. activity.py scores each scan once, in order.
CREATE TABLE IF NOT EXISTS kalshi_scans (
    scan_id     BIGSERIAL PRIMARY KEY,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    scored_at   TIMESTAMPTZ
);

-- What each scan read: a market's volume, and the two moments it was true
-- between -- the venue was asked at asked_at (less OBSERVATION_LAG, in case its
-- listing runs behind) and had answered by answered_at. A hand-off from
-- discovery to activity.py, emptied once a scan is scored. UNLOGGED: it is
-- rewritten every scan, and losing it in a crash only skips one scan's scores.
CREATE UNLOGGED TABLE IF NOT EXISTS kalshi_observations (
    scan_id     BIGINT NOT NULL,
    ticker      TEXT NOT NULL,
    volume      NUMERIC(20, 4) NOT NULL,
    asked_at    TIMESTAMPTZ NOT NULL,
    answered_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS kalshi_observations_scan_idx ON kalshi_observations (scan_id);


-- Kalshi's perpetual futures were dropped on 3 Nov 2026 (never collected
-- here); their empty tables go. A table that somehow holds rows is kept.
-- (Dynamic SQL: a table named in the block itself is looked up even where the
-- branch never runs, and a fresh database has neither.)
DO $$
DECLARE
    has_rows boolean;
BEGIN
    IF to_regclass('kalshi_perp_candles') IS NOT NULL THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM kalshi_perp_candles)' INTO has_rows;
        IF NOT has_rows THEN
            EXECUTE 'DROP TABLE kalshi_perp_candles';
        END IF;
    END IF;
    IF to_regclass('kalshi_perp_markets') IS NOT NULL AND to_regclass('kalshi_perp_candles') IS NULL THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM kalshi_perp_markets)' INTO has_rows;
        IF NOT has_rows THEN
            EXECUTE 'DROP TABLE kalshi_perp_markets';
        END IF;
    END IF;
END $$;


-- ---------------------------------------------------------------------------
-- Polymarket. No series level: markets are grouped under events only.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS poly_events (
    event_id     TEXT PRIMARY KEY,
    slug         TEXT,
    title        TEXT,
    category     TEXT,          -- mapped from the tags to Kalshi's category names
    tags         TEXT[],
    market_count INTEGER
);

CREATE INDEX IF NOT EXISTS poly_events_category_idx ON poly_events (category);

CREATE TABLE IF NOT EXISTS poly_markets (
    condition_id  TEXT PRIMARY KEY,
    event_id      TEXT REFERENCES poly_events (event_id) ON DELETE CASCADE,
    slug          TEXT,
    question      TEXT,
    -- The Yes-side token; its price is the probability.
    yes_token_id  TEXT,
    end_date      TIMESTAMPTZ,
    closed        BOOLEAN,
    -- Same vocabulary as Kalshi's strike_type, worked out from the wording of
    -- the question, so best-effort. strike is the first dollar amount in it.
    strike_type   TEXT,
    strike        NUMERIC(20, 6),
    -- The date the question names, often a day before end_date: "Will Bitcoin
    -- reach $73,000 on August 20?" resolves on the 21st.
    stated_date   DATE,
    volume        NUMERIC(20, 4),
    liquidity     NUMERIC(20, 4),
    raw           JSONB NOT NULL
);

CREATE INDEX IF NOT EXISTS poly_markets_event_idx ON poly_markets (event_id);
CREATE INDEX IF NOT EXISTS poly_markets_end_idx ON poly_markets (end_date);
CREATE INDEX IF NOT EXISTS poly_markets_strike_type_idx ON poly_markets (strike_type);
CREATE INDEX IF NOT EXISTS poly_markets_stated_idx ON poly_markets (stated_date);

-- One row per market per minute, made by aggregate.py from poly_trades: the
-- price of the market's first outcome (its yes_token_id -- a trade in the other
-- outcome at p counts as 1 - p), volume in shares, notional in USDC paid.
CREATE TABLE IF NOT EXISTS poly_candles (
    condition_id  TEXT NOT NULL REFERENCES poly_markets (condition_id) ON DELETE CASCADE,
    ts            TIMESTAMPTZ NOT NULL,
    open          NUMERIC(12, 6),
    high          NUMERIC(12, 6),
    low           NUMERIC(12, 6),
    close         NUMERIC(12, 6),
    volume        NUMERIC(20, 4),
    PRIMARY KEY (condition_id, ts)
);
ALTER TABLE poly_candles ADD COLUMN IF NOT EXISTS notional NUMERIC(20, 4);
ALTER TABLE poly_candles ADD COLUMN IF NOT EXISTS trades INTEGER;
-- Until 3 Nov 2026 history the v1 API would not list was filled from order-book
-- midpoints (source 'mid'); v2 lists every trade, so those candles go and the
-- markets are read again (their trades_from is past history_from, so backfill
-- picks them up). Compressed chunks are decompressed for the delete.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema()
               AND table_name = 'poly_candles' AND column_name = 'source') THEN
        PERFORM set_config('timescaledb.max_tuples_decompressed_per_dml_transaction', '0', true);
        DELETE FROM poly_candles WHERE source = 'mid';
        ALTER TABLE poly_candles DROP COLUMN source;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS poly_candles_ts_idx ON poly_candles (ts);

-- Every Polymarket trade, as its taker's order filled: one row per taker
-- order, so volume is counted once. Kept for the retention window, like the
-- markets. No foreign key: the feed carries trades for markets discovery has
-- not listed yet, and those are kept until it does. trade_id is an md5 of the
-- fields that tell trades apart (transaction, token, wallet, side, size,
-- price), so a trade read twice -- feed and market list -- is stored once.
CREATE TABLE IF NOT EXISTS poly_trades (
    condition_id  TEXT NOT NULL,
    ts            TIMESTAMPTZ NOT NULL,
    trade_id      TEXT NOT NULL,
    token_id      TEXT NOT NULL,
    side          TEXT,
    price         NUMERIC(12, 6) NOT NULL,
    size          NUMERIC(20, 6) NOT NULL,
    wallet        TEXT,
    tx_hash       TEXT,
    PRIMARY KEY (condition_id, ts, trade_id)
);

CREATE INDEX IF NOT EXISTS poly_trades_ts_idx ON poly_trades (ts);

-- Minutes whose trades changed since aggregate.py last made their candle.
-- Written in the same transaction as the trades, emptied as candles are made.
CREATE TABLE IF NOT EXISTS poly_dirty (
    condition_id  TEXT NOT NULL,
    minute        TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (condition_id, minute)
);

-- Collection bookkeeping, one row per market, as kalshi_tracking.
--
--   volume, volume_24h   as last read by discovery
--   moved_at             when a reading last found either one higher: the
--                        market traded at some point before then
--   history_from         where its history starts: its own start, or
--                        BACKFILL_DAYS ago if that is later
--   trades_from/_to      every trade in [trades_from, trades_to) is stored
--
-- Live collection does not move trades_to for every market every few seconds:
-- the feed's own coverage is kept once, in poly_feed, and poly_covered_to()
-- joins the two (see poly_feed).
CREATE TABLE IF NOT EXISTS poly_tracking (
    condition_id  TEXT PRIMARY KEY REFERENCES poly_markets (condition_id) ON DELETE CASCADE,
    volume        NUMERIC(20, 4) NOT NULL DEFAULT 0,
    volume_24h    NUMERIC(20, 4) NOT NULL DEFAULT 0,
    moved_at      TIMESTAMPTZ,
    history_from  TIMESTAMPTZ NOT NULL,
    trades_from   TIMESTAMPTZ NOT NULL,
    trades_to     TIMESTAMPTZ NOT NULL
) WITH (fillfactor = 70);
ALTER TABLE poly_tracking DROP COLUMN IF EXISTS mid_to;

-- The feed of every Polymarket trade, as trades.py has read it: every trade
-- in [covered_from, covered_to) is stored, whatever its market. One row. When
-- the feed has to start again after a gap, every market it was covering has
-- its trades_to moved up to the old covered_to first, so nothing is lost.
CREATE TABLE IF NOT EXISTS poly_feed (
    id            BOOLEAN PRIMARY KEY DEFAULT true CHECK (id),
    covered_from  TIMESTAMPTZ NOT NULL,
    covered_to    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- A market's real trades_to: its own, or the feed's if the feed took over
-- from it without a gap.
CREATE OR REPLACE FUNCTION poly_covered_to(own TIMESTAMPTZ) RETURNS TIMESTAMPTZ
LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN f.covered_from IS NOT NULL AND own >= f.covered_from
                THEN GREATEST(own, f.covered_to) ELSE own END
    FROM (SELECT (SELECT covered_from FROM poly_feed) AS covered_from,
                 (SELECT covered_to FROM poly_feed) AS covered_to) f
$$;


-- ---------------------------------------------------------------------------
-- Title similarity, used by the viewer's "related wording" panel.
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS kalshi_markets_title_trgm_idx
    ON kalshi_markets USING gin (title gin_trgm_ops);
-- The API's q= also matches a ticker prefix; without this that half reads
-- the whole table.
CREATE INDEX IF NOT EXISTS kalshi_markets_ticker_trgm_idx
    ON kalshi_markets USING gin (ticker gin_trgm_ops);

-- Each market's candle count and first and last candle, for the viewer's
-- manage and visualise pages: counting a growing candle table on every page
-- view made them take seconds. viewer.py refreshes it every 10 minutes.
CREATE MATERIALIZED VIEW IF NOT EXISTS kalshi_candle_counts AS
    SELECT ticker, count(*) AS n, min(ts) AS first_ts, max(ts) AS last_ts
    FROM kalshi_candles GROUP BY ticker
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS kalshi_candle_counts_ticker_idx ON kalshi_candle_counts (ticker);
CREATE INDEX IF NOT EXISTS kalshi_candle_counts_n_idx ON kalshi_candle_counts (n DESC, ticker);
CREATE MATERIALIZED VIEW IF NOT EXISTS poly_candle_counts AS
    SELECT condition_id, count(*) AS n, min(ts) AS first_ts, max(ts) AS last_ts
    FROM poly_candles GROUP BY condition_id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS poly_candle_counts_id_idx ON poly_candle_counts (condition_id);
CREATE INDEX IF NOT EXISTS poly_candle_counts_n_idx ON poly_candle_counts (n DESC, condition_id);

-- The viewer's manage page sorts every market by one of these, largest or
-- latest first, a page at a time: each index is in exactly that order, so a
-- page reads a few hundred rows instead of sorting four million.
CREATE INDEX IF NOT EXISTS kalshi_markets_volume_sort_idx
    ON kalshi_markets (volume DESC NULLS LAST, ticker);
CREATE INDEX IF NOT EXISTS kalshi_markets_close_sort_idx
    ON kalshi_markets (close_time DESC NULLS LAST, ticker);
CREATE INDEX IF NOT EXISTS kalshi_markets_open_sort_idx
    ON kalshi_markets (open_time DESC NULLS LAST, ticker);
CREATE INDEX IF NOT EXISTS poly_markets_volume_sort_idx
    ON poly_markets (volume DESC NULLS LAST, condition_id);
CREATE INDEX IF NOT EXISTS poly_markets_end_sort_idx
    ON poly_markets (end_date DESC NULLS LAST, condition_id);

-- Every distinct Kalshi market title, for the viewer's related-wording panel.
-- A price ladder repeats one title across hundreds of markets, so there are a
-- fifth as many titles as markets, and the GiST index walks straight to the
-- nearest ones (ORDER BY title <-> ...) instead of scoring every candidate.
-- discover.py adds titles as it writes markets; store.prune drops titles no
-- market has any more. The hash index leads from a title back to its markets.
CREATE TABLE IF NOT EXISTS kalshi_titles (title TEXT PRIMARY KEY);
CREATE INDEX IF NOT EXISTS kalshi_titles_trgm_idx
    ON kalshi_titles USING gist (title gist_trgm_ops);
CREATE INDEX IF NOT EXISTS kalshi_markets_title_hash_idx
    ON kalshi_markets USING hash (title);
CREATE INDEX IF NOT EXISTS poly_markets_question_trgm_idx
    ON poly_markets USING gin (question gin_trgm_ops);
-- Nearest questions first (ORDER BY question <-> text), for the viewer's
-- related-wording panel: filtering 1.6 million questions by similarity took
-- seconds. Built on the server with CREATE INDEX CONCURRENTLY on 3 Nov 2026.
CREATE INDEX IF NOT EXISTS poly_markets_question_gist_idx
    ON poly_markets USING gist (question gist_trgm_ops);


-- ---------------------------------------------------------------------------
-- Similar markets, within and across venues (match.py, api/similar.py). Every
-- market as one record (propositions.py explains each field); `sources` says,
-- field by field, whether the venue sent it ("native") or it was read from
-- the wording ("regex"). Each record's template has one vector in `templates`
-- (vector.sql, applied once pgvector is enabled).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS propositions (
    venue        TEXT NOT NULL,
    market_id    TEXT NOT NULL,
    group_id     TEXT,
    kind         TEXT,
    subject      TEXT,
    entity       TEXT,
    comparator   TEXT,
    low          DOUBLE PRECISION,
    high         DOUBLE PRECISION,
    unit         TEXT,
    when_at      TEXT,          -- a date, a month (2026-07), a quarter (2026-Q3) or a year
    when_kind    TEXT,          -- by, on, within, after
    placeholder  BOOLEAN NOT NULL DEFAULT false,
    template     TEXT,
    topic        TEXT[],
    sources      JSONB,
    made_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (venue, market_id)
);

CREATE INDEX IF NOT EXISTS propositions_template_idx ON propositions (template);
CREATE INDEX IF NOT EXISTS propositions_group_idx ON propositions (venue, group_id);

-- Template embeddings kept as plain arrays until pgvector is enabled; then
-- vector.sql moves them into `templates` and drops this (so it is only made
-- while `templates` does not exist).
DO $$
BEGIN
    IF to_regclass('templates') IS NULL THEN
        CREATE TABLE IF NOT EXISTS template_vectors (
            template  TEXT PRIMARY KEY,
            model     TEXT NOT NULL,
            vec       REAL[] NOT NULL
        );
    END IF;
END $$;


-- ---------------------------------------------------------------------------
-- The HTTP API (api/). Keys are stored only as a SHA-256 of the key itself:
-- they are long random strings, so a plain hash is enough to make a stolen
-- table useless. `scope` is 'read' (data only) or 'full' (may also ask for
-- backfills); rate_per_min overrides the default request limit.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS api_keys (
    key_id       BIGSERIAL PRIMARY KEY,
    name         TEXT NOT NULL UNIQUE,
    key_hash     TEXT NOT NULL UNIQUE,
    key_prefix   TEXT NOT NULL,         -- the first characters, to tell keys apart
    scope        TEXT NOT NULL DEFAULT 'full' CHECK (scope IN ('read', 'full')),
    rate_per_min INTEGER,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ,
    revoked_at   TIMESTAMPTZ
);

-- Backfills asked for through the API, run by workers inside it. A job is
-- queued, then running, then done or failed; a running job whose heartbeat
-- stops (its process died) is taken over by another worker.
CREATE TABLE IF NOT EXISTS api_jobs (
    job_id         BIGSERIAL PRIMARY KEY,
    key_id         BIGINT REFERENCES api_keys (key_id) ON DELETE SET NULL,
    ticker         TEXT NOT NULL,
    from_ts        TIMESTAMPTZ NOT NULL,
    to_ts          TIMESTAMPTZ NOT NULL,
    status         TEXT NOT NULL DEFAULT 'queued',
    requested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ,
    requests_total INTEGER,
    requests_done  INTEGER NOT NULL DEFAULT 0,
    candles        INTEGER NOT NULL DEFAULT 0,
    error          TEXT,
    heartbeat_at   TIMESTAMPTZ          -- renewed while a worker runs it
);
ALTER TABLE api_jobs ADD COLUMN IF NOT EXISTS heartbeat_at TIMESTAMPTZ;

-- Every API request, for the viewer's /api-usage page. The API writes them in
-- batches a few seconds after the fact; store.prune drops them after 30 days.
CREATE TABLE IF NOT EXISTS api_requests (
    at          TIMESTAMPTZ NOT NULL,
    key_id      BIGINT,                 -- NULL: no key, or a wrong one
    method      TEXT NOT NULL,
    path        TEXT NOT NULL,          -- as asked: /v1/markets/KX.../candles
    endpoint    TEXT,                   -- the route: /v1/markets/{ticker}/candles
    params      JSONB,                  -- the query string
    tickers     TEXT[],                 -- the markets it was about
    status      SMALLINT NOT NULL,
    error       TEXT,                   -- the error code of a refusal
    ms          REAL NOT NULL,
    row_count   INTEGER,
    bytes       BIGINT,
    client_ip   TEXT,
    user_agent  TEXT
);

CREATE INDEX IF NOT EXISTS api_requests_at_idx ON api_requests (at);

-- At most one unfinished job per market.
CREATE UNIQUE INDEX IF NOT EXISTS api_jobs_open_idx ON api_jobs (ticker)
    WHERE status IN ('queued', 'running');


-- ---------------------------------------------------------------------------
-- Upgrades for databases made by an earlier version of this file.
-- ---------------------------------------------------------------------------

-- kalshi_titles is new: fill it once from the markets already stored.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM kalshi_titles) AND EXISTS (SELECT 1 FROM kalshi_markets) THEN
        INSERT INTO kalshi_titles SELECT DISTINCT title FROM kalshi_markets WHERE title IS NOT NULL;
    END IF;
END $$;

-- Candles used to carry a `filled` flag for minutes with no trade. Only traded
-- minutes are stored now. The rollups read the column, so they go first;
-- timescale.sql makes them again.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = current_schema() AND column_name = 'filled'
                 AND table_name IN ('kalshi_candles', 'poly_candles')) THEN
        DROP VIEW IF EXISTS candles_1m, candles_5m, candles_15m, candles_1h;
        DROP MATERIALIZED VIEW IF EXISTS kalshi_candles_1h, kalshi_candles_15m,
            kalshi_candles_5m, poly_candles_1h, poly_candles_15m, poly_candles_5m CASCADE;
        ALTER TABLE kalshi_candles DROP COLUMN IF EXISTS filled;
        ALTER TABLE poly_candles DROP COLUMN IF EXISTS filled;
    END IF;
END $$;

-- The daily data check (check.py): one row per check per run.
CREATE TABLE IF NOT EXISTS checks (
    run_at  TIMESTAMPTZ NOT NULL,
    name    TEXT NOT NULL,
    status  TEXT NOT NULL,          -- ok, warn, fail
    value   TEXT,                   -- one line, for people
    detail  JSONB,
    PRIMARY KEY (run_at, name)
);

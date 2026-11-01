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


-- Perpetual futures. contract_size is needed to read the price at all: a
-- KXBTCPERP close of 6.3865 with contract_size 0.0001 means $63,865 per BTC.
CREATE TABLE IF NOT EXISTS kalshi_perp_markets (
    ticker        TEXT PRIMARY KEY,
    contract_size NUMERIC(20, 6),
    tick_size     NUMERIC(20, 6),
    status        TEXT,
    raw           JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS kalshi_perp_candles (
    ticker        TEXT NOT NULL REFERENCES kalshi_perp_markets (ticker) ON DELETE CASCADE,
    ts            TIMESTAMPTZ NOT NULL,
    open          NUMERIC(20, 6),
    high          NUMERIC(20, 6),
    low           NUMERIC(20, 6),
    close         NUMERIC(20, 6),
    volume        NUMERIC(20, 4),
    open_interest NUMERIC(20, 4),
    PRIMARY KEY (ticker, ts)
);

CREATE INDEX IF NOT EXISTS kalshi_perp_candles_ts_idx ON kalshi_perp_candles (ts);


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

-- Polymarket publishes one price per minute, so open, high, low and close are
-- the same number, and no volume.
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

CREATE INDEX IF NOT EXISTS poly_candles_ts_idx ON poly_candles (ts);


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

-- Backfills asked for through the API, run by a worker inside it. A job is
-- queued, then running, then done or failed; one left running by a restart
-- is queued again.
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
    error          TEXT
);

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
                 AND table_name IN ('kalshi_candles', 'poly_candles', 'kalshi_perp_candles')) THEN
        DROP VIEW IF EXISTS candles_1m, candles_5m, candles_15m, candles_1h;
        DROP MATERIALIZED VIEW IF EXISTS kalshi_candles_1h, kalshi_candles_15m,
            kalshi_candles_5m, poly_candles_1h, poly_candles_15m, poly_candles_5m CASCADE;
        ALTER TABLE kalshi_candles DROP COLUMN IF EXISTS filled;
        ALTER TABLE poly_candles DROP COLUMN IF EXISTS filled;
        ALTER TABLE kalshi_perp_candles DROP COLUMN IF EXISTS filled;
    END IF;
END $$;

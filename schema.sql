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

-- One row per market per minute, stamped with the start of the minute.
-- `filled` marks a minute the venue sent nothing for: the row repeats the
-- previous close with volume 0 so every minute is present.
CREATE TABLE IF NOT EXISTS kalshi_candles (
    ticker        TEXT NOT NULL REFERENCES kalshi_markets (ticker) ON DELETE CASCADE,
    ts            TIMESTAMPTZ NOT NULL,
    open          NUMERIC(12, 6),
    high          NUMERIC(12, 6),
    low           NUMERIC(12, 6),
    close         NUMERIC(12, 6),
    volume        NUMERIC(20, 4),
    open_interest NUMERIC(20, 4),
    filled        BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (ticker, ts)
);

CREATE INDEX IF NOT EXISTS kalshi_candles_ts_idx ON kalshi_candles (ts);


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
    filled        BOOLEAN NOT NULL DEFAULT false,
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
    filled        BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (condition_id, ts)
);

CREATE INDEX IF NOT EXISTS poly_candles_ts_idx ON poly_candles (ts);


-- ---------------------------------------------------------------------------
-- Title similarity, used by the viewer's "related wording" panel.
-- ---------------------------------------------------------------------------

CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS kalshi_markets_title_trgm_idx
    ON kalshi_markets USING gin (title gin_trgm_ops);
CREATE INDEX IF NOT EXISTS poly_markets_question_trgm_idx
    ON poly_markets USING gin (question gin_trgm_ops);

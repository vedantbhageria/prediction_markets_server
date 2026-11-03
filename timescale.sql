-- TimescaleDB layer on top of schema.sql. Applied by store.ensure_schema()
-- only when the timescaledb extension is installed; safe to re-run.
--
-- The candle tables become hypertables (one chunk per day), chunks older than
-- two days are compressed, and 5m / 15m / 1h candles are kept as continuous
-- aggregates that Timescale refreshes by itself. Candles are not deleted by
-- age: they go when their market leaves the store (store.prune).
--
-- Every candle is stamped with the START of the minute it covers, on both
-- venues, so a bucket is just time_bucket(width, ts).

SELECT create_hypertable('kalshi_candles', by_range('ts', INTERVAL '1 day'),
                         if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('poly_candles', by_range('ts', INTERVAL '1 day'),
                         if_not_exists => TRUE, migrate_data => TRUE);
SELECT create_hypertable('poly_trades', by_range('ts', INTERVAL '1 day'),
                         if_not_exists => TRUE, migrate_data => TRUE);

-- Compression: rows grouped per market, newest first, which is how they are read.
DO $$
DECLARE t record;
BEGIN
    FOR t IN SELECT * FROM (VALUES ('kalshi_candles', 'ticker'),
                                   ('poly_candles', 'condition_id'),
                                   ('poly_trades', 'condition_id')) v(tbl, seg)
    LOOP
        IF NOT EXISTS (SELECT 1 FROM timescaledb_information.hypertables
                       WHERE hypertable_schema = current_schema()
                         AND hypertable_name = t.tbl AND compression_enabled) THEN
            EXECUTE format('ALTER TABLE %I SET (timescaledb.compress,'
                           ' timescaledb.compress_segmentby = %L,'
                           ' timescaledb.compress_orderby = %L)', t.tbl, t.seg, 'ts DESC');
        END IF;
        PERFORM add_compression_policy(t.tbl, INTERVAL '2 days', if_not_exists => TRUE);
    END LOOP;
END $$;


-- ---------------------------------------------------------------------------
-- Rollups. The raw tables are one-minute candles; 5m reads them, 15m reads 5m
-- and 1h reads 15m. Only minutes with a trade are stored, so a bucket is made
-- of its traded minutes and `trade_minutes` says how many there were; a bucket
-- with no trade has no row.
-- materialized_only = false: queries include data newer than the last refresh.
-- ---------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS kalshi_candles_5m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT ticker, time_bucket(INTERVAL '5 minutes', ts) AS ts,
       first(open, ts) AS open, max(high) AS high, min(low) AS low,
       last(close, ts) AS close, sum(volume) AS volume,
       last(open_interest, ts) AS open_interest,
       count(*) AS trade_minutes
FROM kalshi_candles
GROUP BY ticker, time_bucket(INTERVAL '5 minutes', ts)
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS kalshi_candles_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT ticker, time_bucket(INTERVAL '15 minutes', ts) AS ts,
       first(open, ts) AS open, max(high) AS high, min(low) AS low,
       last(close, ts) AS close, sum(volume) AS volume,
       last(open_interest, ts) AS open_interest,
       sum(trade_minutes) AS trade_minutes
FROM kalshi_candles_5m
GROUP BY ticker, time_bucket(INTERVAL '15 minutes', ts)
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS kalshi_candles_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT ticker, time_bucket(INTERVAL '1 hour', ts) AS ts,
       first(open, ts) AS open, max(high) AS high, min(low) AS low,
       last(close, ts) AS close, sum(volume) AS volume,
       last(open_interest, ts) AS open_interest,
       sum(trade_minutes) AS trade_minutes
FROM kalshi_candles_15m
GROUP BY ticker, time_bucket(INTERVAL '1 hour', ts)
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS poly_candles_5m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT condition_id, time_bucket(INTERVAL '5 minutes', ts) AS ts,
       first(open, ts) AS open, max(high) AS high, min(low) AS low,
       last(close, ts) AS close, sum(volume) AS volume,
       count(*) AS trade_minutes
FROM poly_candles
GROUP BY condition_id, time_bucket(INTERVAL '5 minutes', ts)
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS poly_candles_15m
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT condition_id, time_bucket(INTERVAL '15 minutes', ts) AS ts,
       first(open, ts) AS open, max(high) AS high, min(low) AS low,
       last(close, ts) AS close, sum(volume) AS volume,
       sum(trade_minutes) AS trade_minutes
FROM poly_candles_5m
GROUP BY condition_id, time_bucket(INTERVAL '15 minutes', ts)
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS poly_candles_1h
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT condition_id, time_bucket(INTERVAL '1 hour', ts) AS ts,
       first(open, ts) AS open, max(high) AS high, min(low) AS low,
       last(close, ts) AS close, sum(volume) AS volume,
       sum(trade_minutes) AS trade_minutes
FROM poly_candles_15m
GROUP BY condition_id, time_bucket(INTERVAL '1 hour', ts)
WITH NO DATA;


-- Both venues side by side, one shape.
CREATE OR REPLACE VIEW candles_1m AS
    SELECT 'kalshi' AS venue, ticker AS market_id, ts, open, high, low, close, volume
    FROM kalshi_candles
    UNION ALL
    SELECT 'poly', condition_id, ts, open, high, low, close, volume FROM poly_candles;

CREATE OR REPLACE VIEW candles_5m AS
    SELECT 'kalshi' AS venue, ticker AS market_id, ts, open, high, low, close, volume
    FROM kalshi_candles_5m
    UNION ALL
    SELECT 'poly', condition_id, ts, open, high, low, close, volume FROM poly_candles_5m;

CREATE OR REPLACE VIEW candles_15m AS
    SELECT 'kalshi' AS venue, ticker AS market_id, ts, open, high, low, close, volume
    FROM kalshi_candles_15m
    UNION ALL
    SELECT 'poly', condition_id, ts, open, high, low, close, volume FROM poly_candles_15m;

CREATE OR REPLACE VIEW candles_1h AS
    SELECT 'kalshi' AS venue, ticker AS market_id, ts, open, high, low, close, volume
    FROM kalshi_candles_1h
    UNION ALL
    SELECT 'poly', condition_id, ts, open, high, low, close, volume FROM poly_candles_1h;

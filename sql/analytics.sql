-- Example analytics on top of the Parquet lake.
-- Usage:
--   .venv/bin/crypto-ingest catalog
--   duckdb data/catalog.duckdb
-- then paste individual statements below.

SET TimeZone = 'UTC';

-- 1. Resample 1-second klines into 5-minute OHLCV bars.
SELECT
    symbol,
    time_bucket(INTERVAL '5 minutes', open_time) AS bucket,
    first(open ORDER BY open_time)   AS open,
    max(high)                        AS high,
    min(low)                         AS low,
    last(close ORDER BY open_time)   AS close,
    sum(volume)                      AS volume,
    sum(quote_volume)                AS quote_volume
FROM klines_1s
GROUP BY symbol, bucket
ORDER BY symbol, bucket;

-- 2. VWAP and taker-buy share per hour.
SELECT
    symbol,
    date_trunc('hour', open_time) AS hour_bucket,
    sum(quote_volume) / nullif(sum(volume), 0)              AS vwap,
    sum(taker_buy_quote) / nullif(sum(quote_volume), 0)     AS taker_buy_share
FROM klines_1m
GROUP BY symbol, hour_bucket
ORDER BY symbol, hour_bucket;

-- 3. Aggressor imbalance from aggregated trades (positive = more buyer-initiated).
SELECT
    symbol,
    date_trunc('hour', trade_time) AS hour_bucket,
    sum(CASE WHEN is_buyer_maker THEN -qty ELSE qty END) AS net_taker_flow,
    count(*)                                             AS trades
FROM agg_trades
GROUP BY symbol, hour_bucket
ORDER BY symbol, hour_bucket;

-- 4. Data coverage check: hours with fewer than 55 one-minute candles.
SELECT
    symbol,
    date_trunc('hour', open_time) AS hour_bucket,
    count(*) AS candles
FROM klines_1m
GROUP BY symbol, hour_bucket
HAVING count(*) < 55
ORDER BY candles
LIMIT 100;

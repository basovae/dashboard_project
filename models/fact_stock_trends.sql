SELECT
    symbol,
    timestamp,
    close,
    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY timestamp) AS prev_close,
    (close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY timestamp)) / LAG(close, 1) OVER (PARTITION BY symbol ORDER BY timestamp) * 100 AS pct_change
FROM {{ ref('stg_stock_prices') }};

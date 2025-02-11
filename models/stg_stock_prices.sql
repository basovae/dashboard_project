SELECT
    symbol,
    timestamp,
    CAST(open AS FLOAT) AS open,
    CAST(close AS FLOAT) AS close,
    CAST(volume AS INT) AS volume
FROM {{ source('stocks_db', 'stock_prices') }};

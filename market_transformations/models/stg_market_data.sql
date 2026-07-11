{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT * FROM {{ source('binance_raw', 'src_market_data') }}
)

SELECT
    id AS transaction_id,
    symbol,
    CAST(price_usd AS DECIMAL(18, 8)) AS current_price_usd,
    CAST(high_price AS DECIMAL(18, 8)) AS high_price_24h,
    CAST(low_price AS DECIMAL(18, 8)) AS low_price_24h,
    CAST(volume_24h AS DECIMAL(28, 8)) AS volume_24h, 
    extracted_at
FROM raw_source
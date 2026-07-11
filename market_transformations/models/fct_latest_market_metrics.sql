{{ config(materialized='table') }}

WITH staged_data AS (
    SELECT * FROM {{ ref('stg_market_data') }}
),

staged_data_with_row_num AS (
    SELECT 
        transaction_id,
        symbol,
        current_price_usd,
        high_price_24h,
        low_price_24h,
        volume_24h,
        extracted_at,
        ROW_NUMBER() OVER (
            PARTITION BY symbol
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM staged_data
)

SELECT
    transaction_id,
    symbol,
    current_price_usd,
    high_price_24h,
    low_price_24h,
    volume_24h,
    extracted_at
FROM staged_data_with_row_num
WHERE row_num = 1

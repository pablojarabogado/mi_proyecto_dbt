 
WITH cleaned AS (
    SELECT
        date,
        UPPER(base_currency) AS base_currency,
        UPPER(target_currency) AS target_currency,
        exchange_rate
    FROM {{ ref('stg_exchange_rates') }}
    WHERE exchange_rate IS NOT NULL
      AND exchange_rate > 0
)

SELECT * FROM cleaned
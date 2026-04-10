/*
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

SELECT * FROM cleaned*/

select
    date,
    upper(base_currency) as base_currency,
    upper(target_currency) as target_currency,
    exchange_rate
from {{ ref('stg_exchange_rates') }}
where exchange_rate is not null
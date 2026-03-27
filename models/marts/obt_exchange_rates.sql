 
SELECT
    date,
    base_currency,
    target_currency,
    exchange_rate
FROM {{ ref('int_exchange_rates_clean') }}
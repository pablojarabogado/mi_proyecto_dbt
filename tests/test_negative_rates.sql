SELECT *
FROM {{ ref('obt_exchange_rates') }}
WHERE exchange_rate <= 0
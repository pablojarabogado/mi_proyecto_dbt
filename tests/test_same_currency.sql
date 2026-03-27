SELECT *
FROM {{ ref('obt_exchange_rates') }}
WHERE base_currency = target_currency
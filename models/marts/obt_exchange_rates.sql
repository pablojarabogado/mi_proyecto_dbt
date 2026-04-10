 /*
SELECT
    date,
    base_currency,
    target_currency,
    exchange_rate
FROM {{ ref('int_exchange_rates_clean') }}*/
select
    cast(e.date as date) as date,
    e.base_currency,
    e.target_currency,
    e.exchange_rate,

    c.country_name,
    c.region,
    c.subregion,
    c.population

from {{ ref('int_exchange_rates_clean') }} e

left join {{ ref('int_countries_clean') }} c
    on e.target_currency = c.currency_code
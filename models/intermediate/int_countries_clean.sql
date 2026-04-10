select distinct
    upper(currency_code) as currency_code,
    country_name,
    region,
    subregion,
    population
from {{ ref('stg_countries') }}
where currency_code is not null
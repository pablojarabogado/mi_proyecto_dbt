-- models/staging/stg_exchange_rates.sql
/*WITH source AS (
    SELECT
        base AS base_currency,
        date::DATE AS date,
        success,
        "timestamp" AS created_at,
        _airbyte_raw_id,
        _airbyte_extracted_at,
        _airbyte_meta,
        rates
    FROM {{ source('raw', 'exchange_rates') }}
),

exploded AS (
    SELECT
        base_currency,
        date,
        created_at,
        success,
        key AS target_currency,
        value::DOUBLE AS exchange_rate
    FROM source,
         JSON_EACH(rates)  -- convierte JSON en filas (key/value)
)

SELECT *
FROM exploded*/
with source as (

    select * 
    from airbyte_curso.main.exchange_rates

),

flatten as (

    select
        base as base_currency,
        date,
        json_extract(rates, '$') as rates_json
    from source

),

final as (

    select
        base_currency,
        date,
        key as target_currency,
        cast(json_extract(rates_json, '$.' || key) as double) as exchange_rate
    from flatten,
    unnest(json_keys(rates_json)) as t(key)

)

select * from final
-- models/staging/stg_exchange_rates.sql
WITH source AS (
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
FROM exploded
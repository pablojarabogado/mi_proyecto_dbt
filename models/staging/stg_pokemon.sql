with source AS (
    select * from {{source('raw','pokemon')}}
),
renamed AS (
    select 
    id as pokemon_id,
    name as pokemon_name,
    height,
    weight,
    base_experience,
    types,
    _airbyte_extracted_at AS loaded_at
    FROM source
)
SELECT * from renamed
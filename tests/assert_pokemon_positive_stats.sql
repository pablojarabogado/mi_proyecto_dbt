SELECT
pokemon_id,
pokemon_name,
height,
weight,
base_experience
FROM {{ ref('stg_pokemon') }}
WHERE height < 0
OR weight < 0
OR base_experience < 0
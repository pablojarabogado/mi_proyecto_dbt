SELECT
pokemon_id,
pokemon_name,
power_tier,
base_experience
FROM {{ ref('obt_pokemon') }}
WHERE power_tier = 'Legendary'
AND base_experience < 200
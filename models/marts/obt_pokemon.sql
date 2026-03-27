with pokemon AS (
    select * from {{ ref('int_pokemon_with_types') }}
),

final AS (
    select pokemon_id,
         pokemon_name,
         height,
         weight,
         base_experience,
         type_primary,
         type_secundary,
         case
             when base_experience >= 200 THEN 'legendary'
             when base_experience >= 100 THEN 'strong'
             else 'normal'
         end AS power_tier
    from pokemon
) 

select * from final
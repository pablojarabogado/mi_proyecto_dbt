with source as (

    select *
    from airbyte_curso.main.rest_countries

),

final as (

    select
        name->>'common' as country_name,
        region,
        subregion,
        population,
        key as currency_code
    from source,
    unnest(json_keys(currencies)) as t(key)

)

select * from final
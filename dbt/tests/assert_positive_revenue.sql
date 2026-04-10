-- Test: Verificar que no hay revenue negativo
select
    order_id,
    price_usd
from {{ ref('stg_orders') }}
where price_usd < 0

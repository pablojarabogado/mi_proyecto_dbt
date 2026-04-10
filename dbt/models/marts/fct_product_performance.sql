{{ config(materialized='table') }}

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

refunds as (
    select
        oi.product_id,
        count(*) as refund_count,
        sum(r.refund_amount_usd) as refund_amount
    from {{ ref('stg_refunds') }} r
    join order_items oi on r.order_item_id = oi.order_item_id
    group by oi.product_id
)

select
    oi.product_id,
    oi.product_name,
    count(*) as units_sold,
    count(distinct oi.order_id) as orders_with_product,
    sum(case when oi.is_primary_item then 1 else 0 end) as times_as_primary,
    sum(case when not oi.is_primary_item then 1 else 0 end) as times_as_cross_sell,
    sum(oi.price_usd) as revenue,
    sum(oi.cogs_usd) as cogs,
    sum(oi.margin_usd) as margin,
    round(sum(oi.margin_usd) * 100.0 / nullif(sum(oi.price_usd), 0), 2) as margin_pct,
    coalesce(r.refund_count, 0) as refund_count,
    coalesce(r.refund_amount, 0) as refund_amount,
    round(coalesce(r.refund_count, 0) * 100.0 / nullif(count(*), 0), 2) as refund_rate
from order_items oi
left join refunds r on oi.product_id = r.product_id
group by oi.product_id, oi.product_name, r.refund_count, r.refund_amount
order by revenue desc

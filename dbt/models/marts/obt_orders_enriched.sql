{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

sessions as (
    select * from {{ ref('stg_sessions') }}
),

products as (
    select * from {{ source('raw', 'products') }}
),

order_items_agg as (
    select
        order_id,
        count(*) as item_count,
        sum(case when is_primary_item then 0 else 1 end) as cross_sell_count
    from {{ ref('stg_order_items') }}
    group by order_id
),

refunds_agg as (
    select
        order_id,
        count(*) as refund_count,
        sum(refund_amount_usd) as total_refund_amount
    from {{ ref('stg_refunds') }}
    group by order_id
)

select
    -- Order info
    o.order_id,
    o.created_at,
    o.order_date,
    o.order_week,
    o.order_month,
    o.order_year,
    o.user_id,
    o.items_purchased,
    o.price_usd,
    o.cogs_usd,
    o.margin_usd,

    -- Session info
    s.website_session_id,
    s.utm_source,
    s.utm_campaign,
    s.utm_content,
    s.device_type,
    s.http_referer,
    s.is_repeat_session,

    -- Product info
    o.primary_product_id,
    p.product_name as primary_product_name,

    -- Order items aggregations
    coalesce(oia.item_count, 0) as item_count,
    coalesce(oia.cross_sell_count, 0) as cross_sell_count,
    oia.cross_sell_count > 0 as has_cross_sell,

    -- Refund info
    coalesce(ra.refund_count, 0) as refund_count,
    coalesce(ra.total_refund_amount, 0) as refund_amount,
    ra.refund_count > 0 as has_refund,
    o.price_usd - coalesce(ra.total_refund_amount, 0) as net_revenue,

    -- Calculated fields
    case
        when o.price_usd >= 100 then 'High Value'
        when o.price_usd >= 50 then 'Medium Value'
        else 'Low Value'
    end as order_tier,

    case
        when s.utm_source = 'direct' then 'Direct'
        when s.utm_source = 'gsearch' then 'Google Search'
        when s.utm_source = 'bsearch' then 'Bing Search'
        when s.utm_source = 'socialbook' then 'Social Media'
        else 'Other'
    end as channel_group

from orders o
left join sessions s on o.website_session_id = s.website_session_id
left join products p on o.primary_product_id = p.product_id
left join order_items_agg oia on o.order_id = oia.order_id
left join refunds_agg ra on o.order_id = ra.order_id

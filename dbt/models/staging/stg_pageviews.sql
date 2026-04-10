with source as (
    select * from {{ source('raw', 'website_pageviews') }}
)

select
    website_pageview_id,
    created_at,
    website_session_id,
    pageview_url,
    date_trunc('day', created_at) as pageview_date
from source

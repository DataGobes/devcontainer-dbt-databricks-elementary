with src as (
    select
        ga_campaign_key as `GA Campaign Key`,
        campaign_id as `Campaign ID`,
        campaign_name as `Campaign Name`

    from {{ ref('dim_ga4_doms_ga_campaign') }}
)

select * from src

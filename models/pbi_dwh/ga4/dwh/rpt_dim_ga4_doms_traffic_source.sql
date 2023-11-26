with src as (
    select
        traffic_source_key as `Traffic Source Key`,
        session_source as `Session Source`,
        session_medium as `Session Medium`,
        custom_channel_grouping as `Custom Channel Grouping`

    from {{ ref('dim_ga4_doms_traffic_source') }}
)

select * from src

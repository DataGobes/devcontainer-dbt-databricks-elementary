with collected_traffic_source as (
    select *
    from {{ ref('enr_ga4_domestic_collected_traffic_source') }}
),

ga_campaigns as (
    select distinct
        manual_campaign_id,
        first_value(manual_campaign_name, true) over (order by event_date desc) as most_recent_campaign_name
    from collected_traffic_source
),

add_key as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['manual_campaign_id']) }} as ga_campaign_key
    from ga_campaigns
),

final as (
    select
        ga_campaign_key,
        manual_campaign_id as campaign_id,
        most_recent_campaign_name as campaign_name,
        current_timestamp() as meta_insert_ts
    from add_key
)

select * from final

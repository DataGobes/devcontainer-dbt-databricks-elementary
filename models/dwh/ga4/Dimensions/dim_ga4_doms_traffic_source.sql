with attributed_traffic_source as (
    select distinct
        traffic_source_key,
        session_source,
        session_medium
    from {{ ref('enr_ga4_attributed_session_traffic_source') }}
),

add_custom_channel_grouping as (
    select
        *,
        {{ custom_channel_grouping('session_source','session_medium') }} as custom_channel_grouping
    from attributed_traffic_source
),

final as (
    select
        traffic_source_key,
        session_source,
        session_medium,
        custom_channel_grouping,
        current_timestamp() as meta_insert_ts
    from add_custom_channel_grouping
)

select * from final

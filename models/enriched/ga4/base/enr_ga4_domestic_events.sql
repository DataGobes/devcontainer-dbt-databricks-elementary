with source as (
    select * from {{ source('src_ga4','ga4_domestic_events') }}
),

params as (
    select * from {{ ref('enr_ga4_domestic_event_parameters') }}
    where event_parameter = 'ga_session_id'
),

ref_property as (
    select * from {{ ref('dim_ga_property_id') }}
),

add_ts as (
    select
        source.*,
        {{ event_key() }} as event_key,
        from_unixtime(
            source.event_timestamp / 1000000, 'yyyy-MM-dd HH:mm:ss'
        ) as event_timestamp_utc,
        ref_property.`Timezone` as timezone,
        from_utc_timestamp(
            from_unixtime(source.event_timestamp / 1000000),
            ref_property.`Timezone`
        ) as event_timestamp_local
    from source
    left outer join ref_property
        on source.property_id = ref_property.property_id
),

add_keys as (
    select
        events.*,
        {{ dbt_utils.generate_surrogate_key(['events.user_pseudo_id', 
                                    'events.stream_id']) 
        }} as client_key,
        {{ dbt_utils.generate_surrogate_key(['events.user_pseudo_id', 
                                    'events.stream_id', 
                                    'params.parameter_value']) 
        }} as session_key
    from add_ts as events
    left join params
        on events.event_key = params.event_key
),

final as (
    select
        event_key,
        client_key,
        session_key,
        event_date,
        event_timestamp_local,
        event_timestamp_utc,
        timezone,
        event_timestamp as event_timestamp_unix,
        event_name,
        user_pseudo_id,
        user_first_touch_timestamp,
        stream_id,
        platform,
        property_id,
        vg,
        current_timestamp() as meta_insert_ts
    from add_keys
)

select * from final

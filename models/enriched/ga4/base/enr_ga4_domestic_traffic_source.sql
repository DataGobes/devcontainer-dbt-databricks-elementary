with source as (
    select * from {{ source('src_ga4','ga4_domestic_events') }}
),

add_array as (
    select
        event_date,
        event_timestamp,
        event_name,
        event_params,
        user_pseudo_id,
        stream_id,
        {{ jsonstring_to_array('traffic_source') }} as traffic_source_array
    from source
),

flattened as (
    select
        *,
        traffic_source_array[0] as traffic_name,
        traffic_source_array[1] as traffic_medium,
        traffic_source_array[2] as traffic_source
    from add_array
),

add_keys as (
    select
        *,
        {{ event_key() }} as event_key,
        {{ dbt_utils.generate_surrogate_key(['traffic_name',
                                    'traffic_medium',
                                    'traffic_source']) 
        }} as traffic_source_key
    from flattened
),

final as (
    select
        event_key,
        traffic_source_key,
        event_date,
        event_name,
        traffic_name,
        traffic_medium,
        traffic_source,
        current_timestamp() as meta_insert_ts
    from add_keys
)

select * from final

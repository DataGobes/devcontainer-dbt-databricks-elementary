with source as (
    select * from {{ source('src_ga4','ga4_domestic_events') }}
),

transform_to_array as (
    select
        *,
        {{ jsonstring_to_array('collected_traffic_source') }} as col_ts_array -- noqa
    from source
),

flattened as (
    select
        *,
        col_ts_array[0] as manual_campaign_id,
        col_ts_array[1] as manual_campaign_name,
        col_ts_array[2] as manual_source,
        col_ts_array[3] as manual_medium,
        col_ts_array[4] as manual_term,
        col_ts_array[5] as manual_content,
        col_ts_array[6] as gclid,
        col_ts_array[7] as dclid,
        col_ts_array[8] as srsltid
    from transform_to_array
),

add_keys as (
    select
        *,
        {{ event_key() }} as event_key,
    {{ dbt_utils.generate_surrogate_key(['manual_campaign_id', 
                                'manual_campaign_name', 
                                'manual_source', 
                                'manual_medium', 
                                'manual_term', 
                                'gclid',
                                'dclid',
                                'srsltid']) 
    }} as collected_traffic_source_key
    from flattened
),

final as (
    select
        event_key,
        collected_traffic_source_key,
        event_date,
        event_name,
        manual_campaign_id,
        manual_campaign_name,
        manual_source,
        manual_medium,
        manual_term,
        manual_content,
        gclid,
        dclid,
        srsltid,
        current_timestamp() as meta_insert_ts
    from add_keys
)

select * from final

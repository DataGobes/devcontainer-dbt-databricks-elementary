with source as (
    select * from {{ source('src_ga4','ga4_domestic_events') }}
),

transform_to_array as (
    select
        *,
        {{ jsonstring_to_array('device') }} as device_array
    from source
),

flattened as (
    select
        *,
        device_array[0] as device_category,
        device_array[1] as mobile_brand_name,
        device_array[2] as mobile_model_name,
        device_array[3] as mobile_marketing_name,
        device_array[4] as mobile_os_hardware_model,
        device_array[5] as operating_system,
        device_array[6] as operating_system_version,
        device_array[7] as vendor_id,
        device_array[8] as advertising_id,
        device_array[9] as device_language,
        device_array[10] as time_zone_offset_seconds,
        device_array[11] as is_limited_ad_tracking
    from transform_to_array
),

add_keys as (
    select
        *,
        {{ event_key() }} as event_key,
        {{ dbt_utils.generate_surrogate_key(['device_category', 
                                    'mobile_brand_name', 
                                    'mobile_model_name', 
                                    'operating_system', 
                                    'operating_system_version', 
                                    'device_language',
                                    'mobile_marketing_name']) 
        }} as device_key
    from flattened
),

final as (
    select
        event_key,
        device_key,
        event_date,
        event_name,
        device_category,
        mobile_brand_name,
        mobile_model_name,
        mobile_marketing_name,
        mobile_os_hardware_model,
        operating_system,
        operating_system_version,
        vendor_id,
        advertising_id,
        device_language,
        time_zone_offset_seconds,
        is_limited_ad_tracking,
        current_timestamp() as meta_insert_ts
    from add_keys
)

select * from final

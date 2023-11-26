with device as (
    select distinct
        device_key,
        device_category,
        mobile_brand_name,
        mobile_model_name,
        operating_system,
        operating_system_version,
        device_language,
        current_timestamp() as meta_insert_ts
    from {{ ref('enr_ga4_domestic_device') }}
)

select * from device

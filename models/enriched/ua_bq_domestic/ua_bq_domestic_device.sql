
{{ config (
   materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id', 'visit_id', 'full_visitor_id', 'visit_date'],   
    tags=["uabq_doms_enriched"]
          ) 
}}

with source as  (
    select *
    from  {{ source('src_ua_bq_raw_domestic', 'ua_bq_domestic_device') }}
        {% if is_incremental() %}
            where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
        {% endif %} 
)

select  
     visit_date
    ,visitId as visit_id
    ,fullVisitorId as full_visitor_id
    ,browser
    ,browserVersion as browser_version
    ,browserSize as browser_size
    ,operatingSystem as operating_system
    ,operatingSystemVersion as operating_system_version
    ,isMobile as is_mobile
    ,mobileDeviceBranding as mobile_device_branding
    ,mobileDeviceModel as mobile_device_model
    ,mobileInputSelector as mobile_input_selector
    ,mobileDeviceInfo as mobile_device_info
    ,mobileDeviceMarketingName as mobile_device_marketing_name
    ,flashVersion as flash_version
    ,javaEnabled as java_enabled
    ,device_language as device_language
    ,screenColors as screen_colors
    ,screenResolution as screen_resolution
    ,deviceCategory as device_category
    ,view_id
    ,vg
    ,{{ dbt_utils.generate_surrogate_key([   'browser',
                                    'browser_version',
                                    'browser_size',
                                    'operating_system',
                                    'operating_system_version',
                                    'is_mobile',
                                    'mobile_device_branding',
                                    'mobile_device_model',
                                    'mobile_input_selector',
                                    'mobile_device_info',
                                    'mobile_device_marketing_name',
                                    'flash_version',
                                    'device_language',
                                    'screen_colors',
                                    'screen_resolution',
                                    'device_category',
                                    'java_enabled'
                                ]) 
      }} as device_key
      ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'visit_date']) }} as session_key
      ,meta_source
      ,current_timestamp() as meta_insert_ts 
from source
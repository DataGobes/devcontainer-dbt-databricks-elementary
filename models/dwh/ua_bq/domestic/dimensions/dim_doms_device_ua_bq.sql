{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['device_key'],   
    tags=["uabq_doms_dwh"]   
          ) 
}}

with device as  (
    select *
    from  {{ ref('ua_bq_domestic_device') }}
            {% if is_incremental() %}
              where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
            {% endif %} 
),

ga_view_id as (
    select *
    from  {{ ref('dim_ga_view_id') }}
)

Select distinct
d.device_key
,d.browser
,d.browser_version
,d.browser_size
,d.operating_system
,d.operating_system_version
,d.is_mobile
,coalesce(d.mobile_device_branding,'Empty') as mobile_device_branding
,coalesce(d.mobile_device_model,'Empty') as mobile_device_model
,coalesce(d.mobile_input_selector,'Empty') as mobile_input_selector
,d.mobile_device_info
,coalesce(d.mobile_device_marketing_name,'Empty') as mobile_device_marketing_name
,d.flash_version
,d.java_enabled
,d.device_language
,d.screen_colors
,d.screen_resolution
,d.device_category
,d.meta_insert_ts

from device d inner join ga_view_id vid
on d.view_id = vid.view_id
and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
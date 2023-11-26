with src as (
    select distinct
        device_key as `Device ID` ,
        device_category  as `Device Category` ,
        browser as `Device - Browser`,
        browser_version as `Device - Browser Version`,
        browser_size as `Device - Browser Size`,
        operating_system as `Device - Operating System`,
        operating_system_version as `Device - Operating System Version`,
        is_mobile as `Is Mobile Device`,
        mobile_device_branding as `Mobile Device Branding`,
        mobile_device_model as `Mobile Device Model`,
        mobile_input_selector  as `Mobile Input Selector`,
        mobile_device_info as `Mobile Device Info`,
        mobile_device_marketing_name as `Mobile Device Marketing Name`,
        flash_version as `Device - Flash Version`,
        java_enabled as `Device - Is Java Enabled`,
        device_language as `Device Language`,
        screen_colors as `Device - Screen Colors`,
        screen_resolution as `Device - Screen Resolution`
    from {{ ref('dim_doms_device_ua_bq') }}
)

select * from src


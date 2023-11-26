with src as (
    select
        device_key as `Device Key`,
        device_category as `Device Category`,
        mobile_brand_name as `Mobile Brand Name`,
        mobile_model_name as `Mobile Model Name`,
        operating_system as `Operating System`,
        operating_system_version as `Operating System Version`,
        device_language as `Device Language`

    from {{ ref('dim_ga4_doms_device') }}
)

select * from src

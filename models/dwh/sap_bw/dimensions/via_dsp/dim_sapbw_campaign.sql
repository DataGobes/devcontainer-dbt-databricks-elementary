{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta" 
) }}
      
with source as (
    select *
    from {{ ref('enr_sapbw_campaign') }}
    where language_id = 1 
),


sk as (
    select  
        reporting_unit_code,
        campaign_code,
        `language`,
        text_short,
        text_medium,
        text_long
    from source 
    union
    select  
        'Not Set' as reporting_unit_code,
        'Not Set' as campaign_code,
        'Not Set' as `language`,
        'Not Set' as text_short,
        'Not Set' as text_medium,
        'Not Set' as text_long
),

final as (

    select 
        *,
        current_timestamp() as meta_insert_ts  
    from sk
)

select * from final
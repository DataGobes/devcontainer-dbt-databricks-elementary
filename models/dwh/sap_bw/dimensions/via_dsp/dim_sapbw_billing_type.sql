{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}
      
with source as (

    select *
    from {{ ref('enr_sapbw_billing_type') }}
    where language = 'E'
    and  billing_type_code != 'Not Set'
),

add_not_set as (

    select 
        reporting_unit_code,
        billing_type_code,
        text_short,
        text_medium,
        text_long
    from source   
    union
    select  
        'Not Set' as reporting_unit_code,
        'Not Set' as billing_type_code,
        'Not Set' as text_short,
        'Not Set' as text_medium,
        'Not Set' as text_long
-- Code 'Z001' not present in dimension source
    union
    select  
        'Not Set'   as reporting_unit_code,
        'Z001'      as billing_type_code,
        'Z001'      as text_short,
        'Z001'      as text_medium,
        'Z001'      as text_long
),

final as (

    select 
        *,
        current_timestamp() as meta_insert_ts  
    from add_not_set
)

select * from final
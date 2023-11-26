{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"  
) }}
      
with source as (

    select *
    from {{ ref('enr_sapbw_soldto_bp_type') }}
    where language = 'E'
    and soldto_business_partner_type_code != 'Not Set'
),

add_not_set as (

    select 
        reporting_unit_code,
        soldto_business_partner_type_code,
        text_short,
        text_medium,
        text_long
    from source   
    union
    select  
        'Not Set' as reporting_unit_code,
        'Not Set' as soldto_business_partner_type_code,
        'Not Set' as text_short,
        'Not Set' as text_medium,
        'Not Set' as text_long
),

final as (

    select 
        *,
        current_timestamp() as meta_insert_ts  
    from add_not_set
)

select * from final
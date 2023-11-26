{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}
      
with source as (

    select *
    from {{ ref('enr_sapbw_soldto_industry') }}
    where language = 'E'
),

add_not_set as (

    select 
        customer_soldto_industry_miele_code,
        text_short,
        text_medium,
        text_long
    from source   
    union
    select  
        'Not Set' as customer_soldto_industry_miele_code,
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
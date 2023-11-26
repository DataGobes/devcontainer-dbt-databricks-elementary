{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}
      
with source as (

    select *
    from {{ ref('enr_sapbw_org_unit') }}
    where language = 'E'
),


orderd_org_unit as (
    select * ,
    row_number() over (partition by org_unit order by date_from desc, date_to desc) as rn 
    from source
),

add_not_set as (

    select       
        org_unit,
        text_short,
        text_medium,
        text_long,
        date_from,
        date_to,
        rn as org_unit_order
    from orderd_org_unit
    union
    select  
        'Not Set' as org_unit,
        'Not Set' as text_short,
        'Not Set' as text_medium,
        'Not Set' as text_long,
        10000101  as date_from,       
        99991231  as date_to,
        1         as org_unit_order
),

final as (

    select 
        *,
        current_timestamp() as meta_insert_ts  
    from add_not_set
)

select * from final
{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}
      
with source as (

    select *
    from {{ ref('enr_sapbw_material') }}
    where language_id = 1
),

add_not_set as (

    select 
        material_code,
        language,
        text_short,
        text_medium,
        text_long
    from source   
    union
    select  
        'Not Set' as material_code,
        'Not Set' as language,
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

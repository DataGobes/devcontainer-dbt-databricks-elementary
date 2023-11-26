{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta" 
) }}
      
with source as (

    select *
    from {{ ref('enr_sapbw_org_distribution_channel') }}
    where language = 'E'
),

add_not_set as (

    select 
        distribution_channel_code,
        text_short
    from source   
    union
    select  
        'Not Set' as distribution_channel_code,
        'Not Set' as text_short
),

final as (

    select 
        *,
        current_timestamp() as meta_insert_ts  
    from add_not_set
)

select * from final

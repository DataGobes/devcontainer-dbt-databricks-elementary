{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}
      
with source as (

    select *
    from {{ ref('enr_sapbw_org_consumer_classification_chain') }}
    where language = 'E'
),

add_not_set as (

    select 
        org_consumer_classification_chain_code,
        text_short,
        text_medium,
        text_long
    from source   
    union
    select  
        'Not Set' as org_consumer_classification_chain_code,
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
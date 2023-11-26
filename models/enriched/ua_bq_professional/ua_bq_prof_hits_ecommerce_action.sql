{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id', 'visit_id', 'full_visitor_id', 'hit_number']   
          ) 
}}

with source AS  (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_hits_ecommerce_action') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %} 

                )

select 
     visit_date
    ,visitId as visit_id
    ,fullVisitorId as full_visitor_id
    ,hitNumber as hit_number
    ,cast(action_type as int) as ecommerce_action_type
    ,cast(step as int) as ecommerce_step
    ,ecommerce_option
    ,view_id
    ,meta_source
    ,vg
    ,current_timestamp() as meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} as hit_key
 from source
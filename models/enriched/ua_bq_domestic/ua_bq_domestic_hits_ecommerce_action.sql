{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hit_key'],
    tags=["uabq_doms_enriched"] 
          ) 
}}

with source AS  (
    select * 
    from {{ source('src_ua_bq_raw_domestic', 'ua_bq_domestic_hits_ecommerce_action') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %} 

                )

select 
     visit_date
    ,visitId AS visit_id
    ,fullVisitorId AS full_visitor_id
    ,hitNumber AS hit_number
    ,cast(action_type as int) AS ecommerce_action_type
    ,cast(step as int) AS ecommerce_step
    ,ecommerce_option
    ,view_id
    ,meta_source
    ,VG
    ,current_timestamp() AS meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} AS hit_key
 from source
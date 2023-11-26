{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id','visit_id', 'full_visitor_id', 'visit_date', 'cmdm_index']   
          ) 
}}

with source as (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_sessions_custom_dimensions') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %}

               )

select 
    visit_date
    ,visitId as visit_id
    ,fullVisitorId as full_visitor_id
    ,cmdm_index
    ,cmdm_value
    ,view_id
    ,meta_source
    ,vg
    ,current_timestamp() as meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'visit_date', 'cmdm_index']) }} as session_key
 from source
{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id','visit_id', 'full_visitor_id', 'hit_number']   
          ) 
}}

with source as  (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_hits_eventinfo') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %} 

                )

select 
    visit_date
    ,visitId as visit_id
    ,fullVisitorId as full_visitor_id
    ,hitNumber as hit_number
    ,eventCategory as event_category
    ,eventAction as event_action
    ,eventLabel as event_label
    ,view_id
    ,meta_source
    ,vg
    ,current_timestamp() as meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} as hit_key
    ,{{ dbt_utils.generate_surrogate_key(['event_category', 'event_action', 'event_label']) }} as event_key
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'visit_date']) }} as session_key  
 from source
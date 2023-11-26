{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hit_key'],
    tags=["uabq_doms_enriched"] 
          ) 
}}

with source AS  (
    select * 
    from {{ source('src_ua_bq_raw_domestic', 'ua_bq_domestic_hits_eventinfo') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %} 

                )

select 
    visit_date
    ,visitId AS visit_id
    ,fullVisitorId AS full_visitor_id
    ,hitNumber AS hit_number
    ,eventCategory AS event_category
    ,eventAction AS event_action
    ,eventLabel AS event_label
    ,view_id
    ,meta_source
    ,VG
    ,current_timestamp() as meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} AS hit_key
    ,{{ dbt_utils.generate_surrogate_key(['event_category', 'event_action', 'event_label']) }} AS event_key
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'visit_date']) }} AS session_key  
 from source
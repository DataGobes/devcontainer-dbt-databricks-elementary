{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id','visit_id', 'full_visitor_id', 'hit_number']   
          ) 
}}

with source  (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_hits') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %}

               )

select 
     visit_date
    ,visitId as visit_id
    ,fullVisitorId as full_visitor_id
    ,hitNumber as hit_number
    ,visit_time
    ,visit_hour
    ,visit_minute
    ,isInteraction as is_interaction
    ,isEntrance as is_entrance
    ,isExit as is_exit
    ,referer 
    ,hit_type
    ,uses_transient_token
    ,dataSource as hit_data_source
    ,view_id
    ,meta_source
    ,vg
    ,current_timestamp() as meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'visit_date']) }} as session_key
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} as hit_key
from source
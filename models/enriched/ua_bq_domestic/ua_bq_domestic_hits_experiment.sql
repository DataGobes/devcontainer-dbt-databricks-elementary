{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hit_key', 'experiment_id'],
    tags=["uabq_doms_enriched"]
          ) 
}}

with source  (
    select * 
    from {{ source('src_ua_bq_raw_domestic', 'ua_bq_domestic_hits_experiment') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %}

               )

select 
  visit_date
  ,visitId as visit_id
  ,fullVisitorId as full_visitor_id
  ,hitNumber as hit_number
  ,experimentid as experiment_id 
  ,experimentvariant as experiment_variant 
  ,view_id
  ,meta_source
  ,vg
  ,current_timestamp() as meta_insert_ts
  ,{{ dbt_utils.generate_surrogate_key(['experiment_id', 'experiment_variant']) }} as experiment_key
  ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} as hit_key
from source
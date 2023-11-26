{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id','visit_id', 'full_visitor_id', 'visit_date']   
          ) 
}}

with source as (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_traffic_source') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %} 
               )

,dedup as (
    select distinct
       visit_date
      ,visitId as visit_id
      ,fullVisitorId as full_visitor_id
      ,referralPath as referral_path
      ,campaign
      ,source
      ,medium
      ,keyword
      ,adContent as ad_content
      ,isTrueDirect as is_true_direct
      ,view_id
      ,meta_source
      ,vg
    from source
)

select 
   *
  ,current_timestamp() as meta_insert_ts
  ,{{ dbt_utils.generate_surrogate_key(['referral_path','campaign', 'source', 'medium', 'keyword', 'ad_content']) }} as traffic_source_key
 from dedup
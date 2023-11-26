{{ config (
    tags=["uabq_doms_enriched"]   
          ) 
}}

with source AS (
    select * 
    from {{ source('src_ua_bq_raw_domestic', 'ua_bq_domestic_traffic_source') }}
                
               )

select 
     visit_date
    ,visitId as visit_id
    ,fullVisitorId as full_visitor_id
    ,referralPath as referral_path
    ,campaign
    ,source
    ,medium
    ,keyword
    ,adContent as ad_content
    ,coalesce(cast(isTrueDirect as STRING), '') as is_true_direct
    ,view_id
    ,vg
    ,{{ dbt_utils.generate_surrogate_key(['referral_path','campaign', 'source', 'medium', 'keyword', 'ad_content', 'is_true_direct']) }} as traffic_source_key
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'visit_date']) }} as session_key
    ,meta_source
    ,current_timestamp() as meta_insert_ts
 from source
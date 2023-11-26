{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['visit_date','view_id','visit_id', 'full_visitor_id', 'hit_number', 'host_name', 'page_path', 'page_title', 'search_keyword', 'page_type']   
          ) 
}}


with source as  (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_hits_page') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %}
),

source2 as  (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_hits_custom_dimensions') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %}
),

hits_page as (
    select 
        hp.visit_date
        ,hp.visitId as visit_id
        ,hp.fullVisitorId as full_visitor_id
        ,hp.hitNumber as hit_number
        ,pagePath as page_path
        ,hostname as host_name
        ,pageTitle as page_title
        ,searchKeyword as search_keyword
        ,case when hcd.cmdm_value is null or hcd.cmdm_value = '' 
            then 'Not Set'  
            else hcd.cmdm_value
        end as page_type 
        ,pagePathLevel1 as page_path_level1
        ,pagePathLevel2 as page_path_level2
        ,pagePathLevel3 as page_path_level3
        ,pagePathLevel4 as page_path_level4
        ,hp.view_id as view_id
        ,hp.meta_source as meta_source
        ,hp.VG as VG
        ,current_timestamp() as meta_insert_ts
    from source hp
    left join source2 hcd
        on hp.view_id=hcd.view_id
        and hp.visit_date=hcd.visit_date
        and hp.visitId=hcd.visitId
        and hp.fullVisitorId =  hcd.fullVisitorId
        and hp.hitNumber =hcd.hitNumber  
)

select 
    * 
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'visit_date']) }} as session_key
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} as hit_key
    ,{{ dbt_utils.generate_surrogate_key(['host_name', 'page_path', 'page_title', 'search_keyword', 'page_type']) }} as page_key
from hits_page
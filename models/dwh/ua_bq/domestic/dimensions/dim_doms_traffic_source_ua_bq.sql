{{ config (   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with traffic_source as  (
    select *
    from  {{ ref('ua_bq_domestic_traffic_source') }}
           
),

ga_view_id as (
    select *
    from  {{ ref('dim_ga_view_id') }}
)

Select distinct
traffic_source_key
,referral_path
,campaign
,source
,medium
,keyword
,ad_content
,is_true_direct
,d.meta_insert_ts
from traffic_source d inner join ga_view_id vid
on d.view_id = vid.view_id
and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd



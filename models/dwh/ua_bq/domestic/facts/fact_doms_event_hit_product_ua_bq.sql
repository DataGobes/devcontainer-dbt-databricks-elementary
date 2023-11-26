{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hit_key', 'event_key', 'product_ua_bq_key'],
    tags=["uabq_doms_dwh"]     
          ) 
}}
 

with ga_view_id as (
    select s.*
    from  {{ ref('dim_ga_view_id') }} s
),

 cte_session as  (
    select d.*
    from  {{ ref('ua_bq_domestic_sessions') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

 product as  (
    select distinct d.view_id, d.hit_key, d.product_ua_bq_key
    from  {{ ref('ua_bq_domestic_hits_product') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

 hits as  (
    select d.*
    from  {{ ref('ua_bq_domestic_hits') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

 hits_eventinfo as  (
    select d.*
    from  {{ ref('ua_bq_domestic_hits_eventinfo') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
    {% if is_incremental() %}
              where d.meta_insert_ts > (select max(meta_insert_ts) from {{ this }}) 
    {% endif %}    
),

 hits_page as  (
    select d.*
    from  {{ ref('ua_bq_domestic_hits_page') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),


traffic_source as  (
    select d.*
    from  {{ ref('ua_bq_domestic_traffic_source') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
           
),

device as  (
    select d.*
    from  {{ ref('ua_bq_domestic_device') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
           
)

select
   hei.vg
  ,hei.view_id
  ,hei.full_visitor_id
  ,s.visit_date
  ,s.session_key
  ,s.channel_grouping
  ,tf.source as traffic_source
  ,tf.medium as traffic_medium
  ,d.device_category
  ,hei.hit_key
  ,hei.event_key
  ,hp.page_key
  ,hpr.product_ua_bq_key
  ,htime.hit_time_key
  ,hei.hit_number
  ,current_timestamp() as meta_insert_ts
from hits_eventinfo hei
inner join hits_page hp
on  hp.hit_key          = hei.hit_key
and hp.view_id          = hei.view_id
and hp.visit_date       = hei.visit_date
inner join product hpr
on  hei.hit_key         = hpr.hit_key
and hei.view_id         = hpr.view_id
inner join cte_session s
on  hei.session_key     = s.session_key
and hei.view_id         = s.view_id
and hei.visit_date      = s.visit_date
inner join hits htime
on hei.hit_key   = htime.hit_key
and hei.view_id  = htime.view_id
left join traffic_source tf 
on  tf.visit_id = hei.visit_id 
and tf.full_visitor_id = hei.full_visitor_id
and tf.visit_date = hei.visit_date
and tf.view_id = hei.view_id
left join device d 
on  d.visit_id = hei.visit_id 
and d.full_visitor_id = hei.full_visitor_id
and d.visit_date = hei.visit_date
and d.view_id = hei.view_id
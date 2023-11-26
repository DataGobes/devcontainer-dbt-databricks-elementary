{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hit_key'],
    tags=["uabq_doms_dwh"]     
          ) 
}}

with ga_view_id as (
    select *
    from  {{ ref('dim_ga_view_id') }} s
),

cte_session as  (
    select d.*
    from  {{ ref('ua_bq_domestic_sessions') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd 

),

hits_filtered as  (
    select d.*
    from  {{ ref('ua_bq_domestic_hits') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
    where d.hit_type = 'PAGE'

),

hits as  (
    select d.*
    from  hits_filtered d
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
  hp.vg
  ,h.view_id
  ,s.visit_date
  ,h.hit_key
  ,hp.page_key
  ,s.session_key
  ,ts.traffic_source_key
  ,d.device_key
  ,h.hit_number
  ,h.visit_time
  ,h.visit_hour
  ,h.visit_minute
  ,cast(coalesce((LEAD (h.visit_time) OVER ( PARTITION BY hp.view_id,hp.visit_date,hp.visit_id,hp.full_visitor_id ORDER BY hp.hit_number ) - h.visit_time)/1000,0) as float)  time_on_page
  ,1 AS pageviews
  ,h.is_entrance
  ,h.is_exit
  ,s.channel_grouping
  ,d.device_category
  ,h.meta_insert_ts
from hits h
inner join hits_page hp
  on hp.visit_id = h.visit_id
  and hp.hit_number = h.hit_number
  and hp.full_visitor_id = h.full_visitor_id
  and hp.view_id= h.view_id
  and hp.visit_date=h.visit_date
inner join cte_session s
  on h.visit_id = s.visit_id
  and h.full_visitor_id = s.full_visitor_id
  and h.view_id= s.view_id
  and h.visit_date=s.visit_date
left outer join traffic_source ts
  on ts.visit_id = s.visit_id 
  and ts.full_visitor_id = s.full_visitor_id
  and ts.visit_date = s.visit_date
  and ts.view_id = s.view_id
left outer join device d
  on  d.visit_id = s.visit_id 
  and d.full_visitor_id = s.full_visitor_id
  and d.visit_date = s.visit_date
  and d.view_id = s.view_id 


with ga_view_id as (
    select s.*
    from  {{ ref('dim_ga_view_id') }} s
),

 cte_session as  (
    select s.*
    from  {{ ref('ua_bq_domestic_sessions') }} s
),



traffic_source as  (
    select s.*
    from  {{ ref('ua_bq_domestic_traffic_source') }} s
           
),


device as  (
     select s.*
     from  {{ ref('ua_bq_domestic_device') }} s
           
)



select
 s.vg
,s.visit_date as session_date
,s.channel_grouping as traffic_channel
,tf.traffic_source_key as traffic_source_id
,d.device_category
,sum(s.visits) as sessions
,sum(s.bounces) as bounces
,sum(s.pageviews) as page_views
,sum(s.time_on_site) as session_duration
,current_timestamp() as meta_insert_ts
from cte_session s
join traffic_source tf 
  on  tf.visit_id = s.visit_id 
  and tf.full_visitor_id = s.full_visitor_id
  and tf.visit_date = s.visit_date
  and tf.view_id = s.view_id
join device d 
  on  d.visit_id = s.visit_id 
  and d.full_visitor_id = s.full_visitor_id
  and d.visit_date = s.visit_date
  and d.view_id = s.view_id
inner join ga_view_id vid
  on s.view_id = vid.view_id
  and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
where s.visit_date > 20210101
group by 1,2,3,4,5



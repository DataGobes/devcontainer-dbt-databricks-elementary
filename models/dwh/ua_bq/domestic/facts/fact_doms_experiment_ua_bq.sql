{{ config (   
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

cte_experiment as  (
    select d.*
    from  {{ ref('ua_bq_domestic_hits_experiment') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd 

),

cte_hit as  (
    select d.hit_key, hit_time_key
    from  {{ ref('ua_bq_domestic_hits') }} d
    inner join ga_view_id vid
      on d.view_id = vid.view_id
      and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd 

)

select
 e.vg
,s.view_id
,s.visit_date
,s.session_key
,e.hit_key
,h.hit_time_key
,e.experiment_key
,e.hit_number
,current_timestamp() as meta_insert_ts
from cte_experiment e
inner join cte_session s
    on s.view_id=e.view_id
    and s.visit_date=e.visit_date
    and s.visit_id=e.visit_id
    and s.full_visitor_id=e.full_visitor_id
left join cte_hit h on e.hit_key = h.hit_key
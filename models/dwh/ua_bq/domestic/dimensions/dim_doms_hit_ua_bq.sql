{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hit_key'],   
    tags=["uabq_doms_dwh"]     
          ) 
}}


with ga_view_id as (
    select *
    from  {{ ref('dim_ga_view_id') }}
),

hit_session as  (
    select s.*
    from  {{ ref('ua_bq_domestic_sessions') }} s
     inner join ga_view_id vid
      on s.view_id = vid.view_id
      and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
            {% if is_incremental() %}
              where s.meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
            {% endif %} 
),



hits as  (
    select s.*
    from  {{ ref('ua_bq_domestic_hits') }} s
    inner join ga_view_id vid
      on s.view_id = vid.view_id
      and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
            {% if is_incremental() %}
              where s.meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
            {% endif %} 
),



ecommerce_action as  (
    select s.*
    from  {{ ref('ua_bq_domestic_hits_ecommerce_action') }} s
    inner join ga_view_id vid
      on s.view_id = vid.view_id
      and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
            {% if is_incremental() %}
              where s.meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
            {% endif %} 
)




Select   
   h.vg
  ,h.view_id
  ,s.visit_date
  ,h.hit_key
  ,h.hit_number
  ,h.is_interaction
  ,h.is_entrance
  ,h.is_exit
  ,h.referer
  ,h.hit_type
  ,h.uses_transient_token
  ,h.hit_data_source
  ,hea.ecommerce_action_type
  ,hea.ecommerce_step
  ,hea.ecommerce_option 
  ,h.meta_insert_ts
from hits h
inner join hit_session s
on  h.visit_id = s.visit_id
and h.full_visitor_id = s.full_visitor_id
and h.view_id = s.view_id
and h.visit_date = s.visit_date
left outer join ecommerce_action hea
on h.hit_key= hea.hit_key
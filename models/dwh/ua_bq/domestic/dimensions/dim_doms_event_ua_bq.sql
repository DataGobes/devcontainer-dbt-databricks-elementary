{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['event_key'],   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with eventinfo as  (
    select *
    from  {{ ref('ua_bq_domestic_hits_eventinfo') }}
            {% if is_incremental() %}
              where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
            {% endif %} 
),

ga_view_id as (
    select *
    from  {{ ref('dim_ga_view_id') }}
)

Select distinct
event_key
,event_category
,event_action
,event_label
,d.meta_insert_ts
from eventinfo d inner join ga_view_id vid
on d.view_id = vid.view_id
and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd

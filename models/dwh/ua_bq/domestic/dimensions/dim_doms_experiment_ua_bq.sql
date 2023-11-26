{{ config (   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with experiment as  (
    select *
    from  {{ ref('ua_bq_domestic_hits_experiment') }}
),

ga_view_id as (
    select *
    from  {{ ref('dim_ga_view_id') }}
)

select distinct
    d.experiment_key
    ,d.experiment_id
    ,d.experiment_variant
    ,d.meta_insert_ts
from experiment d inner join ga_view_id vid
on d.view_id = vid.view_id
and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd

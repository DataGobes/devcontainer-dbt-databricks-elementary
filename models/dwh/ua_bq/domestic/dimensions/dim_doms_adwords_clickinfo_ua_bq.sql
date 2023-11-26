{{ config (   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with adwords_clickinfo as (
    select *
    from {{ ref('ua_bq_domestic_adwords_clickinfo') }}

),

ga_view_id as (
    select *
    from {{ ref('dim_ga_view_id') }}
)

select distinct
    d.click_info_key,
    d.campaignid,
    d.adgroupid,
    d.adnetworktype,
    d.creativeid,
    d.criteriaid,
    d.criteriaparameters,
    d.customerid,
    d.gclid,
    d.isvideoad,
    d.page_number,
    d.slot,
    d.meta_insert_ts
from adwords_clickinfo as d inner join ga_view_id as vid
    on
        d.view_id = vid.view_id
        and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd

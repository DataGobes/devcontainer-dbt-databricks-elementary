{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['session_key'],   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with cte_session as (
    select *
    from {{ ref('ua_bq_domestic_sessions') }}
    {% if is_incremental() %}
        where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
    {% endif %}
),

ga_view_id as (
    select *
    from {{ ref('dim_ga_view_id') }}
),

traffic_source as (
    select s.*
    from {{ ref('ua_bq_domestic_traffic_source') }} as s
    inner join ga_view_id as vid
        on
            s.view_id = vid.view_id
            and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
    {% if is_incremental() %}
        where s.meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
    {% endif %}
),

adword_clickinfo as (
    select c.*
    from {{ ref('ua_bq_domestic_adwords_clickinfo') }} as c
    inner join ga_view_id as vid
        on
            c.view_id = vid.view_id
            and c.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
    {% if is_incremental() %}
        where c.meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
    {% endif %}

),



device as (
    select s.*
    from {{ ref('ua_bq_domestic_device') }} as s
    inner join ga_view_id as vid
        on
            s.view_id = vid.view_id
            and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
    {% if is_incremental() %}
        where s.meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
    {% endif %}
),

geonetwork as (
    select s.*
    from {{ ref('ua_bq_domestic_geonetwork') }} as s
    inner join ga_view_id as vid
        on
            s.view_id = vid.view_id
            and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
    {% if is_incremental() %}
        where s.meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
    {% endif %}
)



select distinct
    s.vg,
    s.view_id,
    s.visit_date,
    s.session_key,
    ts.traffic_source_key,
    d.device_key,
    gn.geonetwork_key,
    s.visit_id,
    s.full_visitor_id,
    s.client_id,
    s.channel_grouping,
    s.social_engagement_type,
    s.visit_start_datetime,
    s.visit_start_time,
    s.meta_insert_ts,
    coalesce(ac.click_info_key, '-1') as click_info_key
from cte_session as s
left outer join traffic_source as ts
    on
        s.visit_id = ts.visit_id
        and s.full_visitor_id = ts.full_visitor_id
        and s.visit_date = ts.visit_date
        and s.view_id = ts.view_id
left outer join adword_clickinfo as ac
    on
        s.visit_id = ac.visit_id
        and s.full_visitor_id = ac.full_visitor_id
        and s.visit_date = ac.visit_date
        and s.view_id = ac.view_id
left outer join device as d
    on
        s.visit_id = d.visit_id
        and s.full_visitor_id = d.full_visitor_id
        and s.visit_date = d.visit_date
        and s.view_id = d.view_id
left outer join geonetwork as gn
    on
        s.visit_id = gn.visit_id
        and s.full_visitor_id = gn.full_visitor_id
        and s.visit_date = gn.visit_date
        and s.view_id = gn.view_id
inner join ga_view_id as vid
    on
        s.view_id = vid.view_id
        and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd

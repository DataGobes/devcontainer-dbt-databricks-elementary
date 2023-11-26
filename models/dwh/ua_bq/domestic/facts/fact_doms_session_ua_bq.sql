{{ config (   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with ga_view_id as (
    select s.*
    from {{ ref('dim_ga_view_id') }} as s
),

cte_session as (
    select s.*
    from {{ ref('ua_bq_domestic_sessions') }} as s
),

traffic_source as (
    select s.*
    from {{ ref('ua_bq_domestic_traffic_source') }} as s

),

adword_clickinfo as (
    select s.*
    from {{ ref('ua_bq_domestic_adwords_clickinfo') }} as s

),

device as (
    select s.*
    from {{ ref('ua_bq_domestic_device') }} as s

),

eventinfo as (
    select
        h.view_id,
        h.visit_date,
        h.visit_id,
        h.full_visitor_id,
        cast(count(*) as int) as events
    from {{ ref('ua_bq_domestic_hits_eventinfo') }} as h
    inner join ga_view_id as vid
        on
            h.view_id = vid.view_id
            and h.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
    group by h.view_id, h.visit_date, h.visit_id, h.full_visitor_id

)

select
    s.vg,
    s.view_id,
    s.session_key,
    d.device_key,
    tf.traffic_source_key,
    s.visit_date,
    s.visit_id,
    s.full_visitor_id,
    s.visits,
    s.hits,
    s.pageviews,
    s.time_on_site,
    s.bounces,
    s.new_visits,
    s.screenviews,
    s.unique_screen_views,
    s.time_on_screen,
    s.session_quality_dim,
    tf.source as traffic_source,
    tf.medium as traffic_medium,
    d.device_category,
    s.channel_grouping,
    s.meta_insert_ts,
    coalesce(ac.click_info_key, '-1') as click_info_key,
    coalesce(h.events, 0) as events
from cte_session as s
left join adword_clickinfo as ac
    on
        s.visit_id = ac.visit_id
        and s.full_visitor_id = ac.full_visitor_id
        and s.visit_date = ac.visit_date
        and s.view_id = ac.view_id
left join traffic_source as tf
    on
        s.visit_id = tf.visit_id
        and s.full_visitor_id = tf.full_visitor_id
        and s.visit_date = tf.visit_date
        and s.view_id = tf.view_id
left join device as d
    on
        s.visit_id = d.visit_id
        and s.full_visitor_id = d.full_visitor_id
        and s.visit_date = d.visit_date
        and s.view_id = d.view_id
left join eventinfo as h
    on
        s.view_id = h.view_id
        and s.visit_date = h.visit_date
        and s.visit_id = h.visit_id
        and s.full_visitor_id = h.full_visitor_id
inner join ga_view_id as vid
    on
        s.view_id = vid.view_id
        and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd

{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id', 'hit_key', 'event_key', 'page_key'],
    merge_exclude_columns = ['meta_insert_ts'],
    tags=["uabq_doms_dwh"]
          ) 
}}

with ga_view_id as (
    select *
    from  {{ ref('dim_ga_view_id') }}
),

events as (
    select ei.* 
    from {{ ref ('ua_bq_domestic_hits_eventinfo') }} as ei
    inner join ga_view_id as vid
      on ei.view_id = vid.view_id
      and ei.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
    {% if is_incremental() %}
        where ei.meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
    {% endif %} 
),

hits as (
    select hits.* 
    from {{ ref('ua_bq_domestic_hits')}} as hits
    inner join ga_view_id as vid
      on hits.view_id = vid.view_id
      and hits.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

pages as (
    select pages.*
    from {{ ref('ua_bq_domestic_hits_page') }} as pages
    inner join ga_view_id as vid
        on pages.view_id = vid.view_id
        and pages.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

uabq_session as (
    select s.*    
    from {{ ref('ua_bq_domestic_sessions') }} as s
    inner join ga_view_id as vid
      on s.view_id = vid.view_id
      and s.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

traffic_source as (
    select ts.* 
    from {{ ref('ua_bq_domestic_traffic_source')}} as ts
    inner join ga_view_id as vid
      on ts.view_id = vid.view_id
      and ts.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

device as (
    select dev.* 
    from {{ ref('ua_bq_domestic_device')}} as dev
    inner join ga_view_id as vid
      on dev.view_id = vid.view_id
      and dev.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

final as (
    select 
        events.vg,
        events.view_id,
        events.visit_date,
        events.session_key,
        events.hit_key,
        events.event_key,
        events.visit_id,
        events.full_visitor_id, 
        events.hit_number,
        hits.visit_time, 
        hits.visit_hour, 
        hits.visit_minute,
        uabq_session.channel_grouping,
        traffic_source.source,
        traffic_source.medium,
        device.device_category,
        pages.page_key,
        current_timestamp() meta_insert_ts,
        current_timestamp() meta_update_ts
    from events
    inner join hits
        on hits.hit_key = events.hit_key 
        and hits.visit_date = events.visit_date
    inner join uabq_session
        on uabq_session.session_key = hits.session_key
    left join traffic_source
        on traffic_source.session_key = uabq_session.session_key
    left join device
        on device.session_key = uabq_session.session_key
    left join pages
        on hits.hit_key = pages.hit_key
)

select * from final

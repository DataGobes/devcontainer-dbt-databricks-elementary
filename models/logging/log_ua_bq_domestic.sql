{{ config (
    materialized='incremental'
    )
}}

with device as (
    select count(*) as record_count from {{ ref("dim_doms_device_ua_bq") }}
),

event_uabq as (
    select count(*) as record_count from {{ ref("dim_doms_event_ua_bq") }}
),

experiment as (
    select count(*) as record_count from {{ ref("dim_doms_experiment_ua_bq") }}
),

hit as (
    select count(*) as record_count from {{ ref("dim_doms_hit_ua_bq") }}
),

page_uabq as (
    select count(*) as record_count from {{ ref("dim_doms_page_ua_bq") }}
),

product as (
    select count(*) as record_count from {{ ref("dim_doms_product_ua_bq") }}
),

session_uabq as (
    select count(*) as record_count from {{ ref("dim_doms_session_ua_bq") }}
),

traffic_source as (
    select count(*) as record_count from {{ ref("dim_doms_traffic_source_ua_bq") }}
),

final as (
    select
        current_date() as date_yyyymmdd,
        'device' as table_name,
        'rowcount' as agg_type,
        record_count
    from device
    union all
    select
        current_date() as date_yyyymmdd,
        'event' as table_name,
        'rowcount' as agg_type,
        record_count
    from event_uabq
    union all
    select
        current_date() as date_yyyymmdd,
        'experiment' as table_name,
        'rowcount' as agg_type,
        record_count
    from experiment
    union all
    select
        current_date() as date_yyyymmdd,
        'hit' as table_name,
        'rowcount' as agg_type,
        record_count
    from hit
    union all
    select
        current_date() as date_yyyymmdd,
        'page' as table_name,
        'rowcount' as agg_type,
        record_count
    from page_uabq
    union all
    select
        current_date() as date_yyyymmdd,
        'product' as table_name,
        'rowcount' as agg_type,
        record_count
    from product
    union all 
    select
        current_date() as date_yyyymmdd,
        'session' as table_name,
        'rowcount' as agg_type,
        record_count
    from session_uabq
    union all 
    select
        current_date() as date_yyyymmdd,
        'traffic_source' as table_name,
        'rowcount' as agg_type,
        record_count
    from traffic_source
)

select * from final
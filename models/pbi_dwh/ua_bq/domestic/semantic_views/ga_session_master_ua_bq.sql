with src as (
    select distinct
         session_key as `Session ID`
        ,traffic_source_key as `Traffic Source ID`
        ,click_info_key as `Click Info ID`
        ,device_key as `Device ID` 
        ,visit_id as `Visit Id`
        ,full_visitor_id as `Full Visitor ID`
        ,channel_grouping as `Traffic Channel`
        ,social_engagement_type as `Social Engagement Type`
        ,client_id as `Client Id`
        ,cast(visit_date as integer) as `Visit Date` 
        ,view_id as `GA View Id`
        ,vg
    from {{ ref('dim_doms_session_ua_bq') }}
)

select * from src

ga_session_master_ua_bq
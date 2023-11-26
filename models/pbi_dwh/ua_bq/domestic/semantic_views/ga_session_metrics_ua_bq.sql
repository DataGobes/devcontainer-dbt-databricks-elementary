with src as (
    select 
         view_id AS `View ID`
        ,vg
        ,session_key AS `Session ID`
        ,cast(visit_date as integer) AS `Session Date`
        ,cast(new_visits as integer) AS `New Visits`
        ,channel_grouping AS `Traffic Channel`
        ,traffic_source AS `Traffic Source`
        ,traffic_medium AS `Traffic Medium`
        ,device_category AS `Device Category`
        ,sum(coalesce(visits,0)) AS `Sessions`
        ,sum(coalesce(time_on_site,0)) AS `Session Duration`
        ,sum(coalesce(bounces,0)) AS `Bounces`
        ,sum(coalesce(pageviews,0)) AS `Page Views`
        ,visit_id AS `Visit ID`
        ,full_visitor_id AS `Full Visitor ID`
     from {{ ref('fact_doms_session_ua_bq') }}
     group by 
         view_id
        ,vg
        ,session_key 
        ,visit_date
        ,new_visits 
        ,channel_grouping
        ,traffic_source 
        ,traffic_medium 
        ,device_category 
        ,visit_id
        ,full_visitor_id
)

select * from src


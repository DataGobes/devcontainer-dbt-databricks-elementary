with src as (
    select 
        view_id
        ,vg
        ,cast(visit_date as integer) as `WebPage Date`
        ,page_key AS `Page Path ID`
        ,session_key as `Session ID`
        ,pageviews AS `Page Views`
        ,time_on_page AS `Time On Page`
        ,coalesce(cast(is_entrance AS int), 0) AS Entrances
        ,coalesce(cast(is_exit AS int), 0) AS Exits  
    from {{ ref('fact_doms_page_hit_ua_bq') }}
)

select * from src


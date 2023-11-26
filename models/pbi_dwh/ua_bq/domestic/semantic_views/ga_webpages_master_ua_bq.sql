with src as (
    select distinct
         page_key as `WebPage ID`
        ,host_name as `Home Page`
        ,page_path as `WebPage Path`
        ,base_url as  `Base URL`
        ,concat(host_name,page_path) as `Full URL`
        ,page_type as `Page Type` 
    from {{ ref('dim_doms_page_ua_bq') }}
)

select * from src


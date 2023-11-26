{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['page_key'],   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with page as  (
    select *
    from  {{ ref('ua_bq_domestic_hits_page') }}
            {% if is_incremental() %}
              where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
            {% endif %} 
),

ga_view_id as (
    select *
    from  {{ ref('dim_ga_view_id') }}
)

Select distinct
 page_key
,page_path
,case  when CHARINDEX('?', `page_path`) != 0 and CHARINDEX('#', `page_path`) = 0  then LEFT(`page_path`, CHARINDEX('?', `page_path`) -1) 
       when CHARINDEX('#', `page_path`) != 0 and CHARINDEX('?', `page_path`)  = 0  then LEFT(`page_path`, CHARINDEX('#', `page_path`) -1)
       when CHARINDEX('#', `page_path`) != 0 and CHARINDEX('?', `page_path`) != 0 and (CHARINDEX('#', `page_path`) < CHARINDEX('?', `page_path`)) then LEFT(`page_path`, CHARINDEX('#', `page_path`) -1) 
       when CHARINDEX('#', `page_path`) != 0 and CHARINDEX('?', `page_path`) != 0 and (CHARINDEX('#', `page_path`) > CHARINDEX('?', `page_path`)) then LEFT(`page_path`, CHARINDEX('?', `page_path`) -1)
else `page_path` 
end as `base_url`
,host_name
,page_title
,page_type as page_type_original
,case when page_path='/e/search-s' then 'search_results'
        when page_path='/layer/cart' then 'overlay_cart'
        when page_path='/layer/information/product-added-to-cart' then 'overlay_addtocart'
else page_type end as page_type
,search_keyword
,page_path_level1
,page_path_level2
,page_path_level3
,page_path_level4
,d.meta_insert_ts
from page d inner join ga_view_id vid
on d.view_id = vid.view_id
and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd

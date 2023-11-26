with fact_page_hit as  
(
    select d.*
    from  {{ ref('fact_doms_page_hit_ua_bq') }} d
),

dim_page as  
(
    select d.*
    from  {{ ref('dim_doms_page_ua_bq') }} d
  
)


select
fp.vg as VG,
fp.visit_date as `webpage_date`,
fp.channel_grouping as traffic_channel,
fp.traffic_source_key as traffic_source_id,
fp.device_category as device_category,
dp.page_type as page_type,
sum(fp.pageviews) as page_views,
count(distinct fp.session_key) as unique_page_views,
sum(coalesce(cast(fp.is_entrance AS int), 0)) as entrances,
SUM(coalesce(cast(fp.is_exit AS int), 0)) AS exits,
current_timestamp() as meta_insert_ts
from fact_page_hit as fp
inner join dim_page dp on dp.page_key = fp.page_key
where fp.visit_date >= 20210101
group by 1,2,3,4,5,6




with 
b as (select country_code2,currency_code_ISO  from {{ ref('dim_currency_per_country') }} 
union 
select distinct'GB',currency_code_ISO 
 from {{ ref('dim_currency_per_country') }}  where country_code2='UK'),


a as (select distinct vg,
    vg_ga_name,
    country_name,
    region,
    `Cluster` as cluster_name,
    `Top_6` as top6,
    `Large_or_Small` as large_or_small,
    `Global_Focus_Markets`  as global_focus_market, 
    `Platform` as current_platform_website,
    View_ID as view_id,
    `Homepage_URL` as homepage_url,
   -- b.currency_code_ISO as currency_code,
    from_dt,
    to_dt,
    is_active,
    case when lower(`Platform`) = 'hybris' then 1 
         when lower(`Platform`) = 'intershop' then 2
         when lower(`Platform`) = 'legacy' then 3
        else 4
    end as order_website
  from {{ ref('vg_history_seed') }}),
  
  q as (
select a.*,b.currency_code_ISO as currency_code from a left join b
on a.VG = b.country_code2
  where a.is_active='Y'
  ),

  t as (
select q.*,
    row_number() over (partition by vg order by order_website, view_id, from_dt desc) as rn from q )

    select distinct
    t.vg, 
    t.country_name, 
    t.region, 
    t.cluster_name, 
    t.top6, 
    t.large_or_small, 
    t.global_focus_market, 
    t.current_platform_website, 
    t.homepage_url, 
    t.currency_code,
    t.is_active,
    date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss.SSS z') as meta_insert_ts
    from t
    where rn=1
    order by vg


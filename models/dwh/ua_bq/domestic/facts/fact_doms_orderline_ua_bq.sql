{{ config (   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with hits_transaction as (
    select
        h.vg,
        h.view_id,
        h.visit_date,
        h.visit_id,
        h.full_visitor_id,
        h.hit_number,
        h.transaction_id,
        h.hit_key,
        h.order_key,
        h.meta_insert_ts
    from
        (select
            vg,
            view_id,
            visit_date,
            visit_id,
            full_visitor_id,
            hit_number,
            transaction_id,
            hit_key,
            order_key,
            meta_insert_ts,
            row_number() over (partition by order_key order by visit_date asc) as rnk
        from {{ ref('ua_bq_domestic_hits_transaction') }}) as h
    inner join {{ ref('dim_ga_view_id') }} as vid
        on
            h.view_id = vid.view_id
            and h.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
    where h.rnk = 1
),

ga_view_id as (
    select s.*
    from {{ ref('dim_ga_view_id') }} as s
),

cte_session as (
    select d.*
    from {{ ref('ua_bq_domestic_sessions') }} as d
    inner join ga_view_id as vid
        on
            d.view_id = vid.view_id
            and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

cte_product as (
    select d.*
    from {{ ref('ua_bq_domestic_hits_product') }} as d
    inner join ga_view_id as vid
        on
            d.view_id = vid.view_id
            and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
),

traffic_source as (
    select d.*
    from {{ ref('ua_bq_domestic_traffic_source') }} as d
    inner join ga_view_id as vid
        on
            d.view_id = vid.view_id
            and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd

),

adword_clickinfo as (
    select d.*
    from {{ ref('ua_bq_domestic_adwords_clickinfo') }} as d
    inner join ga_view_id as vid
        on
            d.view_id = vid.view_id
            and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd

),

device as (
    select d.*
    from {{ ref('ua_bq_domestic_device') }} as d
    inner join ga_view_id as vid
        on
            d.view_id = vid.view_id
            and d.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd

)

select
    vg,
    view_id,
    order_date,
    session_key,
    hit_key,
    traffic_source_key,
    click_info_key,
    device_key,
    hit_time_key,
    order_key,
    product_ua_bq_key,
    item_price,
    item_price_eur,
    local_item_price,
    item_list_position,
    item_coupon_code,
    meta_insert_ts,
    sum(item_revenue) as item_revenue,
    sum(item_revenue_eur) as item_revenue_eur,
    sum(item_quantity) as item_quantity
from
    (
        select
            ht.vg,
            ht.view_id,
            s.visit_date as order_date,
            s.session_key,
            ht.hit_key,
            ts.traffic_source_key,
            d.device_key,
            '-1' as hit_time_key,
            ht.order_key,
            p.product_ua_bq_key,
            p.product_revenue as item_revenue,
            p.product_list_position as item_list_position,
            p.product_coupon_code as item_coupon_code,
            ht.meta_insert_ts,
            coalesce(ac.click_info_key, '-1') as click_info_key,
            case
                when fx.fx_rate = 0 or fx.fx_rate is null then 0 else round(p.product_revenue / fx.fx_rate, 2)
            end as item_revenue_eur,
            coalesce(p.product_price, 0) as item_price,
            case
                when fx.fx_rate = 0 or fx.fx_rate is null then 0 else
                    round(coalesce(p.product_price, 0) / fx.fx_rate, 2)
            end as item_price_eur,
            coalesce(p.local_product_price, 0) as local_item_price,
            coalesce(p.product_quantity, 0) as item_quantity,
            case
                when
                    p.product_coupon_code is not null and p.product_coupon_code not in ('', ' ', 'null', '(not set)')
                    then 0
                else 1
            end as flag_prd_coupon
        from hits_transaction as ht
        inner join cte_session as s
            on
                ht.visit_id = s.visit_id
                and ht.full_visitor_id = s.full_visitor_id
                and ht.view_id = s.view_id
                and ht.visit_date = s.visit_date
        inner join cte_product as p
            on
                ht.hit_key = p.hit_key
                and ht.view_id = p.view_id
                and ht.visit_date = p.visit_date
        left outer join traffic_source as ts
            on
                s.visit_id = ts.visit_id
                and s.full_visitor_id = ts.full_visitor_id
                and s.visit_date = ts.visit_date
                and s.view_id = ts.view_id
        left outer join adword_clickinfo as ac
            on
                s.visit_id = ac.visit_id
                and s.full_visitor_id = ac.full_visitor_id
                and s.visit_date = ac.visit_date
                and s.view_id = ac.view_id
        left outer join device as d
            on
                s.visit_id = d.visit_id
                and s.full_visitor_id = d.full_visitor_id
                and s.visit_date = d.visit_date
                and s.view_id = d.view_id
        left join {{ source('src_reference', 'dim_date') }} as dd
            on s.visit_date = dd.date_key
        left join {{ source('src_reference', 'dim_vg') }} as v
            on ht.vg = v.vg
        left join {{ source('src_reference', 'dim_fx_rate') }} as fx
            on
                dd.month_year = fx.month_year
                and v.currency_code = fx.from_curr
    )
group by
    vg,
    view_id,
    order_date,
    session_key,
    hit_key,
    traffic_source_key,
    click_info_key,
    device_key,
    hit_time_key,
    order_key,
    product_ua_bq_key,
    item_price,
    item_price_eur,
    local_item_price,
    item_list_position,
    item_coupon_code,
    meta_insert_ts

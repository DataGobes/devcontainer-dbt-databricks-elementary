{{ config (   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with ga_view_id as (
    select *
    from {{ ref('dim_ga_view_id') }}
),

transactions as (
    select ht.*
    from {{ ref('ua_bq_domestic_hits_transaction') }} as ht
    inner join ga_view_id as vid
        on
            ht.view_id = vid.view_id
            and ht.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
            and ht.view_id != 102220228

    union

    select ht.*
    from {{ ref('ua_bq_domestic_hits_transaction') }} as ht
    inner join ga_view_id as vid
        on
            ht.view_id = vid.view_id
            and ht.visit_date between vid.from_dt_yyyymmdd and vid.to_dt_yyyymmdd
            and ht.view_id != 102220228
    inner join {{ ref('ua_bq_domestic_hits') }} as a
        on ht.hit_key = a.hit_key and a.hit_type = 'TRANSACTION' and a.view_id = 102220228



),

add_rank as (
    select
        *,
        row_number() over (partition by order_key order by visit_date asc) as rnk
    from transactions
),

transactions_deduped as (
    select h.*
    from add_rank as h
    where h.rnk = 1
),

hits as (
    select
        a.view_id,
        a.hit_key,
        a.consent_info,
        a.hour_padded,
        a.minute_padded,
        coalesce(b.time_key, -1) as hit_time_key
    from
        (select distinct
            view_id,
            hit_key,
            hit_data_source as consent_info,
            cast(concat('1', lpad(visit_hour, 2, '0'), lpad(visit_minute, 2, '0'), '00') as bigint) as hit_time_key,
            lpad(visit_hour, 2, '0') as hour_padded,
            lpad(visit_minute, 2, '0') as minute_padded
        from {{ ref('ua_bq_domestic_hits') }}) as a
    left join {{ source('src_reference', 'dim_time') }} as b
        on a.hit_time_key = b.time_key

),

fact_orderline as (
    select
        view_id,
        order_key,
        sum(item_revenue) as total_item_revenue
    from {{ ref('fact_doms_orderline_ua_bq') }}
    group by view_id, order_key
),

cte_session as (
    select d.*
    from {{ ref('ua_bq_domestic_sessions') }} as d
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
    x.vg,
    x.view_id,
    x.order_date,
    x.order_key,
    x.hit_key,
    x.hit_time_key,
    tf.traffic_source_key,
    d.device_key,
    x.consent_info,
    x.session_key,
    x.transaction_id,
    x.transaction_revenue,
    x.affiliation,
    x.currency_code,
    x.transaction_coupon,
    ds.meta_insert_ts,
    coalesce(ac.click_info_key, '-1') as click_info_key,
    case
        when fx.fx_rate = 0 or fx.fx_rate is null then 0 else round(x.transaction_revenue / fx.fx_rate, 2)
    end as transaction_revenue_eur,
    coalesce(x.transaction_tax, 0) as transaction_tax,
    case
        when fx.fx_rate = 0 or fx.fx_rate is null then 0 else round(coalesce(x.transaction_tax, 0) / fx.fx_rate, 2)
    end as transaction_tax_eur,
    coalesce(x.transaction_shipping, 0) as transaction_shipping,
    case
        when fx.fx_rate = 0 or fx.fx_rate is null then 0 else round(coalesce(x.transaction_shipping, 0) / fx.fx_rate, 2)
    end as transaction_shipping_eur,
    x.transaction_revenue - coalesce(fol.total_item_revenue, 0) as diff_revenue_hdr_item_total
from
    (
        select
            ht.vg,
            ht.view_id,
            ht.order_key,
            ht.hit_key,
            h.hit_time_key,
            h.consent_info,
            s.session_key,
            ht.transaction_id,
            max(s.visit_date) as order_date,
            sum(ht.transaction_revenue) as transaction_revenue,
            sum(ht.transaction_tax) as transaction_tax,
            sum(ht.transaction_shipping) as transaction_shipping,
            max(ht.affiliation) as affiliation,
            max(ht.currency_code) as currency_code,
            max(ht.transaction_coupon) as transaction_coupon
        from transactions_deduped as ht
        inner join cte_session as s
            on
                ht.visit_id = s.visit_id
                and ht.full_visitor_id = s.full_visitor_id
                and ht.view_id = s.view_id
                and ht.visit_date = s.visit_date
        inner join hits as h
            on
                ht.hit_key = h.hit_key
                and ht.view_id = h.view_id
        group by
            ht.vg,
            ht.view_id,
            ht.order_key,
            ht.hit_key,
            h.hit_time_key,
            h.consent_info,
            s.session_key,
            ht.transaction_id
    ) as x
inner join cte_session as ds
    on x.session_key = ds.session_key
left join traffic_source as tf
    on
        ds.visit_id = tf.visit_id
        and ds.full_visitor_id = tf.full_visitor_id
        and ds.view_id = tf.view_id
        and ds.visit_date = tf.visit_date
left join adword_clickinfo as ac
    on
        ds.visit_id = ac.visit_id
        and ds.full_visitor_id = ac.full_visitor_id
        and ds.view_id = ac.view_id
        and ds.visit_date = ac.visit_date
left join device as d
    on
        ds.visit_id = d.visit_id
        and ds.full_visitor_id = d.full_visitor_id
        and ds.view_id = d.view_id
        and ds.visit_date = d.visit_date
left join {{ source('src_reference', 'dim_date') }} as dd
    on x.order_date = dd.date_key
left join {{ source('src_reference', 'dim_vg') }} as v
    on x.vg = v.vg
left join {{ source('src_reference', 'dim_fx_rate') }} as fx
    on
        dd.month_year = fx.month_year
        and v.currency_code = fx.from_curr
left join fact_orderline as fol
    on
        x.view_id = fol.view_id
        and x.order_key = fol.order_key

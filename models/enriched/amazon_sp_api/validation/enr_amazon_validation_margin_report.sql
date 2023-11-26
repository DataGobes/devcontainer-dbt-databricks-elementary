with history as (
    select
        vendor,
        year(day_date) as yr,
        month(day_date) as mnth,
        avg(net_pure_product_margin) as net_pure_product_margin
    from {{ source('src_amazon_sp_api', 'amazon_sp_api_net_pure_product_margin_report') }}
    group by vendor, year(day_date), month(day_date)
    order by year(day_date) desc, month(day_date) desc
),

validation as (
    select
        vendor as vendor,
        month_start_date,
        month_end_date,
        year(month_start_date) as yr,
        month(month_start_date) as mnth,
        avg(net_pure_product_margin) as net_pure_product_margin_val
    from {{ source('src_amazon_sp_api_validation', 'amazon_sp_api_validation_net_pure_product_margin_report') }}
    where month_start_date >= '2021-01-01'
    group by vendor, year(month_start_date), month(month_start_date), month_start_date, month_end_date
    order by year(month_start_date) desc, month(month_start_date) desc
),

joined as (
    select
        validation.vendor,
        validation.yr,
        validation.mnth,
        validation.month_start_date,
        validation.month_end_date,
        history.net_pure_product_margin,
        validation.net_pure_product_margin_val
    from validation
    left join history
        on
            validation.vendor = history.vendor
            and validation.yr = history.yr
            and validation.mnth = history.mnth
)

select
    *,
    current_timestamp() as meta_insert_ts
from joined
where net_pure_product_margin != net_pure_product_margin_val or net_pure_product_margin is null
order by yr desc, mnth desc

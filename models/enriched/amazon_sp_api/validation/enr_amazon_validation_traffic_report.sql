with history as (
    select
        vendor,
        year(day_date) as yr,
        month(day_date) as mnth,
        sum(glance_views) as glance_views
    from {{ source('src_amazon_sp_api', 'amazon_sp_api_traffic_report') }}
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
        sum(glance_views) as glance_views_val
    from {{ source('src_amazon_sp_api_validation', 'amazon_sp_api_validation_traffic_report') }}
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
        history.glance_views,
        validation.glance_views_val
    from validation
    left join history
        on
            validation.vendor = history.vendor
            and validation.yr = history.yr
            and validation.mnth = history.mnth
    where validation.month_start_date >= '2021-01-01'
)

select
    *,
    current_timestamp() as meta_insert_ts
from joined
where glance_views != glance_views_val or glance_views is null
order by yr desc, mnth desc

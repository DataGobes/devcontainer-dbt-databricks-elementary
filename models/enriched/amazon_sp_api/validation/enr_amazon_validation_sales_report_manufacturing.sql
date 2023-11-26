with history as (
    select
        vendor,
        year(day_date) as yr,
        month(day_date) as mnth,
        round(sum(customer_returns), 2) as customer_returns,
        round(sum(ordered_revenue_amount), 2) as ordered_revenue_amount,
        round(sum(ordered_units), 2) as ordered_units,
        round(sum(shipped_cogs_amount), 2) as shipped_cogs_amount,
        round(sum(shipped_revenue_amount), 2) as shipped_revenue_amount,
        round(sum(shipped_units), 2) as shipped_units
    from {{ source('src_amazon_sp_api', 'amazon_sp_api_sales_report_manufacturing') }}
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
        round(sum(customer_returns), 2) as customer_returns_val,
        round(sum(ordered_revenue_amount), 2) as ordered_revenue_amount_val,
        round(sum(ordered_units), 2) as ordered_units_val,
        round(sum(shipped_cogs_amount), 2) as shipped_cogs_amount_val,
        round(sum(shipped_revenue_amount), 2) as shipped_revenue_amount_val,
        round(sum(shipped_units), 2) as shipped_units_val
    from {{ source('src_amazon_sp_api_validation', 'amazon_sp_api_validation_sales_report_manufacturing') }}
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
        history.customer_returns,
        validation.customer_returns_val,
        history.ordered_revenue_amount,
        validation.ordered_revenue_amount_val,
        history.ordered_units,
        validation.ordered_units_val,
        history.shipped_cogs_amount,
        validation.shipped_cogs_amount_val,
        history.shipped_revenue_amount,
        validation.shipped_revenue_amount_val,
        history.shipped_units,
        validation.shipped_units_val
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
where
    customer_returns != customer_returns_val or customer_returns is null
    or floor(ordered_revenue_amount) != floor(ordered_revenue_amount_val) or ordered_revenue_amount is null
    or ordered_units != ordered_units_val or ordered_units is null
    or floor(shipped_cogs_amount) != floor(shipped_cogs_amount_val) or shipped_cogs_amount is null
    or floor(shipped_revenue_amount) != floor(shipped_revenue_amount_val) or shipped_revenue_amount is null
    or shipped_units != shipped_units_val or shipped_units is null
order by yr desc, mnth desc

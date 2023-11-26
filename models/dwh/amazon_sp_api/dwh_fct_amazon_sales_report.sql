with sourcing as (
    select * from {{ ref('enr_amazon_sales_report_sourcing') }}
),

manufacturing as (
    select * from {{ ref('enr_amazon_sales_report_manufacturing') }}
),

joined as (
    select
        coalesce(sourcing.id, manufacturing.id) as id,
        coalesce(sourcing.vendor, manufacturing.vendor) as vendor,
        coalesce(sourcing.amazon_country, manufacturing.amazon_country) as amazon_country,
        coalesce(sourcing.vg, manufacturing.vg) as vg,
        coalesce(sourcing.marketplace_id, manufacturing.marketplace_id) as marketplace_id,
        coalesce(sourcing.report_type, manufacturing.report_type) as report_type,
        coalesce(sourcing.report_period, manufacturing.report_period) as report_period,
        coalesce(sourcing.selling_program, manufacturing.selling_program) as selling_program,
        coalesce(sourcing.asin_code, manufacturing.asin_code) as asin_code,
        coalesce(sourcing.day_date, manufacturing.day_date) as day_date,
        coalesce(sourcing.report_local_currency, manufacturing.report_local_currency) as report_local_currency,
        coalesce(sourcing.customer_returns, 0) as customer_returns_sourcing,
        coalesce(manufacturing.customer_returns, 0) as customer_returns_manufacturing,
        coalesce(sourcing.shipped_cogs_amount_lc, 0) as shipped_cogs_amount_lc_sourcing,
        coalesce(manufacturing.shipped_cogs_amount_lc, 0) as shipped_cogs_amount_lc_manufacturing,
        coalesce(sourcing.shipped_cogs_amount_eur, 0) as shipped_cogs_amount_eur_sourcing,
        coalesce(manufacturing.shipped_cogs_amount_eur, 0) as shipped_cogs_amount_eur_manufacturing,
        coalesce(sourcing.shipped_revenue_amount_lc, 0) as shipped_revenue_amount_lc_sourcing,
        coalesce(manufacturing.shipped_revenue_amount_lc, 0) as shipped_revenue_amount_lc_manufacturing,
        coalesce(sourcing.shipped_revenue_amount_eur, 0) as shipped_revenue_amount_eur_sourcing,
        coalesce(manufacturing.shipped_revenue_amount_eur, 0) as shipped_revenue_amount_eur_manufacturing,
        coalesce(sourcing.shipped_units, 0) as shipped_units_sourcing,
        coalesce(manufacturing.shipped_units, 0) as shipped_units_manufacturing,
        coalesce(manufacturing.ordered_revenue_amount_lc, 0) as ordered_revenue_amount_lc,
        coalesce(manufacturing.ordered_revenue_amount_eur, 0) as ordered_revenue_amount_eur,
        coalesce(manufacturing.ordered_units, 0) as ordered_units
    from sourcing
    full join manufacturing
        on sourcing.id = manufacturing.id
),

result as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key(['amazon_country', 'asin_code']) }} as amazon_product_key,
        current_timestamp() as meta_insert_ts
    from
        joined
)

select *
from result

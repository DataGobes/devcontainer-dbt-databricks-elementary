with source as (
    select * from {{ ref('dwh_fct_amazon_sales_report') }}
),

renamed as (
    select
        amazon_product_key as `Amazon Product Key`,
        id as `Id`,
        vendor as `Vendor`,
        amazon_country as `Amazon Country`,
        vg as `VG`,
        marketplace_id as `Marketplace ID`,
        report_type as `Report Type`,
        report_period as `Report Period`,
        selling_program as `Selling Program`,
        asin_code as `ASIN Code`,
        day_date as `Date`,
        report_local_currency as `Local Currency`,
        customer_returns_sourcing as `Customer Returns - Sourcing`,
        customer_returns_manufacturing as `Customer Returns - Manufacturing`,
        shipped_cogs_amount_lc_sourcing as `Shipped COGS Amount LC - Sourcing`,
        shipped_cogs_amount_lc_manufacturing as `Shipped COGS Amount LC - Manufacturing`,
        shipped_cogs_amount_eur_sourcing as `Shipped COGS Amount EUR - Sourcing`,
        shipped_cogs_amount_eur_manufacturing as `Shipped COGS Amount EUR - Manufacturing`,
        shipped_revenue_amount_lc_sourcing as `Shipped Revenue Amount LC - Sourcing`,
        shipped_revenue_amount_lc_manufacturing as `Shipped Revenue Amount LC - Manufacturing`,
        shipped_revenue_amount_eur_sourcing as `Shipped Revenue Amount EUR - Sourcing`,
        shipped_revenue_amount_eur_manufacturing as `Shipped Revenue Amount EUR - Manufacturing`,
        shipped_units_sourcing as `Shipped Units - Sourcing`,
        shipped_units_manufacturing as `Shipped Units - Manufacturing`,
        ordered_revenue_amount_lc as `Ordered Revenue Amount LC`,
        ordered_revenue_amount_eur as `Ordered Revenue Amount EUR`,
        ordered_units as `Ordered Units`
    from source
)

select * from renamed

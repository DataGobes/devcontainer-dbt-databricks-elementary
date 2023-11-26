with dwh_table as (
    select * from {{ ref('dwh_fct_amazon_net_pure_product_margin_report') }}
),

pbi_view as (
    select
        amazon_product_key as `Amazon Product Key`,
        id,
        vendor as `Vendor`,
        amazon_country as `Amazon Country`,
        vg as `VG`,
        marketplace_id as `Marketplace ID`,
        report_type as `Report Type`,
        report_period as `Report Period`,
        asin_code as `ASIN Code`,
        day_date as `Date`,
        net_pure_product_margin as `Net Pure Product Margin`
    from dwh_table
)

select * from pbi_view

{{ config(
    schema = "restricted_pbi_dwh",
    materialized = "view"
) }}
     

with source as (
    select * from {{ ref('fact_sapbw_ecomm_customer_analysis_base') }}
),

renamed as (
    select 
        reporting_unit_code         as `Reporting Unit Code`,
        vg                          as `VG`,
        date_key                    as `Date Key`,
        month_year                  as `Month Year`,
        mth_start_date              as `Mth Start Date`,
        order_number                as `Order Number`,
        customer_soldto_code        as `Customer Soldto Code`,
        currency_code               as `Currency Code`,
        amt_net_value               as `Amt Net Value`,
        amt_net_value_eur           as `Amt Net Value Eur`,
        customer_purchase_order     as `Customer Purchase Order`,
        new_customer_flag           as `New Customer Flag,`,
        recurring_customers_flag    as `Recurring Customers Flag`
    from source
)

select * from renamed

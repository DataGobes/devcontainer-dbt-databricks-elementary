{{ config(
    schema = "restricted_pbi_dwh",
    materialized = "view"
) }}
     

with source as (
    select * from {{ ref('fact_sapbw_ecomm_customer_analysis_month') }}
),

renamed as (
    select 
        reporting_unit_code     as `Reporting Unit Code`,
        vg                      as `VG`,
        date_key                as `Date Key`,
        month_year              as `Month Year`,
        mth_start_date          as `Mth Start Date`,
        currency_code           as `Currency Code`,
        amt_net_value           as `Amt Net Value`,
        amt_net_value_eur       as `Amt Net Value Eur`,
        ct_distinct_orders      as `Number of Distinct Orders`,
        new_customers           as `New Customers`,
        recurring_customers     as `Recurring Customers`
    from source
)

select * from renamed

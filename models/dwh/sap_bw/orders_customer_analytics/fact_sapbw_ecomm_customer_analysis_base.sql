{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"  
) }}

with base as (
    select  
        reporting_unit_code,
        vg, 
        date_key, 
        order_number,
        customer_soldto_code,             
        currency_code,
        sum(amt_net_value) as amt_net_value,
        sum(amt_net_value_EUR) as amt_net_value_eur
    from {{ ref('fact_sapbw_sales_order_details') }} 
    where is_ecomm='Y' and is_individual_consumer='Y' and date_key >= 20220101
    group by 
        reporting_unit_code,
        vg, 
        date_key, 
        order_number,
        customer_soldto_code,             
        currency_code
),


dim_date as (
    select *
    from
        {{ source('src_reference', 'dim_date') }}
),


add_date as (
    select 
        a.reporting_unit_code,
        a.vg, 
        a.date_key, 
        b.month_year,
        b.mth_start_date,
        a.order_number,
        a.customer_soldto_code, 
        a.currency_code,
        a.amt_net_value,
        a.amt_net_value_eur, 
        row_number() over (partition by a.reporting_unit_code, a.vg, a.customer_soldto_code order by a.date_key) as customer_purchase_order
    from base a 
    left join dim_date b 
      on a.date_key = b.date_key
) ,

final as (
    select 
        reporting_unit_code,
        vg, 
        date_key, 
        month_year,
        mth_start_date,
        order_number,
        customer_soldto_code, 
        currency_code,
        amt_net_value, 
        amt_net_value_eur,
        customer_purchase_order,        
        case when customer_purchase_order = 1 then 1 else 0 end as new_customer_flag,
        case when customer_purchase_order > 1 then 1 else 0 end as recurring_customers_flag
    from add_date
)

select *, 
        current_timestamp() as meta_insert_ts  from final
order by reporting_unit_code,vg,customer_soldto_code,customer_purchase_order
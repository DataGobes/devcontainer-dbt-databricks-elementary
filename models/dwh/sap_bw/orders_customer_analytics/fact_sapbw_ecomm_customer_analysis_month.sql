{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}

with base as (
    select *
    from {{ ref('fact_sapbw_ecomm_customer_analysis_base') }} 
),

dim_date as (
    select *
    from
        {{ source('src_reference', 'dim_date') }}
),

base_agg as (
    select 
        reporting_unit_code, 
        vg,
        month_year,
        mth_start_date,
        currency_code,
        sum(amt_net_value) as amt_net_value, 
        sum(amt_net_value_eur) as amt_net_value_eur,
        count(distinct order_number) as ct_distinct_orders
    from base 
    group by 
        reporting_unit_code,         
        vg,
        month_year,
        mth_start_date,
        currency_code
),

aov as (

    select d.date_key, 
           base_agg.*
    from base_agg 
    left join dim_date d 
    where base_agg.mth_start_date = d.cal_date
),


mth_view as (
    select *, 
        max(new_customer_flag) over (partition by reporting_unit_code, vg, month_year, customer_soldto_code) as max_nc_month
    from base 
),


new_cust as (
    select 
        reporting_unit_code, 
        vg,
        month_year,
        mth_start_date,
        sum(max_nc_month) as new_customers
    from mth_view     
    where max_nc_month=1 and new_customer_flag=1
    group by  
        reporting_unit_code, 
        vg,
        month_year,
        mth_start_date
),

recurr_cust1 as (
    select distinct
        reporting_unit_code, 
        vg,
        month_year,
        mth_start_date,
        customer_soldto_code,
        case when max_nc_month=1 then 0 else customer_soldto_code end as recurring_customers_flag 
    from mth_view     
    where max_nc_month=0
),

recurr_cust2 as (
select
        reporting_unit_code, 
        vg,
        month_year,
        mth_start_date,
        count(distinct recurring_customers_flag) as recurring_customers
from recurr_cust1
  group by reporting_unit_code, 
        vg,
        month_year,
        mth_start_date
),

final as (
   select
        aov.reporting_unit_code, 
        aov.vg,
        aov.date_key,
        aov.month_year,
        aov.mth_start_date,
        aov.currency_code,
        aov.amt_net_value, 
        aov.amt_net_value_eur,
        aov.ct_distinct_orders,
        coalesce(new_cust.new_customers,0) as new_customers,
        coalesce(recurr_cust2.recurring_customers,0) as recurring_customers
   from aov 
   left join new_cust 
     on aov.reporting_unit_code = new_cust.reporting_unit_code
     and aov.month_year = new_cust.month_year
   left join recurr_cust2 
     on aov.reporting_unit_code = recurr_cust2.reporting_unit_code
     and aov.month_year = recurr_cust2.month_year
)
   

--load all records except the current month (this will not display relevant results since it is not a complete month)
select *, 
        current_timestamp() as meta_insert_ts from final
where month_year <  (select month_year from reference.dim_date where cal_date = current_date()) 
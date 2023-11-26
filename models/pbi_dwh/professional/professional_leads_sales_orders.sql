with leads_funnel as (
    select 
        *,
        concat(
            substring(month_year_predecessor_sales,4,4),
            substring(month_year_predecessor_sales,1,2)
        ) as month_year
    from {{ ref('sap_c4c_report_lead_funnel') }}
),

fx_rates as (
    select *
    from {{ source('src_reference', 'dim_fx_rate') }}
),

sales_orders as (
    select distinct
        lead_id,
        sales_quote_id,
        sales_order,
        sales_order_text,
        gross_amount,
        gross_amount_currency,
        net_amount,
        net_amount_currency,
        month_year
    from leads_funnel
    where sales_order != ''
),

add_currency_conversion as (
    select
        sales_orders.*,
        -- case statement to fill records where amount = 0 but no currency provided
        case when sales_orders.gross_amount = 0
             then 0
             else {{ convert_to_eur('sales_orders.gross_amount', 'fx_gross.fx_rate') }}
        end as gross_amount_eur,
        case when sales_orders.net_amount = 0
             then 0
             else  {{ convert_to_eur('sales_orders.net_amount', 'fx_net.fx_rate') }}
        end as net_amount_eur
    from sales_orders
    left outer join fx_rates as fx_gross
        on sales_orders.gross_amount_currency = fx_gross.from_curr
        and sales_orders.month_year = fx_gross.month_year
    left outer join fx_rates as fx_net
        on sales_orders.net_amount_currency = fx_net.from_curr
        and sales_orders.month_year = fx_net.month_year
),

renamed as (
    select
        lead_id as `Lead ID`,
        sales_quote_id as `Sales Quote ID`,
        sales_order as `Sales Order`,
        sales_order_text as `Sales Order Text`,
        cast(gross_amount as double) as `Gross Amount`,
        gross_amount_eur as `Gross Amount in Euro`,
        cast(net_amount as double) as `Net Amount`,
        net_amount_eur as `Net Amount in Euro`
    from add_currency_conversion
)

select * from renamed

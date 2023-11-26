with items as (
    select
        *,
        substring(event_date, 1, 6) as month_year
    from {{ ref('enr_ga4_domestic_items') }}
    where event_name = 'purchase'
),

orders as (
    select *
    from {{ ref('enr_ga4_domestic_ecommerce') }}
    where
        event_name = 'purchase'
),

currency_param as (
    select
        event_key,
        upper(parameter_value) as currency
    from {{ ref('enr_ga4_domestic_event_parameters') }}
    where
        event_name = 'purchase'
        and event_parameter = 'currency'
),

fx_rate as (
    select *
    from {{ source('src_reference', 'dim_fx_rate') }}
),

add_fx as (
    select
        items.*,
        {{ convert_to_eur('items.price', 'fx_rate.fx_rate') }} as price_eur,
        {{ convert_to_eur('items.item_revenue', 'fx_rate.fx_rate') }} as item_revenue_eur,
        {{ convert_to_eur('items.item_refund', 'fx_rate.fx_rate') }} as item_refund_eur
    from items
    left outer join currency_param
        on items.event_key = currency_param.event_key
    left outer join fx_rate
        on
            currency_param.currency = fx_rate.from_curr
            and items.month_year = fx_rate.month_year
)

select
    orderline.event_key as order_key,
    orderline.item_key,
    cast(orderline.event_date as integer) as date_key,
    orders.transaction_id,
    cast(orderline.price as double) as price,
    orderline.price_eur,
    cast(orderline.item_revenue as double) as item_revenue,
    orderline.item_revenue_eur,
    cast(orderline.item_refund as double) as item_refund,
    orderline.item_refund_eur,
    cast(orderline.quantity as integer) as quantity,
    orderline.coupon,
    orderline.vg,
    orderline.property_id,
    current_timestamp() as meta_insert_ts
from add_fx as orderline
left outer join orders
    on orderline.event_key = orders.event_key
where orders.transaction_id_rownum = 1

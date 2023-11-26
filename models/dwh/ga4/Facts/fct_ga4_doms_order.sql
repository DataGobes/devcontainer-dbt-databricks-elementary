with purchases as (
    select
        *,
        substring(event_date, 1, 6) as month_year
    from {{ ref('enr_ga4_domestic_ecommerce') }}
    where
        event_name = 'purchase'
        and transaction_id_rownum = 1
),

events as (
    select *
    from {{ ref('enr_ga4_domestic_events') }}
),

device as (
    select *
    from {{ ref('enr_ga4_domestic_device') }}
),

geo as (
    select *
    from {{ ref('enr_ga4_domestic_geo') }}
),

session_attributes as (
    select *
    from {{ ref('enr_ga4_shared_session_logic') }}
),

params as (
    select *
    from {{ ref('enr_ga4_domestic_event_parameters') }}
    where
        event_name = 'purchase'
        and event_parameter in ('checkout_type', 'order_coupon', 'order_discount', 'overall_discount')
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
        purchases.*,
        currency_param.currency,
        {{ convert_to_eur('purchases.purchase_revenue', 'fx_rate.fx_rate') }} as purchase_revenue_eur,
        {{ convert_to_eur('purchases.shipping_value', 'fx_rate.fx_rate') }} as shipping_value_eur,
        {{ convert_to_eur('purchases.tax_value', 'fx_rate.fx_rate') }} as tax_value_eur,
        {{ convert_to_eur('purchases.refund_value', 'fx_rate.fx_rate') }} as refund_value_eur
    from purchases
    left outer join currency_param
        on purchases.event_key = currency_param.event_key
    left outer join fx_rate
        on currency_param.currency = fx_rate.from_curr and purchases.month_year = fx_rate.month_year
),

final as (
    select
        orders.event_key as order_key,
        cast(orders.event_date as integer) as date_key,
        events.session_key,
        device.device_key,
        geo.geo_key,
        sa.traffic_source_key,
        events.event_timestamp_local,
        events.event_timestamp_utc,
        orders.transaction_id,
        orders.currency,
        cast(orders.purchase_revenue as double) as purchase_revenue,
        orders.purchase_revenue_eur,
        cast(orders.shipping_value as double) as shipping_value,
        orders.shipping_value_eur,
        cast(orders.tax_value as double) as tax_value,
        orders.tax_value_eur,
        cast(orders.refund_value as double) as refund_value,
        orders.refund_value_eur,
        orders.unique_items,
        cast(orders.total_item_quantity as integer) as total_item_quantity,
        checkout_type.parameter_value as checkout_type,
        coupon.parameter_value as order_coupon,
        discount.param_value_double as order_discount,
        overall_discount.param_value_double as overall_discount,
        events.vg,
        events.property_id,
        current_timestamp() as meta_insert_ts
    from add_fx as orders
    left outer join device
        on orders.event_key = device.event_key
    left outer join geo
        on orders.event_key = geo.event_key
    left outer join events
        on orders.event_key = events.event_key
    left outer join params as checkout_type
        on
            orders.event_key = checkout_type.event_key
            and checkout_type.event_parameter = 'checkout_type'
    left outer join params as discount
        on
            orders.event_key = discount.event_key
            and discount.event_parameter = 'order_discount'
    left outer join params as overall_discount
        on
            orders.event_key = overall_discount.event_key
            and overall_discount.event_parameter = 'overall_discount'
    left outer join params as coupon
        on
            orders.event_key = coupon.event_key
            and coupon.event_parameter = 'order_coupon'
    left outer join session_attributes as sa
        on events.session_key = sa.session_key
)

select * from final

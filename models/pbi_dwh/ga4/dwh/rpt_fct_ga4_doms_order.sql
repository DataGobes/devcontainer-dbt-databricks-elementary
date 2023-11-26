with src as (
    select
        order_key as `Order Key`,
        date_key as `Date Key`,
        device_key as `Device Key`,
        session_key as `Session Key`,
        geo_key as `Geo Key`,
        traffic_source_key as `Traffic Source Key`,
        event_timestamp_local as `Event Timestamp Local`,
        event_timestamp_utc as `Event Timestamp UTC`,
        transaction_id as `Transaction ID`,
        currency as `Currency`,
        purchase_revenue as `Purchase Revenue`,
        purchase_revenue_eur as `Purchase Revenue In Euro`,
        shipping_value as `Shipping Value`,
        shipping_value_eur as `Shipping Value in Euro`,
        tax_value as `Tax Value`,
        tax_value_eur as `Tax Value in Euro`,
        refund_value as `Refund Value`,
        refund_value_eur as `Refund Value in Euro`,
        total_item_quantity as `Total Item Quantity`,
        checkout_type as `Checkout Type`,
        order_coupon as `Order Coupon`,
        order_discount as `Order Discount`,
        overall_discount as `Overall Discount`,
        vg as `VG`,
        property_id as `Property ID`

    from {{ ref('fct_ga4_doms_order') }}
)

select * from src

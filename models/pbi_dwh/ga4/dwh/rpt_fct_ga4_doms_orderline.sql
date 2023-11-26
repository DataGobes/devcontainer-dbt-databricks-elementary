with src as (
    select
        order_key as `Order Key`,
        item_key as `Item Key`,
        date_key as `Date Key`,
        transaction_id as `Transaction ID`,
        price as `Price`,
        price_eur as `Price In Euro`,
        item_revenue as `Item Revenue`,
        item_revenue_eur as `Item Revenue In Euro`,
        item_refund as `Item Refund`,
        item_refund_eur as `Item Refund In Euro`,
        quantity as `Quantity`,
        coupon as `Coupon`,
        vg as `VG`,
        property_id as `Property ID`

    from {{ ref('fct_ga4_doms_orderline') }}
)

select * from src

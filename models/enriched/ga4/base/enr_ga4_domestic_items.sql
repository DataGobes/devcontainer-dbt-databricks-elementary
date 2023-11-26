with source as (
    select * from {{ source('src_ga4','ga4_domestic_events') }}
    where event_name in (
        'add_payment_info', 'add_shipping_info', 'add_to_cart', 'add_to_wishlist',
        'begin_checkout', 'purchase', 'refund', 'remove_from_cart', 'select_item',
        'select_promotion', 'view_item_list', 'view_promotion', 'view_item'
    )
    and items is not null
),

add_unnested as (
    select
        *,
        {{ unnest_json_string('items') }} as items_unnested
    from source
),

flattened as (
    select
        *,
        items_unnested[0] as item_id,
        items_unnested[1] as item_name,
        items_unnested[2] as item_brand,
        items_unnested[3] as item_variant,
        items_unnested[4] as item_category,
        items_unnested[5] as item_category2,
        items_unnested[6] as item_category3,
        items_unnested[7] as item_category4,
        items_unnested[8] as item_category5,
        items_unnested[9] as price_in_usd,
        items_unnested[10] as price,
        items_unnested[11] as quantity,
        items_unnested[13] as item_revenue,
        items_unnested[15] as item_refund,
        items_unnested[16] as coupon,
        items_unnested[17] as affiliation,
        items_unnested[18] as location_id,
        items_unnested[19] as item_list_id,
        items_unnested[20] as item_list_name,
        items_unnested[21] as item_list_index,
        items_unnested[22] as promotion_id,
        items_unnested[23] as promotion_name,
        items_unnested[24] as creative_name,
        items_unnested[25] as creative_slot
    from add_unnested
),

add_keys as (
    select
        *,
        {{ event_key() }} as event_key,
        {{ dbt_utils.generate_surrogate_key(['item_id', 
                                    'item_name', 
                                    'item_brand', 
                                    'item_variant', 
                                    'item_category', 
                                    'item_category2',
                                    'item_category3',
                                    'item_category4',
                                    'item_category5']) 
        }} as item_key
    from flattened
),

final as (
    select
        event_key,
        item_key,
        event_date,
        event_name,
        item_id,
        item_name,
        item_brand,
        item_variant,
        item_category,
        item_category2,
        item_category3,
        item_category4,
        item_category5,
        price_in_usd,
        price,
        quantity,
        item_revenue,
        item_refund,
        coupon,
        affiliation,
        location_id,
        item_list_id,
        item_list_name,
        item_list_index,
        promotion_id,
        promotion_name,
        creative_name,
        creative_slot,
        vg,
        property_id,
        current_timestamp() as meta_insert_ts
    from add_keys
)

select * from final

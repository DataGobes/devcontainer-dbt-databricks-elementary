with source as (
    select * from {{ source('src_ga4','ga4_domestic_events') }}
    where ecommerce is not null
),

add_event_key as (
    select
        *,
        {{ event_key() }} as event_key
    from source
),

add_array as (
    select
        *,
        {{ jsonstring_to_array('ecommerce') }} as ecom_array
    from add_event_key
),

flattened as (
    select
        *,
        ecom_array[0] as total_item_quantity,
        ecom_array[1] as purchase_revenue_in_usd,
        ecom_array[2] as purchase_revenue,
        ecom_array[3] as refund_value_in_usd,
        ecom_array[4] as refund_value,
        ecom_array[5] as shipping_value_in_usd,
        ecom_array[6] as shipping_value,
        ecom_array[7] as tax_value_in_usd,
        ecom_array[8] as tax_value,
        ecom_array[9] as unique_items,
        ecom_array[10] as transaction_id
    from add_array
),

purchase_deduplication as (
    select
        *,
        case
            when event_name = 'purchase' and transaction_id is not null
                then row_number() over (partition by transaction_id, property_id order by event_timestamp)
        end as transaction_id_rownum
    from flattened
),

final as (
    select
        event_key,
        event_date,
        event_name,
        total_item_quantity,
        purchase_revenue_in_usd,
        purchase_revenue,
        refund_value_in_usd,
        refund_value,
        shipping_value_in_usd,
        shipping_value,
        tax_value_in_usd,
        tax_value,
        transaction_id,
        unique_items,
        transaction_id_rownum,
        vg,
        property_id,
        current_timestamp() as meta_insert_ts
    from purchase_deduplication
)

select * from final

with items as (
    select distinct
        item_key,
        item_id,
        item_name,
        item_brand,
        item_variant,
        item_category,
        item_category2,
        item_category3,
        item_category4,
        item_category5,
        current_timestamp() as meta_insert_ts
    from {{ ref('enr_ga4_domestic_items') }}
)

select * from items

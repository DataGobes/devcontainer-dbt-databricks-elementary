with src as (
    select
        item_key as `Item Key`,
        item_id as `Item ID`,
        item_name as `Item Name`,
        item_brand as `Item Brand`,
        item_variant as `Item Variant`,
        item_category as `Item Category`,
        item_category2 as `Item Category 2`,
        item_category3 as `Item Category 3`,
        item_category4 as `Item Category 4`,
        item_category5 as `Item Category 5`

    from {{ ref('dim_ga4_doms_ga_item') }}
)

select * from src

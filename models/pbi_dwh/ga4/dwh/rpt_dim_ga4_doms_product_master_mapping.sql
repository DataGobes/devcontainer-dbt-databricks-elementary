with src as (
    select
        item_key as `Item Key`,
        dim_product_master_key as `Dim Product Master Key`,
        ga_item_id as `GA Item ID`,
        product_id_source as `Product ID Source`

    from {{ ref('dim_ga4_doms_product_master_mapping') }}
)

select * from src

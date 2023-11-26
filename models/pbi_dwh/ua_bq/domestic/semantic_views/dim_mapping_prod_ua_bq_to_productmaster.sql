with src as (
    select  
        product_ua_bq_key           as `Product UA BQ Id` ,
        product_ua_bq_sku           as `Product UA BQ SKU` ,
        dim_product_master_id       as `Dim Product Master ID`
    from {{ ref('dim_doms_mapping_prod_ua_bq_miele_sku') }}
)

select * from src

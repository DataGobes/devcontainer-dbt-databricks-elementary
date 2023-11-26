with src as (
    select distinct
     product_ua_bq_key as `Product UA BQ ID` 
    ,product_sku as `Product UA BQ SKU Code` 
    ,product_name as `Product UA BQ Name` 
    ,product_category as `Product UA BQ Category`
    ,ua_bq_product_category_l1 as `Product Category Level1` 
    ,ua_bq_product_category_l2 as `Product Category Level2` 
    ,ua_bq_product_category_l3 as `Product Category Level3` 
    ,ua_bq_product_category_l4 as `Product Category Level4` 
    ,ua_bq_product_category_l5 as `Product Category Level5` 
    ,product_variant as `Product UA BQ Variant` 
    ,Product_Brand as `Product UA BQ Brand`
    from {{ ref('dim_doms_product_ua_bq') }}
)

select * from src

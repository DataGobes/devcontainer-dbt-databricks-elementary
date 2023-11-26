{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['product_ua_bq_key'],   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with product as  (
    select *
    from  {{ ref('ua_bq_domestic_hits_product') }}
            {% if is_incremental() %}
              where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
            {% endif %} 
)

select distinct
          r.product_ua_bq_key
         ,r.product_sku
         ,r.v2_product_name as product_name
         ,r.v2_product_category as product_category
         ,element_at(r.mapped_prod_hier,1) AS ua_bq_product_category_l1
         ,element_at(r.mapped_prod_hier,2) AS ua_bq_product_category_l2 
         ,element_at(r.mapped_prod_hier,3) AS ua_bq_product_category_l3 
         ,element_at(r.mapped_prod_hier,4) AS ua_bq_product_category_l4 
         ,element_at(r.mapped_prod_hier,5) AS ua_bq_product_category_l5
         ,r.product_variant
         ,r.product_brand
         ,r.max_date
         ,r.meta_insert_ts  
         FROM 
         (
           select 
              product_ua_bq_key
             ,product_sku
             ,v2_product_name
             ,v2_product_category
             ,product_variant
             ,product_brand
             ,mapped_prod_hier
             ,max(product_date) AS max_date
             ,meta_insert_ts
           from
             (SELECT DISTINCT 
                product_ua_bq_key
                ,product_sku
                ,v2_product_name
                ,v2_product_category
                ,product_variant
                ,product_brand
                ,split(v2_product_category,'/') AS mapped_prod_hier
                ,visit_date as product_date
                ,meta_insert_ts
              FROM product
              )t
            group by 
              product_ua_bq_key
             ,product_sku
             ,v2_product_name
             ,v2_product_category
             ,product_variant
             ,product_brand
             ,mapped_prod_hier
             ,meta_insert_ts
         ) r
     union all
 select 
          '-1' as product_ua_bq_key
         ,'Not Set' as product_sku
         ,'Not Set' as product_name 
         ,'Not Set' as product_category
         ,'Not Set' as ua_bq_product_category_l1
         ,'Not Set' as ua_bq_product_category_l2 
         ,'Not Set' as ua_bq_product_category_l3 
         ,'Not Set' as ua_bq_product_category_l4 
         ,'Not Set' as ua_bq_product_category_l5
         ,'Not Set' as product_variant
         ,'Not Set' as product_brand
         ,-1 as max_date
         ,'1900-01-01' as meta_insert_ts

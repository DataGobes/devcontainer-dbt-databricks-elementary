{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id','visit_id', 'full_visitor_id', 'hit_number', 'product_sku']   
          ) 
}}

with source as  (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_hits_product') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %}
)

select 
     visit_date
    ,visitId as visit_id
    ,fullVisitorId as full_visitor_id
    ,hitNumber as hit_number
    ,productSKU as product_sku
    ,v2ProductName as v2_product_name
    ,v2ProductCategory as v2_product_category
    ,productVariant as product_variant
    ,productBrand as product_brand
    ,productRevenue/1000000 as product_revenue
    ,productPrice/1000000 as product_price
    ,localProductPrice/1000000 as local_product_price
    ,productQuantity as product_quantity
    ,productListPosition as product_list_position
    ,productCouponCode as product_coupon_code
    ,view_id
    ,meta_source
    ,vg
    ,current_timestamp() as meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} as hit_key
    ,{{ dbt_utils.generate_surrogate_key(['product_sku', 'v2_product_name', 'v2_product_category', 'product_variant', 'product_brand']) }} as product_ua_bq_key
 from source
{{ config (   
    tags=["uabq_doms_dwh"]     
          ) 
}}

with product as  (
    select *
    from  {{ ref('ua_bq_domestic_hits_product') }}
           
),

product_master as  (
    select *
    from  {{ source('src_dim_product_master', 'dim_product_master_pim') }}
           
)



select distinct
  c.product_ua_bq_key,
  c.product_ua_bq_sku,
  coalesce(d.dim_product_master_id,'-1') as dim_product_master_id,
  c.meta_insert_ts
  
from 
(select distinct
      product_ua_bq_key                                                         as product_ua_bq_key,                                                     
      lpad(trim(product_sku),8,'0')                                             as product_ua_bq_id,
      product_sku                                                               as product_ua_bq_sku,
      meta_insert_ts
  from product
) c
left join product_master d
on c.product_ua_bq_id = d.dim_product_master_id
union all
select
          '-1' as product_ua_bq_key
         ,'Not Set' as product_sku
         ,'-1' as dim_product_master_id
         ,'1900-01-01' as meta_insert_ts

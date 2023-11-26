{{ config (
    tags=["uabq_doms_enriched"]   
          ) 
}}

with source as (
    select * 
    from {{ source('src_ua_bq_raw_domestic', 'ua_bq_domestic_hits_product_custom_dimensions') }}
                
               )

select 
     visit_date
    ,visitId as visit_id
    ,fullVisitorId as full_visitor_id
    ,hitNumber as hit_number
    ,productSKU as product_sku
    ,cmdm_index
    ,cmdm_value
    ,view_id
    ,meta_source
    ,VG
    ,current_timestamp() as meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} as hit_key
 from source
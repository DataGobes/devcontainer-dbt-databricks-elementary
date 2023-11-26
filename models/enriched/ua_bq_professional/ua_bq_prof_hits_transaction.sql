{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id','visit_id', 'full_visitor_id', 'hit_number']   
          ) 
}}

with source as (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_hits_transaction') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %}
                )

select
    visit_date
   ,visitId as visit_id
   ,fullVisitorId as full_visitor_id
   ,hitNumber as hit_number
   ,transactionId as transaction_id
   ,transactionRevenue/1000000 as transaction_revenue
   ,transactionTax/1000000 as transaction_tax
   ,transactionShipping/1000000 as transaction_shipping
   ,affiliation as affiliation
   ,currencyCode as currency_code
   ,transactionCoupon as transaction_coupon
   ,view_id
   ,meta_source
   ,vg
   ,current_timestamp() as meta_insert_ts
   ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'hit_number']) }} as hit_key
   ,{{ dbt_utils.generate_surrogate_key(['view_id', 'transaction_id']) }} as order_key
from source
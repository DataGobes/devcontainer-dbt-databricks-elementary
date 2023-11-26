{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"  
) }}

with source as (
    select distinct crm_transaction_type
from  {{ ref('sales_orders_n3_gc_prctyp_to_filter_raw_data') }}
)

select * from source
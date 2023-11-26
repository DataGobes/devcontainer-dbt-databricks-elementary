{{ config(
    tags=["dq_tests"]
) }}

-- For all VG's the most recent product availability data should not be older than 2 days ago. 
-- If this test failes it might indicate missing files

select 
    vg, 
    max(date_key) 
from {{ source('src_dwh', 'fact_psa_hybris_inventory') }}
group by 1 
having max(date_key) < date_format(current_date() -1, 'yyyyMMdd')
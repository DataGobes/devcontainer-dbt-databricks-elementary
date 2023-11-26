{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}
      
with source as (
    select distinct
        trim(member_code) as member_code,
        trim(member_desc) as member_desc
from {{ ref('vgd_4_06_sales_group') }}
),

ordered_source as (
    select 
        member_code,
        member_desc,
        row_number() over (partition by member_code order by member_desc) as rn
from source
),

add_not_set as (
    select
        member_code,
        member_desc
    from ordered_source  
    where rn=1 
    union
    select  
        'Not Set' as member_code,
        'Not Set' as member_desc
),

final as (
    select 
        *,
        current_timestamp() as meta_insert_ts  
    from add_not_set
)

select * from final
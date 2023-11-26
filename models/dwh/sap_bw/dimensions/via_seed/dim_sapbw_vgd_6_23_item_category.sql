{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}
      
with source as (
    select distinct
        trim(member_code) as member_code,
        trim(member_desc) as member_desc,
        trim(flag_relevant) as flag_relevant
from {{ ref('vgd_6_23_item_category') }}
),

ordered_source as (
    select 
        member_code,
        member_desc,
        flag_relevant,
        row_number() over (partition by member_code order by member_desc) as rn
from source
),

add_not_set as (
    select
        member_code,
        member_desc,
        flag_relevant
    from ordered_source  
    where rn=1 
    union
    select  
        'Not Set' as member_code,
        'Not Set' as member_desc,
        'Not Set' as flag_relevant
),

final as (
    select 
        *,
        current_timestamp() as meta_insert_ts  
    from add_not_set
)

select * from final
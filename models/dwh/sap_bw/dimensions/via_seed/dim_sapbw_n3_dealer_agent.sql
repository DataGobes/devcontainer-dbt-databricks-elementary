{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"  
) }}
      
with source as (
   select distinct
        trim(rep_unit_code) as rep_unit_code,
        trim(dealer_agent_code) as dealer_agent_code,
        trim(dealer_agent_desc) as dealer_agent_desc
from {{ ref('n3_dealer_agent') }}
),

ordered_source as (
   select 
        rep_unit_code,
        dealer_agent_code,
        dealer_agent_desc,
        row_number() over (partition by rep_unit_code, dealer_agent_code order by dealer_agent_desc) as rn
from source
),

add_not_set as (
    select
        rep_unit_code,
        dealer_agent_code,
        dealer_agent_desc
    from ordered_source  
    where rn=1 
    union
    select  
        'Not Set' as rep_unit_code,
        'Not Set' as dealer_agent_code,
        'Not Set' as dealer_agent_desc
),

final as (
    select 
        *,
        current_timestamp() as meta_insert_ts  
    from add_not_set
)


select * from final
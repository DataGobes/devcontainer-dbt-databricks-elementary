{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"   
) }}
      
with source as (
   select distinct
        rep_unit_code,
        int_acct_customer_code,
        int_acct_customer_desc
    from {{ ref('n3_int_acct_customers') }}
),

ordered_source as (
   select 
        rep_unit_code,
        int_acct_customer_code,
        int_acct_customer_desc,
        row_number() over (partition by rep_unit_code,int_acct_customer_code order by int_acct_customer_desc) as rn
from source
),

add_not_set as (
    select
        rep_unit_code,
        int_acct_customer_code,
        int_acct_customer_desc
    from ordered_source  
    where rn=1 
    union
    select  
        'Not Set' as rep_unit_code,
        'Not Set' as int_acct_customer_code,
        'Not Set' as int_acct_customer_desc
),

final as (
    select 
        *,
        current_timestamp() as meta_insert_ts  
    from add_not_set
)

select * from final


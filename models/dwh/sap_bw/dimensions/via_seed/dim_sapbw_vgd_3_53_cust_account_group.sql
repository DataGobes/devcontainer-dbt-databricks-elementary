{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"  
) }}
      
with source as (
   select distinct
        trim(client_number) as client_number,
        trim(language_key) as language_key,
        case when trim(language_key)='D' then 1 else 99 end as order_language_key,
        trim(customer_account_group_code) as customer_account_group_code,
        trim(account_group_name) as account_group_name,
        trim(flag_relevant) as flag_relevant
from {{ ref('vgd_3_53_cust_account_group') }}
),

ordered_source as (
   select 
        client_number,
        language_key,        
        customer_account_group_code,
        account_group_name,
        flag_relevant,
        row_number() over (partition by customer_account_group_code order by client_number, order_language_key,account_group_name,flag_relevant) as rn
from source
),

add_not_set as (
    select
        client_number,
        language_key,        
        customer_account_group_code,
        account_group_name,
        flag_relevant
    from ordered_source  
    where rn=1 
    union
    select  
        'Not Set' as client_number,
        'Not Set' as language_key,
        'Not Set' as customer_account_group_code,
        'Not Set' as account_group_name,
        'Not Set' as flag_relevant
),

final as (
    select 
        *,
        current_timestamp() as meta_insert_ts  
    from add_not_set
)


select * from final

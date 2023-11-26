with src as (
select 
account_key as `Account Key`, 
account_id as `Account Id`,
account_currency as `Account Currency`,
Account_name as `Account Name`,
Account_Type as `Account Type`,
Account_labels as `Account Labels`,
vg as VG

from {{ ref('dim_account_google_ads')}}
)

    select * from src
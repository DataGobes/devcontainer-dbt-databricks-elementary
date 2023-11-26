{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['account_key'],
    location_root='/mnt/deltalake/dwh/',
    post_hook=["ALTER TABLE {{ this }} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);","{{ analyze_table() }}"]   
    ) }}

with src as (
select distinct 
account_key,
account_id,
account_currency

from {{ ref('google_ads_keyword')}}
)

select 
    s.account_key,
    s.account_id,
    s.account_currency, 
    m.Account_name,
    m.Account_Type,
    m.Account_labels,
    m.vg,
    current_timestamp() as   meta_insert_ts
from src s left join 
{{ref('Google_Ads_Account_VG_Map')}} m
on s.account_id=m.account_external_customer_id
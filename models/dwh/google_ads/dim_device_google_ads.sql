{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['device_key'],
    location_root='/mnt/deltalake/dwh/',
    post_hook=["ALTER TABLE {{ this }} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);","{{ analyze_table() }}"]   
    ) }}

with src as (
    select distinct 
    device_key,
    device,
    current_timestamp() as meta_insert_ts
 from {{ ref('google_ads_keyword')}})

    select * from src
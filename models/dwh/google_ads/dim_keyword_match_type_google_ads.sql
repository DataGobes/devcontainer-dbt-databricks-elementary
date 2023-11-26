{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['keyword_matchtype_key'],
    location_root='/mnt/deltalake/dwh/',
    post_hook=["ALTER TABLE {{ this }} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);","{{ analyze_table() }}"]   
    ) }}

with src as (
    select     distinct 
        keyword_matchtype_key,
        keyword_matchtype,
        current_timestamp() as meta_insert_ts

    from {{ ref('google_ads_keyword')}})

select * from src
{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['adgroup_key'],
    location_root='/mnt/deltalake/dwh/',
    post_hook=["ALTER TABLE {{ this }} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);","{{ analyze_table() }}"]   
    ) }}

with src as (
    select 
        adgroup_key,
        adgroup_id,
        adgroup_name,
        adgroup_labels,
        adgroup_type,
        row_number() over(partition by adgroup_key,adgroup_id order by date_key desc) as rn 
    from     
        (
        select 
            distinct 
            max(date_key)  as date_key,
            adgroup_key,
            adgroup_id,
            adgroup_name,
            adgroup_labels,
            adgroup_type

        from {{ ref('google_ads_keyword')}}

        group by 
            adgroup_key,
            adgroup_id,
            adgroup_name,
            adgroup_labels, 
            adgroup_type
        )
)

select 
adgroup_key,
adgroup_id,
adgroup_name,
adgroup_labels,
adgroup_type,
current_timestamp() as meta_insert_ts

from src where rn =1
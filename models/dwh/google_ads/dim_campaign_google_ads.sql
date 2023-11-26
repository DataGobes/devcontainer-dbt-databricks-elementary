{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['campaign_key'],
    location_root='/mnt/deltalake/dwh/',
    post_hook=["ALTER TABLE {{ this }} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);","{{ analyze_table() }}"]   
    ) }}

with src as 
(

    select 
            campaign_key,
            campaign_id,
            campaign_name,
            campaign_group,
            row_number() over(partition by campaign_key,campaign_id order by date_key desc) as rn 
    from 

    (
                select 
                    distinct 
                    max(date_key)  as date_key,
                    campaign_key,
                    campaign_id,
                    campaign_name,
                    campaign_group
                    
                from {{ ref('google_ads_keyword')}}

                group by 
                    campaign_key,
                    campaign_id,
                    campaign_name,
                    campaign_group
    )

)

    select 
        campaign_key,
        campaign_id,
        campaign_name,
        campaign_group,
        current_timestamp() as meta_insert_ts
    from src
    where rn=1


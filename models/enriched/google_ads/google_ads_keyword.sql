{{ config(
    location_root='/mnt/deltalake/enriched/',
    post_hook=["ALTER TABLE {{ this }} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);","ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;","vacuum {{ this }}"]   
    ) }}

select
    `date` as metric_date,
    device as device_key,
    device,
    customer_id as account_key,
    customer_id as account_id,
    customer_currencycode as account_currency,
    campaign_name as campaign_name,
    campaign_campaigngroup as campaign_group,
    adgroup_id as adgroup_key,
    adgroup_id as adgroup_id,
    adgroup_name as adgroup_name,
    adgroup_labels as adgroup_labels,
    adgroup_type as adgroup_type,
    adgroupcriterion_keyword_text as keyword_key,
    adgroupcriterion_keyword_text as keyword_text,
    adgroupcriterion_keyword_matchtype as keyword_matchtype_key,
    adgroupcriterion_keyword_matchtype as keyword_matchtype,
    clicks as clicks,
    costmicros as cost_micros,
    impressions as impressions,
    topimpressionpercentage as top_impression_percentage,
    absolutetopimpressionpercentage as absolute_top_impression_percentage,
    searchabsolutetopimpressionshare as search_abs_top_impression_share,
    searchimpressionshare as search_impression_share,
    searchtopimpressionshare as search_top_impression_share,
    meta_src_pipeline_run_id,
    meta_src,
    meta_insert_ts as meta_src_insert_ts,
    date_format(`Date`, 'yyyyMMdd') as date_key,
    element_at(split(adgroup_campaign, '/'), 4) as campaign_key,
    element_at(split(adgroup_campaign, '/'), 4) as campaign_id,
    current_timestamp() as meta_insert_ts

from

    {{ source('src_google_ads', 'google_ads_keyword_view') }}

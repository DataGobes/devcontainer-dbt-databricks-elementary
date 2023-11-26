{{ config (
    tags=["uabq_doms_enriched"]   
          ) 
}}

with source as (
    select *
    from {{ source('src_ua_bq_raw_domestic', 'ua_bq_domestic_traffic_source') }}
    where campaignid is not null

)

select
    visit_date,
    visitid as visit_id,
    fullvisitorid as full_visitor_id,
    adgroupid,
    adnetworktype,
    campaignid,
    creativeid,
    criteriaid,
    criteriaparameters,
    customerid,
    gclid,
    isvideoad,
    page as page_number,
    slot,
    view_id,
    vg,
    {{ dbt_utils.generate_surrogate_key([ 'adGroupId' ,'adNetworkType' ,'campaignId' ,'creativeId' ,'criteriaId' ,'criteriaParameters' ,'customerId' ,'gclId' ,'isVideoAd' ,'page' ,'slot']) }} as click_info_key,
    {{ dbt_utils.generate_surrogate_key(['referralPath','campaign', 'source', 'medium', 'keyword', 'adContent', 'isTrueDirect']) }} as traffic_source_key,
    {{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'visit_date']) }} as session_key,
    meta_source,
    current_timestamp() as meta_insert_ts
from source

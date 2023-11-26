{{ config(
    location_root='/mnt/deltalake/dwh/',
    post_hook=["ALTER TABLE {{ this }} SET TBLPROPERTIES (delta.autoOptimize.optimizeWrite = true, delta.autoOptimize.autoCompact = true);","{{ analyze_table() }}","vacuum {{ this }}" ]   
    ) }}

with

ads as (
    select
        date_key,
        account_key,
        account_currency,
        campaign_key,
        adgroup_key,
        device_key,
        keyword_key,
        keyword_matchtype_key,
        SUBSTR(date_key, 1, 6) as month_year,
        COALESCE(clicks, 0) as clicks,
        COALESCE(cost_micros, 0) as cost_micros,
        COALESCE(impressions, 0) as impressions,
        COALESCE(top_impression_percentage, 0) as top_impression_percentage,
        COALESCE(absolute_top_impression_percentage, 0) as absolute_top_impression_percentage,
        COALESCE(search_abs_top_impression_share, 0) as search_abs_top_impression_share,
        COALESCE(search_impression_share, 0) as search_impression_share,
        COALESCE(search_top_impression_share, 0) as search_top_impression_share
    from {{ ref('google_ads_keyword') }}
),


account_vg_map as (
    select
        account_external_customer_id,
        vg
    from {{ ref('Google_Ads_Account_VG_Map') }}
),


vg_fx as (
    select
        m.vg as vg,
        ar.date_key,
        ar.account_key,
        ar.campaign_key,
        ar.adgroup_key,
        ar.device_key,
        ar.keyword_key,
        ar.keyword_matchtype_key,
        ar.clicks,
        ar.cost_micros,
        ar.account_currency,
        ar.impressions,
        ar.search_impression_share,
        ar.search_top_impression_share,
        ar.cost_micros / 1000000 as cost,
        (ar.cost_micros / 1000000) / dfx.fx_rate as cost_eur,
        ar.impressions * ar.top_impression_percentage as impressions_on_top,
        ar.impressions * ar.absolute_top_impression_percentage as impressions_on_absolute_top,
        ar.impressions / ar.search_impression_share as eligible_impressions,
        CURRENT_TIMESTAMP() as meta_insert_ts
    from ads as ar
    left join reference.dim_fx_rate as dfx
        on
            ar.account_currency = dfx.from_curr
            and ar.month_year = dfx.month_year
    left join account_vg_map as m
        on ar.account_key = m.account_external_customer_id

)

select * from vg_fx

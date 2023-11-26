with src as (
    select
        vg as vg,
        date_key as `Date Key`,
        account_key as `Account Key`,
        campaign_key as `Campaign Key`,
        adgroup_key as `Adgroup Key`,
        device_key as `Device Key`,
        keyword_key as `Keyword key`,
        keyword_matchtype_key as `Keyword Match Type Key`,
        clicks as `Clicks`,
        cost_micros as `Cost Micros`,
        cost as `Cost`,
        cost_eur as `Cost in EUR`,
        account_currency as `Account Currency`,
        impressions as impressions,
        impressions_on_top as `Impressions On Top`,
        impressions_on_absolute_top as `Impressions On Absolute Top`,
        eligible_impressions as `Eligible Impressions`,
        search_impression_share as `Search Impression Share`,
        search_top_impression_share as `Search top Impression Share`
    from {{ ref('fct_keyword_google_ads') }}
)

select * from src

{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['session_key'],
    tags=["uabq_doms_enriched"]   
          ) 
}}

with source as (
    select * 
    from {{ source('src_ua_bq_raw_domestic', 'ua_bq_domestic_sessions') }}
                {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
                {% endif %}
               )


select 
    visit_date
    ,visitNumber as visit_number
    ,visitId as visit_id
    ,visitStartTime + visit_date as visit_start_datetime
    ,visitStartTime as visit_start_time
    ,fullVisitorId as full_visitor_id
    ,clientId as client_id
    ,channelGrouping as channel_grouping
    ,socialEngagementType as social_engagement_type
    ,visits
    ,hits
    ,pageviews
    ,timeOnSite as time_on_site
    ,bounces
    ,session_transactions
    ,transactionRevenue/1000000 as transaction_revenue
    ,newVisits as new_visits
    ,screenviews
    ,uniqueScreenviews as unique_screen_views
    ,timeOnScreen as time_on_screen
    ,totalTransactionRevenue/1000000 as total_transaction_revenue
    ,sessionQualityDim as session_quality_dim
    ,view_id
    ,meta_source
    ,VG
    ,current_timestamp() as meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['view_id', 'visit_id', 'full_visitor_id', 'visit_date']) }} as session_key
 from source
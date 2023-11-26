with src as (
    select
        session_key as `Session Key`,
        date_key as `Date Key`,
        device_key as `Device Key`,
        traffic_source_key as `Traffic Source Key`,
        ga_campaign_key as `GA Campaign Key`,
        session_start_ts_local as `Session Start TS Local`,
        is_session_engaged as `Is Session Engaged`,
        engagement_time as `Engagement Time`,
        session_duration as `Session Duration`,
        is_first_visit as `Is First Visit`,
        device_category as `Device Category`,
        is_consented as `Is Consented`,
        session_source as `Session Source`,
        session_medium as `Session Medium`,
        campaign_id as `Campaign ID`,
        vg as `VG`,
        property_id as `Property ID`

    from {{ ref('fct_ga4_doms_session') }}
)

select * from src

with src as (
    select
        session_key as `Session Key`,
        date_key as `Date Key`,
        traffic_source_key as `Traffic Source Key`,
        event_timestamp_local as `Event Timestamp Local`,
        event_timestamp_utc as `Event Timestamp UTC`,
        page_location as `Page Location`,
        base_url as `Base URL`,
        clean_path as `Clean Path`,
        page_type as `Page Type`,
        page_title as `Page Title`,
        page_referrer as `Page Refferer`,
        pageview_order as `Pageview Order`,
        is_landing_pageview as `Is Landing Pageview`,
        is_exit_pageview as `Is Exit Pageview`,
        vg as `VG`,
        property_id as `Property ID`

    from {{ ref('fct_ga4_doms_pageview') }}
)

select * from src

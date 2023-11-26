with pageview_events as (
    select *
    from {{ ref('enr_ga4_domestic_events') }}
    where event_name = 'page_view'
),

pageview_params as (
    select *
    from {{ ref('enr_ga4_domestic_event_parameters') }}
    where
        event_name = 'page_view'
        and event_parameter in (
            'page_type', 'engagement_time_msec', 'page_referrer', 'page_title', 'page_location', 'clean_path'
        )
),

session_attributes as (
    select *
    from {{ ref('enr_ga4_shared_session_logic') }}
),

page_location as (
    select
        event_key,
        parameter_value as page_location,
        {{ base_url('parameter_value') }} as base_url
    from pageview_params
    where event_parameter = 'page_location'
),

order_pageviews as (
    select
        event_key,
        session_key,
        cast(event_date as integer) as date_key,
        event_timestamp_local,
        event_timestamp_utc,
        property_id,
        vg,
        row_number() over (partition by session_key order by event_timestamp_utc) as pageview_order,
        row_number() over (partition by session_key order by event_timestamp_utc desc) as pageview_order_reverse
    from pageview_events
),

landing_exit_logic as (
    select
        *,
        case
            when pageview_order = 1
                then 1
            else 0
        end as is_landing_pageview,
        case
            when pageview_order_reverse = 1
                then 1
            else 0
        end as is_exit_pageview
    from order_pageviews
),

final as (
    select
        pageviews.session_key,
        pageviews.date_key,
        sa.traffic_source_key,
        pageviews.event_timestamp_local,
        pageviews.event_timestamp_utc,
        page_location.page_location,
        page_location.base_url,
        clean_path.parameter_value as clean_path,
        page_type.parameter_value as page_type,
        page_title.parameter_value as page_title,
        referrer.parameter_value as page_referrer,
        pageviews.pageview_order,
        pageviews.is_landing_pageview,
        pageviews.is_exit_pageview,
        pageviews.vg,
        pageviews.property_id,
        current_timestamp() as meta_insert_ts
    from landing_exit_logic as pageviews
    left outer join
        page_location
        on pageviews.event_key = page_location.event_key
    left outer join
        pageview_params as page_type
        on
            pageviews.event_key = page_type.event_key
            and page_type.event_parameter = 'page_type'
    left outer join
        pageview_params as page_title
        on
            pageviews.event_key = page_title.event_key
            and page_title.event_parameter = 'page_title'
    left outer join
        pageview_params as clean_path
        on
            pageviews.event_key = clean_path.event_key
            and clean_path.event_parameter = 'clean_path'
    left outer join
        pageview_params as referrer
        on
            pageviews.event_key = referrer.event_key
            and referrer.event_parameter = 'page_referrer'
    left outer join
        pageview_params as eng_time
        on
            pageviews.event_key = eng_time.event_key
            and eng_time.event_parameter = 'engagement_time_msec'
    left outer join
        session_attributes as sa
        on pageviews.session_key = sa.session_key

)

select * from final

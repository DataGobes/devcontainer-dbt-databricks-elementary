with events as (
    select *
    from {{ ref('enr_ga4_domestic_events') }}
),

params as (
    select *
    from {{ ref('enr_ga4_domestic_event_parameters') }}
),

device as (
    select *
    from {{ ref('enr_ga4_domestic_device') }}
),

session_attributes as (
    select *
    from {{ ref('enr_ga4_shared_session_logic') }}
),

session_parameters as (
    select
        events.session_key,
        events.vg,
        events.property_id,
        cast(min(events.event_date) as int) as date_key,
        min(events.event_timestamp_local) as session_start_ts_local,
        datediff(second, min(events.event_timestamp_utc), max(events.event_timestamp_utc)) as session_duration, --noqa: RF02
        --using the session engaged string parameter specifically will filter out session_start events
        coalesce(max(engaged.param_value_string), 0) as is_session_engaged,
        sum(engagement_msec.param_value_int) / 1000 as engagement_time,
        case
            when max(session_nr.parameter_value) = 1
                then 1
            else 0
        end as is_first_visit
    from events
    left outer join params as engaged
        on
            events.event_key = engaged.event_key
            and engaged.event_parameter = 'session_engaged'
    left outer join params as engagement_msec
        on
            events.event_key = engagement_msec.event_key
            and engagement_msec.event_parameter = 'engagement_time_msec'
    left outer join params as session_nr
        on
            events.event_key = session_nr.event_key
            and session_nr.event_parameter = 'ga_session_number'
    group by
        events.session_key,
        events.vg,
        events.property_id
),

first_device as (
    select distinct
        events.session_key,
        first(device.device_key) over (
            partition by events.session_key
            order by events.event_timestamp_unix
        ) as device_key,
        first(device.device_category) over (
            partition by events.session_key
            order by events.event_timestamp_unix
        ) as device_category
    from events
    inner join device
        on events.event_key = device.event_key
),

join_together as (
    select
        sp.*,
        fd.device_key,
        fd.device_category,
        sa.is_consented,
        sa.traffic_source_key,
        sa.ga_campaign_key,
        sa.session_source,
        sa.session_medium,
        sa.campaign_id
    from session_parameters as sp
    left outer join first_device as fd
        on sp.session_key = fd.session_key
    left outer join session_attributes as sa
        on sp.session_key = sa.session_key
),

final as (
    select
        session_key,
        date_key,
        device_key,
        traffic_source_key,
        ga_campaign_key,
        session_start_ts_local,
        is_session_engaged,
        engagement_time,
        session_duration,
        is_first_visit,
        device_category,
        is_consented,
        session_source,
        session_medium,
        campaign_id,
        vg,
        property_id,
        current_timestamp() as meta_insert_ts
    from join_together
)

select * from final

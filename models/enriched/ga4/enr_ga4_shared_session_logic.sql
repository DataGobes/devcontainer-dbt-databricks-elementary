{# 
Combines business logic on a session level to be reused in multiple DWH models
#}

with events as (
    select *
    from {{ ref('enr_ga4_domestic_events') }}
),

params as (
    select *
    from {{ ref('enr_ga4_domestic_event_parameters') }}
),

attributed_traffic_source as (
    select *
    from {{ ref('enr_ga4_attributed_session_traffic_source') }}
),

session_logic as (
    select
        events.session_key,
        --all events from before the consent parameter was implemented are consented. Therefore the default is 1.
        case
            when max(params.parameter_value) = 'true' then 1
            when max(params.parameter_value) = 'false' then 0
            else 1
        end as is_consented
    from events
    left outer join params
        on
            events.event_key = params.event_key
            and params.event_parameter = 'statistical_consent'
    group by
        events.session_key
),

add_session_traffic_source as (
    select
        sl.*,
        ats.traffic_source_key,
        ats.ga_campaign_key,
        ats.session_source,
        ats.session_medium,
        ats.campaign_id
    from session_logic as sl
    left outer join attributed_traffic_source as ats
        on sl.session_key = ats.session_key
),

final as (
    select
        session_key,
        is_consented,
        traffic_source_key,
        ga_campaign_key,
        session_source,
        session_medium,
        campaign_id,
        current_timestamp() as meta_insert_ts
    from add_session_traffic_source
)

select * from final

with
events as (
    select *
    from {{ ref('enr_ga4_domestic_events') }}
),

params as (
    select *
    from {{ ref('enr_ga4_domestic_event_parameters') }}
    where event_parameter = ('ga_session_id')
),

session_attributes as (
    select *
    from {{ ref('enr_ga4_shared_session_logic') }}
),

join_together as (
    select distinct
        events.session_key,
        events.user_pseudo_id,
        params.parameter_value as ga_session_id,
        session_attributes.is_consented
    from events
    left outer join params
        on events.event_key = params.event_key
    left outer join session_attributes
        on events.session_key = session_attributes.session_key
),

final as (
    select
        session_key,
        ga_session_id,
        user_pseudo_id,
        is_consented,
        current_timestamp() as meta_insert_ts
    from join_together
)

select * from final

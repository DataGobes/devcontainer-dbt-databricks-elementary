{# 
This view contains the logic to calculate the attributed traffic source for a session.
The output is a table on session level with the attributed traffic source columns.

Logic Summary:
If gclid is present then google/cpc
Otherwise take the first non-null traffic source of the session
If there is none then take the last valid traffic source of previous session from the client 90 days back
#}

--join collected traffic source with events to bring the session and timestamp
with collected_traffic_source as (
    select
        cts.*,
        e.session_key,
        e.event_timestamp_unix,
        e.event_timestamp_utc,
        e.client_key
    from {{ ref('enr_ga4_domestic_collected_traffic_source') }} as cts
    inner join {{ ref('enr_ga4_domestic_events') }} as e
        on cts.event_key = e.event_key
),

-- get the event_key for the first non-null traffic source from every session
first_traffic_source_event as (
    select distinct
        session_key,
        first_value(event_key) over (partition by session_key order by event_timestamp_unix) as first_valid_event_key
    from collected_traffic_source
    where manual_medium is not null or manual_source is not null or gclid is not null
),

all_sessions as (
    select
        cts.session_key,
        cts.client_key,
        ftse.first_valid_event_key,
        min(cts.event_timestamp_utc) as session_start_ts
    from collected_traffic_source as cts
    left outer join first_traffic_source_event as ftse
        on cts.session_key = ftse.session_key
    group by
        cts.session_key,
        cts.client_key,
        ftse.first_valid_event_key
),

-- get the last non-null traffic source from all sessions of this client in the past 90 days
include_previous_client_sessions as (
    select
        session_key,
        last_value(first_valid_event_key, true) over (
            partition by client_key
            order by cast(session_start_ts as timestamp)
            range between interval 90 days preceding and current row
        ) as first_valid_event_key
    from all_sessions
),

-- use the event_key to retrieve the corresponding traffic source attributes
get_traffic_source as (
    select
        ipcs.session_key,
        cts.manual_source,
        cts.manual_medium,
        cts.manual_campaign_id,
        cts.gclid
    from include_previous_client_sessions as ipcs
    left outer join collected_traffic_source as cts
        on ipcs.first_valid_event_key = cts.event_key
),

-- assign google/cpc in case gclid is present
gclid_logic as (
    select
        session_key,
        manual_campaign_id,
        case
            when gclid is not null then 'google'
            else manual_source
        end as session_source,
        case
            when gclid is not null then 'cpc'
            else manual_medium
        end as session_medium
    from get_traffic_source
),

null_handling as (
    select distinct
        session_key,
        manual_campaign_id,
        coalesce(session_source, '(direct)') as session_source,
        coalesce(session_medium, '(none)') as session_medium
    from gclid_logic
),

final as (
    select
        session_key,
        manual_campaign_id as campaign_id,
        session_source,
        session_medium,
        {{ dbt_utils.generate_surrogate_key(['session_source','session_medium']) }} as traffic_source_key,
        {{ dbt_utils.generate_surrogate_key(['manual_campaign_id']) }} as ga_campaign_key,
        current_timestamp() as meta_insert_ts
    from null_handling
)

select * from final

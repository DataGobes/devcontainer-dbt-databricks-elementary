with src as (
    select *
    from {{ ref('enr_ga4_domestic_event_parameters') }}
)

select * from src

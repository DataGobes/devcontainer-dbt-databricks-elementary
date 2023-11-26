with src as (
    select *
    from {{ ref('enr_ga4_domestic_events') }}
)

select * from src

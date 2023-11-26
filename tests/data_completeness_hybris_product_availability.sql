{{ config(
    tags=["dq_tests"]
) }}

-- Check if there is data for every VG for every day since the start of measurement
-- If this test failes it might indicate missing data in the history
-- starting 2023-08-01 because a lot of data is missing from before

with grouped as (
    select 
        date_key,
        vg
    from {{ source('src_dwh', 'fact_psa_hybris_inventory') }}
    where date_key >= 20230801
    group by date_key, vg
),

calculations as (
  select
    vg, 
    count(*) as number_of_days, 
    datediff(current_date, to_date(min(date_key), 'yyyyMMdd')) as expected_number_of_days
  from
    grouped 
  group by vg
)

select * from calculations where number_of_days < expected_number_of_days
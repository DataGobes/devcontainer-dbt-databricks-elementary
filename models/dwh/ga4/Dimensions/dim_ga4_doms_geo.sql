with geo as (
    select distinct
        geo_key,
        city,
        metro,
        region,
        country,
        sub_continent,
        continent,
        current_timestamp() as meta_insert_ts
    from {{ ref('enr_ga4_domestic_geo') }}
)

select * from geo

with src as (
    select
        geo_key as `Geo Key`,
        city as `City`,
        metro as `Metro`,
        region as `Region`,
        country as `Country`,
        sub_continent as `Sub Continent`,
        continent as `Continent`

    from {{ ref('dim_ga4_doms_geo') }}
)

select * from src

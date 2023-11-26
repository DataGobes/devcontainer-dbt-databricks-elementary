{{
    config(
        materialized='incremental',
        unique_key=['gsc_property', 'search_date', 'query', 'country_code_alpha3', 'device']
    )
}}

with source as (

    select
        *,
        cast(`date` as date) as search_date
    from
        {{ source('src_google_search_console', 'google_search_console_by_query') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where meta_insert_ts > (select max(meta_src_insert_ts) from {{ this }})
    {% endif %}
),

countries as (
    select *
    from
        {{ ref('country_codes') }}
),

dim_vg as (
    select *
    from
        {{ source('src_reference', 'dim_vg') }}
),

target_updates as (
    select
        s.gsc_property,
        s.query,
        case when locate('miele', lower(s.query)) > 0 then 'Branded' else 'Non-Branded' end as query_type,
        s.country as gsc_country_code,
        c.en as country_name,
        c.alpha2 as country_code_alpha2,
        c.alpha3 as country_code_alpha3,
        s.search_date,
        {{ convert_date_to_key('search_date') }},
        s.device,
        cast(s.clicks as integer) as clicks,
        cast(s.impressions as integer) as impressions,
        cast(s.position as float) as results_position,
        h.vg as vg,
        current_timestamp() as meta_insert_ts,
        s.meta_insert_ts as meta_src_insert_ts
    from source as s
    left join countries as c
        on s.country = c.alpha3
    left join dim_vg as h
        on replace(s.gsc_property, 'sc-domain:', '') = replace({{ dbt_utils.get_url_host(field='h.homepage_url') }}, 'www.', '')
)

select *
from target_updates

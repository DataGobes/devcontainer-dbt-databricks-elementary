{{
    config(
        materialized='incremental',
        unique_key=['gsc_property', 'country_code_alpha3', 'search_date', 'device', 'page', 'query']
    )
}}

with source as (

    select
        *,
        cast(`date` as date) as search_date,
        {{ dbt_utils.get_url_host(field='page') }} as url_host,
        {{ dbt_utils.get_url_path(field='page') }} as url_path
    from
        {{ source('src_google_search_console', 'google_search_console') }}
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

source_extended as (
    select
        *,
        case
            when (url_path rlike '^e/.+[0-9]+-c$') then 'PLP'
            when (url_path rlike '^e/.+[0-9]+-p$') then 'PDP'
            when (url_path rlike '^c/.+-[0-9]+\.htm$') then 'Category Page'
            when (url_path rlike '^e/.+-[0-9]+-h$') then 'Content Hub'
            when (url_path rlike '^ip/.+-[0-9]+\.htm$') then 'IP'
            when (url_path rlike '^e\/(?!.*-h$).*$') then 'Buyers Guide'
            when (len(url_path) == 0) then 'Home'
            else 'Other'
        end as page_type,
        nullif(regexp_extract(`page`, 'mat=([^&]*)'), '') as pim_code_mat_regex,
        nullif(regexp_extract(`page`, '(\\d{7,8})-(c|p)', 1), '') as pim_code_p_regex,
        nullif(regexp_extract(`page`, 'name=([^&]*)'), '') as product_name_regex
        -- macro below currently (dbt-labs/dbt_utils 0.9.5) not working so commented out
    {# {{ dbt_utils.get_url_parameter(field='page', url_parameter='mat') }} as query_param_mat #}
    {# {{ dbt_utils.get_url_parameter(field='page', url_parameter='name') }} as query_param_name #}
    from source
),

target_updates as (
    select
        s.gsc_property,
        s.query,
        case
            when locate('miele', lower(s.query)) > 0
                then 'Branded'
            else 'Non-Branded'
        end as query_type,
        s.`page`,
        --regex to remove everything after ? (query params) or # (navigation in html)
        regexp_replace(s.`page`, r"[\?|#].*$", '') as page_cleaned, -- noqa
        s.url_host as host_name,
        s.url_path as page_url,
        s.page_type,
        coalesce(
            s.pim_code_mat_regex,
            s.pim_code_p_regex
            -- query_param_mat,
        ) as product_pim_code,
        /*coalesce(
            product_name_regex,
            query_param_name
        ) as product_name,
        */--
        s.product_name_regex as product_name,
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
    from source_extended as s
    left join countries as c
        on s.country = c.alpha3
    left join dim_vg as h
        on replace(s.gsc_property, 'sc-domain:', '') = replace({{ dbt_utils.get_url_host(field='h.homepage_url') }}, 'www.', '')
)

select *
from target_updates

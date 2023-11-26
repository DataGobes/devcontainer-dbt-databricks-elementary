{{
    config(
        materialized='incremental',
        unique_key=['gsc_property', 'country_code_alpha3', 'search_date', 'device', 'page', 'query']
    )
}}

with source_updates as (

    select
        gsc_property,
        query,
        query_type,
        `page`,
        page_cleaned,
        host_name,
        page_url,
        page_type,
        product_pim_code,
        --product_name,
        gsc_country_code,
        country_name,
        country_code_alpha2,
        country_code_alpha3,
        search_date,
        search_date_key,
        device,
        clicks,
        impressions,
        results_position,
        vg,
        meta_insert_ts as meta_src_insert_ts
    from
        {{ ref('enr_google_search_console') }}
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where meta_insert_ts > (select max(meta_src_insert_ts) from {{ this }})
    {% endif %}

),

target_updates as (
    select
        *,
        current_timestamp() as meta_insert_ts
    from
        source_updates
)

select *
from target_updates

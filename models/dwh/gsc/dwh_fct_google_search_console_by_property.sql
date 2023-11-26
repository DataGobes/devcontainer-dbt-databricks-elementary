{{
    config(
        materialized='incremental',
        unique_key=['gsc_property', 'search_date','device']
    )
}}

with source_updates as (

    select
        gsc_property,
        search_date,
        search_date_key,
        device,
        clicks,
        impressions,
        results_position,
        vg,
        meta_insert_ts as meta_src_insert_ts
    from
        {{ ref('enr_google_search_console_by_property') }}
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

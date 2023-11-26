{{
    config(
        materialized='view'
    )
}}

with source_cleaned as (

    select * except (meta_insert_ts, meta_src_insert_ts)
    from
        {{ ref('dwh_fct_google_search_console_by_property') }}

)

select *
from source_cleaned

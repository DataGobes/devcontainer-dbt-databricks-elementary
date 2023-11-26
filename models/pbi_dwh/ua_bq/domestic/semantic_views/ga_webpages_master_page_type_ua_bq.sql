with src as (
    select
        *
    from {{ ref('ga_webpages_master_ua_bq') }}
)

select distinct 
    `Page Type`
from src

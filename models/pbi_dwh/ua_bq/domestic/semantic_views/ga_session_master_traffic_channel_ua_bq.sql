with src as (
    select
        *
    from {{ ref('ga_session_master_ua_bq') }}
)

select distinct 
    `Traffic Channel`
from src

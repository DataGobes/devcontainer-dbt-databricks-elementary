with src as (
    select
        *
    from {{ ref('ga_device_type_ua_bq') }}
)

select distinct 
    `Device Category`
from src

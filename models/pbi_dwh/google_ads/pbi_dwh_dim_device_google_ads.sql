
with src as (
    select 
    device_key as `Device Key`,
    device as Device
 from {{ ref('dim_device_google_ads')}})

    select * from src
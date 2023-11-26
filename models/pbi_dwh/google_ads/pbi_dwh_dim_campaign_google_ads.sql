
with src as (

select 
campaign_key as `Campaign Key`,
campaign_id as `Campaign Id`,
campaign_name as `Campaign Name`,
campaign_group as `Campaign Group`

from {{ ref('dim_campaign_google_ads')}})

    select * from src
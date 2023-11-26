with src as (
select 
    adgroup_key as `Adgroup Key`,
    adgroup_id as `Adgroup Id`,
    adgroup_name as `Adgroup Name`,
    adgroup_labels as `Adgroup Labels`,
    adgroup_type as `Adgroup Type`

from {{ ref('dim_adgroup_google_ads')}}
)

select * from src
with source as (
    select * from {{ ref('amazon_vendors') }}
),

cfg as (
    select distinct
        vg,
        parameter_name,
        parameter_value
    from {{ source('src_reference', 'dim_global_config') }}
    where parameter_name = 'amazon_country_code'
),

dummy as ( --noqa:ST03
    select 1 from {{ source('src_amazon_sp_api', 'amazon_sp_api_traffic_report') }}
)

select
    source.vendor,
    cfg.vg,
    source.amazon_country,
    source.vendor_code,
    source.vendor_name,
    source.calculate_in_sourcing
from source
left join cfg
    on left(source.vendor, 2) = cfg.parameter_value

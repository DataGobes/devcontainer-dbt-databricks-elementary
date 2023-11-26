with source as (
    select * from {{ source('src_amazon_sp_api', 'amazon_sp_api_traffic_report') }}
),

cfg as (
    select distinct
        vg,
        parameter_name,
        parameter_value
    from {{ source('src_reference', 'dim_global_config') }}
    where parameter_name = 'amazon_country_code'
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['vendor', 'day_date', 'asin']) }} as id,
        source.vendor,
        left(source.vendor, 2) as `amazon_country`,
        cfg.vg,
        source.marketplace_id,
        source.report_type,
        source.report_period,
        source.asin as `asin_code`,
        source.day_date,
        source.glance_views,
        current_timestamp() as meta_insert_ts,
        source._meta_insert_ts as meta_load_ts,
        source._meta_src as meta_src,
        source._meta_src_modification_ts as meta_src_modification_ts,
        source._meta_pipeline_run_id as meta_pipeline_run_id
    from source
    left join cfg
        on left(source.vendor, 2) = cfg.parameter_value
)

select * from renamed

with source as (
    select * from {{ source('src_amazon_sp_api', 'amazon_sp_api_sales_report_sourcing') }}
),

fx as (
    select * from {{ source('src_reference', 'dim_fx_rate') }}
),

cfg as (
    select distinct
        vg,
        parameter_name,
        parameter_value
    from {{ source('src_reference', 'dim_global_config') }} where parameter_name = 'amazon_country_code'
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
        source.selling_program,
        source.distributor_view,
        source.asin as `asin_code`,
        source.day_date,
        source.customer_returns,
        source.shipped_cogs_currency_code as report_local_currency,
        {{ convert_lc_to_eur('shipped_cogs_amount', 'fx.FX_RATE') }}
        {{ convert_lc_to_eur('shipped_revenue_amount', 'fx.FX_RATE') }}
        source.shipped_units,
        current_timestamp() as meta_insert_ts,
        source._meta_insert_ts as meta_load_ts,
        source._meta_src as meta_src,
        source._meta_src_modification_ts as meta_src_modification_ts,
        source._meta_pipeline_run_id as meta_pipeline_run_id
    from source
    left join fx
        on
            concat(date_format(source.day_date, 'yyyy'), date_format(source.day_date, 'MM')) = fx.`MONTH_YEAR`
            and source.shipped_currency_code = fx.`FROM_CURR`
    left join cfg
        on
            left(source.vendor, 2) = cfg.parameter_value
)

select * from renamed

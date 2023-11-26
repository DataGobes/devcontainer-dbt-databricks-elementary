with enr_table as (
    select * from {{ ref('enr_amazon_traffic_report') }}
),

dwh_table as (
    select
        id,
        vendor,
        amazon_country,
        vg,
        marketplace_id,
        report_type,
        report_period,
        asin_code,
        {{ dbt_utils.generate_surrogate_key(['amazon_country', 'asin_code']) }} as amazon_product_key,
        day_date,
        glance_views,
        current_timestamp() as meta_insert_ts
    from enr_table
)

select * from dwh_table

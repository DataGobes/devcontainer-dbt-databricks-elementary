with source as (
    select * from {{ source('src_amazon_sp_api', 'amazon_sp_api_inventory_report_manufacturing') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key(['vendor', 'day_date', 'asin']) }} as id,
        vendor,
        marketplace_id,
        report_type,
        report_period,
        selling_program,
        distributor_view,
        asin as `asin_code`,
        left(vendor, 2) as `amazon_country`,
        day_date,
        procurable_product_out_of_stock_rate,
        open_purchase_order_units,
        receive_fill_rate,
        average_vendor_lead_time_days,
        sell_through_rate,
        unfilled_customer_ordered_units,
        vendor_confirmation_rate,
        net_received_inventory_cost_amount,
        net_received_inventory_cost_currency_code,
        net_received_inventory_units,
        sellable_on_hand_inventory_cost_amount,
        sellable_on_hand_inventory_cost_currency_code,
        sellable_on_hand_inventory_units,
        unsellable_on_hand_inventory_cost_amount,
        unsellable_on_hand_inventory_cost_currency_code,
        unsellable_on_hand_inventory_units,
        aged_90_plus_days_sellable_inventory_cost_amount,
        aged_90_plus_days_sellable_inventory_cost_currency_code,
        aged_90_plus_days_sellable_inventory_units,
        unhealthy_inventory_cost_amount,
        unhealthy_inventory_cost_currency_code,
        unhealthy_inventory_units,
        uft,
        _meta_src,
        _meta_src_modification_ts,
        _meta_insert_ts,
        _meta_pipeline_run_id
    from source
)

select * from renamed

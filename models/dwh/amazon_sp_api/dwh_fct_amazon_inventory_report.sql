with all_asins as (
    select distinct
        asin_code,
        amazon_country
    from {{ ref('enr_amazon_sales_report_sourcing') }}
    union
    select distinct
        asin_code,
        amazon_country
    from {{ ref('enr_amazon_sales_report_manufacturing') }}
    union
    select distinct
        asin_code,
        amazon_country
    from {{ ref('enr_amazon_traffic_report') }}
),

manufacturing as (
    select * from {{ ref('enr_amazon_inventory_report_manufacturing') }}
),

sourcing as (
    select * from {{ ref('enr_amazon_inventory_report_sourcing') }}
),


joined as (
    select
        coalesce(sourcing.id, manufacturing.id) as id,
        coalesce(sourcing.vendor, manufacturing.vendor) as vendor,
        coalesce(sourcing.amazon_country, manufacturing.amazon_country) as amazon_country,
        coalesce(sourcing.marketplace_id, manufacturing.marketplace_id) as marketplace_id,
        coalesce(sourcing.report_type, manufacturing.report_type) as report_type,
        coalesce(sourcing.report_period, manufacturing.report_period) as report_period,
        coalesce(sourcing.selling_program, manufacturing.selling_program) as selling_program,
        coalesce(sourcing.distributor_view, manufacturing.distributor_view) as distributor_view,
        coalesce(sourcing.asin_code, manufacturing.asin_code) as asin_code,
        coalesce(sourcing.day_date, manufacturing.day_date) as day_date,
        coalesce(sourcing.procurable_product_out_of_stock_rate, 0) as procurable_product_out_of_stock_rate_sourcing,
        coalesce(manufacturing.procurable_product_out_of_stock_rate, 0) as procurable_product_out_of_stock_rate_manufacturing,
        coalesce(sourcing.open_purchase_order_units, 0) as open_purchase_order_units_sourcing,
        coalesce(manufacturing.open_purchase_order_units, 0) as open_purchase_order_units_manufacturing,
        coalesce(sourcing.receive_fill_rate, 0) as receive_fill_rate_sourcing,
        coalesce(manufacturing.receive_fill_rate, 0) as receive_fill_rate_manufacturing,
        coalesce(sourcing.average_vendor_lead_time_days, 0) as average_vendor_lead_time_days_sourcing,
        coalesce(manufacturing.average_vendor_lead_time_days, 0) as average_vendor_lead_time_days_manufacturing,
        coalesce(sourcing.sell_through_rate, 0) as sell_through_rate_sourcing,
        coalesce(manufacturing.sell_through_rate, 0) as sell_through_rate_manufacturing,
        coalesce(sourcing.unfilled_customer_ordered_units, 0) as unfilled_customer_ordered_units_sourcing,
        coalesce(manufacturing.unfilled_customer_ordered_units, 0) as unfilled_customer_ordered_units_manufacturing,
        coalesce(sourcing.vendor_confirmation_rate, 0) as vendor_confirmation_rate_sourcing,
        coalesce(manufacturing.vendor_confirmation_rate, 0) as vendor_confirmation_rate_manufacturing,
        coalesce(sourcing.net_received_inventory_cost_amount, 0) as net_received_inventory_cost_amount_sourcing,
        coalesce(manufacturing.net_received_inventory_cost_amount, 0) as net_received_inventory_cost_amount_manufacturing,
        coalesce(sourcing.net_received_inventory_cost_currency_code, 0) as net_received_inventory_cost_currency_code_sourcing,
        coalesce(manufacturing.net_received_inventory_cost_currency_code, 0) as net_received_inventory_cost_currency_code_manufacturing,
        coalesce(sourcing.net_received_inventory_units, 0) as net_received_inventory_units_sourcing,
        coalesce(manufacturing.net_received_inventory_units, 0) as net_received_inventory_units_manufacturing,
        coalesce(sourcing.sellable_on_hand_inventory_cost_amount, 0) as sellable_on_hand_inventory_cost_amount_sourcing,
        coalesce(manufacturing.sellable_on_hand_inventory_cost_amount, 0) as sellable_on_hand_inventory_cost_amount_manufacturing,
        coalesce(sourcing.sellable_on_hand_inventory_cost_currency_code, 0) as sellable_on_hand_inventory_cost_currency_code_sourcing,
        coalesce(manufacturing.sellable_on_hand_inventory_cost_currency_code, 0) as sellable_on_hand_inventory_cost_currency_code_manufacturing,
        coalesce(sourcing.sellable_on_hand_inventory_units, 0) as sellable_on_hand_inventory_units_sourcing,
        coalesce(manufacturing.sellable_on_hand_inventory_units, 0) as sellable_on_hand_inventory_units_manufacturing,
        coalesce(sourcing.unsellable_on_hand_inventory_cost_amount, 0) as unsellable_on_hand_inventory_cost_amount_sourcing,
        coalesce(manufacturing.unsellable_on_hand_inventory_cost_amount, 0) as unsellable_on_hand_inventory_cost_amount_manufacturing,
        coalesce(sourcing.unsellable_on_hand_inventory_cost_currency_code, 0) as unsellable_on_hand_inventory_cost_currency_code_sourcing,
        coalesce(manufacturing.unsellable_on_hand_inventory_cost_currency_code, 0) as unsellable_on_hand_inventory_cost_currency_code_manufacturing,
        coalesce(sourcing.unsellable_on_hand_inventory_units, 0) as unsellable_on_hand_inventory_units_sourcing,
        coalesce(manufacturing.unsellable_on_hand_inventory_units, 0) as unsellable_on_hand_inventory_units_manufacturing,
        coalesce(sourcing.aged_90_plus_days_sellable_inventory_cost_amount, 0) as aged_90_plus_days_sellable_inventory_cost_amount_sourcing,
        coalesce(manufacturing.aged_90_plus_days_sellable_inventory_cost_amount, 0) as aged_90_plus_days_sellable_inventory_cost_amount_manufacturing,
        coalesce(sourcing.aged_90_plus_days_sellable_inventory_cost_currency_code, 0) as aged_90_plus_days_sellable_inventory_cost_currency_code_sourcing,
        coalesce(manufacturing.aged_90_plus_days_sellable_inventory_cost_currency_code, 0) as aged_90_plus_days_sellable_inventory_cost_currency_code_manufacturing,
        coalesce(sourcing.aged_90_plus_days_sellable_inventory_units, 0) as aged_90_plus_days_sellable_inventory_units_sourcing,
        coalesce(manufacturing.aged_90_plus_days_sellable_inventory_units, 0) as aged_90_plus_days_sellable_inventory_units_manufacturing,
        coalesce(sourcing.unhealthy_inventory_cost_amount, 0) as unhealthy_inventory_cost_amount_sourcing,
        coalesce(manufacturing.unhealthy_inventory_cost_amount, 0) as unhealthy_inventory_cost_amount_manufacturing,
        coalesce(sourcing.unhealthy_inventory_cost_currency_code, 0) as unhealthy_inventory_cost_currency_code_sourcing,
        coalesce(manufacturing.unhealthy_inventory_cost_currency_code, 0) as unhealthy_inventory_cost_currency_code_manufacturing,
        coalesce(sourcing.unhealthy_inventory_units, 0) as unhealthy_inventory_units_sourcing,
        coalesce(manufacturing.unhealthy_inventory_units, 0) as unhealthy_inventory_units_manufacturing

    from sourcing
    full join manufacturing
        on sourcing.id = manufacturing.id
)


select
    j.id,
    j.vendor,
    j.marketplace_id,
    j.report_type,
    j.report_period,
    j.selling_program,
    j.distributor_view,
    a.asin_code,
    a.amazon_country,
    j.day_date,
    j.procurable_product_out_of_stock_rate_sourcing,
    j.procurable_product_out_of_stock_rate_manufacturing,
    j.open_purchase_order_units_sourcing,
    j.open_purchase_order_units_manufacturing,
    j.receive_fill_rate_sourcing,
    j.receive_fill_rate_manufacturing,
    j.average_vendor_lead_time_days_sourcing,
    j.average_vendor_lead_time_days_manufacturing,
    j.sell_through_rate_sourcing,
    j.sell_through_rate_manufacturing,
    j.unfilled_customer_ordered_units_sourcing,
    j.unfilled_customer_ordered_units_manufacturing,
    j.vendor_confirmation_rate_sourcing,
    j.vendor_confirmation_rate_manufacturing,
    j.net_received_inventory_cost_amount_sourcing,
    j.net_received_inventory_cost_amount_manufacturing,
    j.net_received_inventory_cost_currency_code_sourcing,
    j.net_received_inventory_cost_currency_code_manufacturing,
    j.net_received_inventory_units_sourcing,
    j.net_received_inventory_units_manufacturing,
    j.sellable_on_hand_inventory_cost_amount_sourcing,
    j.sellable_on_hand_inventory_cost_amount_manufacturing,
    j.sellable_on_hand_inventory_cost_currency_code_sourcing,
    j.sellable_on_hand_inventory_cost_currency_code_manufacturing,
    j.sellable_on_hand_inventory_units_sourcing,
    j.sellable_on_hand_inventory_units_manufacturing,
    j.unsellable_on_hand_inventory_cost_amount_sourcing,
    j.unsellable_on_hand_inventory_cost_amount_manufacturing,
    j.unsellable_on_hand_inventory_cost_currency_code_sourcing,
    j.unsellable_on_hand_inventory_cost_currency_code_manufacturing,
    j.unsellable_on_hand_inventory_units_sourcing,
    j.unsellable_on_hand_inventory_units_manufacturing,
    j.aged_90_plus_days_sellable_inventory_cost_amount_sourcing,
    j.aged_90_plus_days_sellable_inventory_cost_amount_manufacturing,
    j.aged_90_plus_days_sellable_inventory_cost_currency_code_sourcing,
    j.aged_90_plus_days_sellable_inventory_cost_currency_code_manufacturing,
    j.aged_90_plus_days_sellable_inventory_units_sourcing,
    j.aged_90_plus_days_sellable_inventory_units_manufacturing,
    j.unhealthy_inventory_cost_amount_sourcing,
    j.unhealthy_inventory_cost_amount_manufacturing,
    j.unhealthy_inventory_cost_currency_code_sourcing,
    j.unhealthy_inventory_cost_currency_code_manufacturing,
    j.unhealthy_inventory_units_sourcing,
    j.unhealthy_inventory_units_manufacturing,
    {{ dbt_utils.generate_surrogate_key(['a.amazon_country', 'a.asin_code']) }} as amazon_product_key,
    current_timestamp() as meta_insert_ts

from all_asins as a
left join joined as j on a.asin_code = j.asin_code and a.amazon_country = j.amazon_country

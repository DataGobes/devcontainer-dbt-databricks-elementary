with source as (
    select * from {{ ref('dwh_fct_amazon_inventory_report') }}
),

renamed as (
    select
        j.id as `Id`,
        j.vendor as `Vendor`,
        j.marketplace_id as `Marketplace Id`,
        j.report_type as `Report Type`,
        j.report_period as `Report Period`,
        j.selling_program as `Selling Program`,
        j.distributor_view as `Distributor View`,
        j.asin_code as `ASIN Code`,
        j.amazon_country as `Amazon Country`,
        j.amazon_product_key as `Amazon Product Key`,
        j.day_date as `Date`,
        --sourcing
        j.procurable_product_out_of_stock_rate_sourcing as `Procurable Product Out Of Stock Rate Sourcing`,
        j.procurable_product_out_of_stock_rate_manufacturing as `Procurable Product Out Of Stock Rate Manufacturing`,
        j.open_purchase_order_units_sourcing as `Open Purchase Order Units Sourcing`,
        j.open_purchase_order_units_manufacturing as `Open Purchase Order Units Manufacturing`,
        j.receive_fill_rate_sourcing as `Receive Fill Rate Sourcing`,
        j.receive_fill_rate_manufacturing as `Receive Fill Rate Manufacturing`,
        j.average_vendor_lead_time_days_sourcing as `Average Vendor Lead Time Days Sourcing`,
        j.average_vendor_lead_time_days_manufacturing as `Average Vendor Lead Time Days Manufacturing`,
        j.sell_through_rate_sourcing as `Sell Trough Rate`,
        j.sell_through_rate_manufacturing as `Sell Trough Rate Manufacturing`,
        j.unfilled_customer_ordered_units_sourcing as `Unfilled Customer Ordered Units Sourcing`,
        j.unfilled_customer_ordered_units_manufacturing as `Unfilled Customer Ordered Units Manufacturing`,
        j.vendor_confirmation_rate_sourcing as `Vendor Confirmation Rate Sourcing`,
        j.vendor_confirmation_rate_manufacturing as `Vendor Confirmation Rate Manufacturing`,
        j.net_received_inventory_cost_amount_sourcing as `Net Received Inventory Cost Amount Sourcing`,
        j.net_received_inventory_cost_amount_manufacturing as `Net Received Inventory Cost Amount Manufacturing`,
        j.net_received_inventory_cost_currency_code_sourcing as `Net Received Inventory Cost Currency Code Sourcing`,
        j.net_received_inventory_cost_currency_code_manufacturing as `Net Received Inventory Cost Currency Code Manufacturing`,
        j.net_received_inventory_units_sourcing as `Net Received Inventory Units Sourcing`,
        j.net_received_inventory_units_manufacturing as `Net Received Inventory Units Manufacturing`,
        j.sellable_on_hand_inventory_cost_amount_sourcing as `Sellable On Hand Inventory Cost Amount Sourcing`,
        j.sellable_on_hand_inventory_cost_amount_manufacturing as `Sellable On Hand Inventory Cost Amount Manufacturing`,
        j.sellable_on_hand_inventory_cost_currency_code_sourcing as `Sellable On Hand Inventory Cost Currency Code Sourcing`,
        j.sellable_on_hand_inventory_cost_currency_code_manufacturing as `Sellable On Hand Inventory Cost Currency Code Manufacturing`,
        j.sellable_on_hand_inventory_units_sourcing as `Sellable On Hand Inventory Units Sourcing`,
        j.sellable_on_hand_inventory_units_manufacturing as `Sellable On Hand Inventory Units Manufacturing`,
        j.unsellable_on_hand_inventory_cost_amount_sourcing as `Unsellable On Hand Inventory Cost Amount Sourcing`,
        j.unsellable_on_hand_inventory_cost_amount_manufacturing as `Unsellable On Hand Inventory Cost Amount Manufacturing`,
        j.unsellable_on_hand_inventory_cost_currency_code_sourcing as `Unsellable On Hand Inventory Cost Currency Code Sourcing`,
        j.unsellable_on_hand_inventory_cost_currency_code_manufacturing as `Unsellable On Hand Inventory Cost Currency Code Manufacturing`,
        j.unsellable_on_hand_inventory_units_sourcing as `Unsellable On Hand Inventory Units Sourcing`,
        j.unsellable_on_hand_inventory_units_manufacturing as `Unsellable On Hand Inventory Units Manufacturing`,
        j.aged_90_plus_days_sellable_inventory_cost_amount_sourcing as `Aged 90 Plus Days Sellable Inventory Cost Amount Sourcing`,
        j.aged_90_plus_days_sellable_inventory_cost_amount_manufacturing as `Aged 90 Plus Days Sellable Inventory Cost Amount Manufacturing`,
        j.aged_90_plus_days_sellable_inventory_cost_currency_code_sourcing as `Aged 90 Plus Days Sellable Inventory Cost Currency Code Sourcing`,
        j.aged_90_plus_days_sellable_inventory_cost_currency_code_manufacturing as `Aged 90 Plus Days Sellable Inventory Cost Currency Code Manufacturing`,
        j.aged_90_plus_days_sellable_inventory_units_sourcing as `Aged 90 Plus Days Sellable Inventory Units Sourcing`,
        j.aged_90_plus_days_sellable_inventory_units_manufacturing as `Aged 90 Plus Days Sellable Inventory Units Manufacturing`,
        j.unhealthy_inventory_cost_amount_sourcing as `Unhealthy Inventory Cost Amount Sourcing`,
        j.unhealthy_inventory_cost_amount_manufacturing as `Unhealthy Inventory Cost Amount Manufacturing`,
        j.unhealthy_inventory_cost_currency_code_sourcing as `Unhealthy Inventory Cost Currency Code Sourcing`,
        j.unhealthy_inventory_cost_currency_code_manufacturing as `Unhealthy Inventory Cost Currency Code Manufacturing`,
        j.unhealthy_inventory_units_sourcing as `Unhealthy Inventory Units Sourcing`,
        j.unhealthy_inventory_units_manufacturing as `Unhealthy Inventory Units Manufacturing`

    from source as j

)

select * from renamed

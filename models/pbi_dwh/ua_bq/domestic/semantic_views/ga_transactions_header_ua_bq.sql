with src as (
    select distinct
    view_id as `GA View ID`
    ,vg
    ,cast(order_date as integer) as `Transaction Date`
    ,traffic_source_key as `Traffic Source ID`
    ,click_info_key as `Click Info ID`
    ,device_key as `Device ID`
    ,order_key as `Order ID`
    ,hit_key as `Hit ID`
    ,session_key as `Session ID`    
    ,consent_info as `Consent Info`
    ,Transaction_ID	  as `Transaction ID`    
    ,Currency_Code	      as `Currency Code`
    ,Transaction_Revenue	 as `GA Transaction Revenue`
    ,Transaction_Shipping	 as `GA Transaction Shipping`
    ,Transaction_Tax	 as `GA Transaction Tax`
    ,Transaction_Revenue_EUR	 as `GA Transaction Revenue EUR`
    ,Transaction_Shipping_EUR	 as `GA Transaction Shipping EUR`
    ,Transaction_Tax_EUR	 as `GA Transaction Tax EUR`
    ,diff_revenue_hdr_item_total as `Diff Revenue Hdr-ItemTotal`
    ,affiliation as `Affiliation`
    ,transaction_coupon as `Transaction Coupon` 
    from {{ ref('fact_doms_order_ua_bq') }}
)

select * from src


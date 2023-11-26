with src as (
    select 
    view_id as `GA View ID`
    ,vg
    ,cast(order_date as integer) as `Transaction Date`
    ,traffic_source_key as `Traffic Source ID`
    ,click_info_key as `Click Info ID`
    ,device_key as `Device ID`
    ,order_key as `Order ID`
    ,hit_key as `Hit ID`
    ,session_key as `Session ID` 
    ,product_ua_bq_key	  as `Product UA BQ ID`
    ,'ST'	              as `Unit Of Measure`    
    ,Item_Revenue	      as `GA Item Revenue`
    ,Item_Revenue_EUR     as `GA Item Revenue EUR`
    ,cast(item_Quantity as integer)	 as `GA Item Quantity`
    ,cast(item_list_position as integer)   as `GA Item Position `
    ,item_coupon_code     as `GA Item Coupon Code`   
    
    from {{ ref('fact_doms_orderline_ua_bq') }}
)

select * from src

ga_transactions_item_ua_bq
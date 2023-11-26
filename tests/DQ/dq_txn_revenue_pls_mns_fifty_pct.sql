
{{ config(
    enabled=true,
    tags=["DQ"]
 ) }}

with data as (
select sum(transaction_revenue) as today_revenue,vg,view_id,order_date revenue_date from dwh.fact_order_ua_bq
where 
order_date >= date_format(current_date()-3 , "yyyyMMdd") and order_date <= date_format(current_date()-2 , "yyyyMMdd")
group by vg,view_id,order_date),

prvious_day_revenue as (select *, lead(today_revenue) over (partition by vg,view_id order by revenue_date desc) as yesterday_revenue from data),

plusminuspct as (select *,((yesterday_revenue*50)/100) as ten_pct_of_yesterday_revenue, yesterday_revenue+(((yesterday_revenue)*50)/100) as yesterday_revenue_plus_50,yesterday_revenue-(((yesterday_revenue)*50)/100) as yesterday_revenue_minus_50 from prvious_day_revenue)

select * from plusminuspct where yesterday_revenue is not null
 and (today_revenue > yesterday_revenue_plus_50 or today_revenue < yesterday_revenue_minus_50)
order by view_id,revenue_date
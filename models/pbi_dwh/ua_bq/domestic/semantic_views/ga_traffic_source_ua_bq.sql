with src as (
    select distinct
        traffic_source_key as `Traffic Source ID` 
        ,referral_path     as `Referral Path` 
        ,campaign as `Campaign`
        ,source   as `Traffic Source`
        ,medium   as `Traffic Medium`
        ,keyword  as `Keyword`
        ,ad_content as `Ad Content`
        ,is_true_direct as `Was Source Session Direct`
    from {{ ref('dim_doms_traffic_source_ua_bq') }}
)

select * from src


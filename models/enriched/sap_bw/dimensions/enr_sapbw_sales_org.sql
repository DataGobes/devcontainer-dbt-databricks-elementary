{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"   
) }}

with source as (

    select *
    from
        {{ source('src_sap_bw', 'sap_bw_master_sales_org') }}

),

final as (

    select 
        coalesce(cast(`/BIC/GSALESORG` as varchar(256)), 'Not Set') as sales_org,
        coalesce(cast(LANGU as varchar(256)), 'Not Set') as language,
        coalesce(cast(TXTSH as varchar(256)), 'Not Set') as text_short,
        coalesce(cast(TXTMD as varchar(256)), 'Not Set') as text_medium,
        coalesce(cast(TXTLG as varchar(256)), 'Not Set') as text_long,
        current_timestamp() as meta_insert_ts  
    from source    
)

select * from final
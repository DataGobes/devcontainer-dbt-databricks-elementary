{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"  
) }}

with source as (

    select *
    from
        {{ source('src_sap_bw', 'sap_bw_master_buying_group') }}

),

final as (

    select 
        coalesce(cast(`/BIC/GC_0001` as varchar(256)), 'Not Set') as reporting_unit_code,
        coalesce(cast(`/BIC/UC_1183` as varchar(256)), 'Not Set') as buying_group_code,       
        coalesce(cast(TXTSH as varchar(256)), 'Not Set') as text_short,
        coalesce(cast(TXTMD as varchar(256)), 'Not Set') as text_medium,
        coalesce(cast(TXTLG as varchar(256)), 'Not Set') as text_long,
        current_timestamp() as meta_insert_ts  
    from source 
)   

select * from final
{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"  
) }}

with source as (

    select *
    from
        {{ source('src_sap_bw', 'sap_bw_master_billing_type_next') }}

),

final as (

    select 
        coalesce(cast(`/BIC/GC_0001` as varchar(256)), 'Not Set') as reporting_unit_code,
        coalesce(cast(`/BIC/UBILL_TYP` as varchar(256)), 'Not Set') as billing_type_code,
        coalesce(cast(LANGU as varchar(256)), 'Not Set') as language,
        coalesce(cast(TXTSH as varchar(256)), 'Not Set') as text_short,
        coalesce(cast(TXTMD as varchar(256)), 'Not Set') as text_medium,
        coalesce(cast(TXTLG as varchar(256)), 'Not Set') as text_long,
        current_timestamp() as meta_insert_ts  
    from source   
)

select * from final
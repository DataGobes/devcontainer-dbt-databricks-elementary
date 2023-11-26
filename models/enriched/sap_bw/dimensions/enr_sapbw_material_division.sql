{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"   
) }}

with source as (

    select *
    from
        {{ source('src_sap_bw', 'sap_bw_master_material_div') }}

),

final as (

    select 
        coalesce(cast(`/BIC/GC_0036` as varchar(256)), 'Not Set') as material_division,
        coalesce(cast(LANGU as varchar(256)), 'Not Set') as language,
        coalesce(cast(TXTSH as varchar(256)), 'Not Set') as text_short,
        coalesce(cast(TXTMD as varchar(256)), 'Not Set') as text_medium,
        coalesce(cast(TXTLG as varchar(256)), 'Not Set') as text_long,
        current_timestamp() as meta_insert_ts  
    from source   
)

select * from final
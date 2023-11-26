{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"
) }}

with source as (

    select *
    from
        {{ source('src_sap_bw', 'sap_bw_master_material') }}

),

final as (

    select 
        coalesce(cast(`/BIC/GMATERIAL` as varchar(256)), 'Not Set') as material_code,
        coalesce(cast(LANGU as varchar(256)), 'Not Set') as language,
        coalesce(cast(TXTSH as varchar(256)), 'Not Set') as text_short,
        coalesce(cast(TXTMD as varchar(256)), 'Not Set') as text_medium,
        coalesce(cast(TXTLG as varchar(256)), 'Not Set') as text_long,
        {{ sort_language('LANGU', '`/BIC/GMATERIAL`') }} as language_id,
        current_timestamp() as meta_insert_ts  
    from source   
)

select * from final
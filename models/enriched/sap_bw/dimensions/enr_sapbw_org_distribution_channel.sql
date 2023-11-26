{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"  
) }}

with source as (

    select *
    from
        {{ source('src_sap_bw', 'sap_bw_master_org_distr_channel') }}

),

final as (

    select 
        coalesce(cast(`/BIC/GDISTR_CH` as varchar(256)), 'Not Set') as distribution_channel_code,
        coalesce(cast(LANGU as varchar(256)), 'Not Set') as language,
        coalesce(cast(TXTSH as varchar(256)), 'Not Set') as text_short,
        current_timestamp() as meta_insert_ts  
    from source
)

select * from final
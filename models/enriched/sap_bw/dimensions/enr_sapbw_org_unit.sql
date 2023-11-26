{{ config(
    schema = "restricted_enriched",
    materialized = "table",
    format = "delta"  
) }}

with source as (

    select *
    from
        {{ source('src_sap_bw', 'sap_bw_master_org_unit') }}

),

renamed as (

-- custom date conversion because date/string is mixed in the source
    select distinct
        coalesce(cast(`/BIC/GORGUNIT` as varchar(256)), 'Not Set') as org_unit,
        coalesce(cast(LANGU as varchar(256)), 'Not Set') as language,
        coalesce(cast(TXTSH as varchar(256)), 'Not Set') as text_short,
        coalesce(cast(TXTMD as varchar(256)), 'Not Set') as text_medium,
        coalesce(cast(TXTLG as varchar(256)), 'Not Set') as text_long,
        coalesce(cast(replace(substring(DATEFROM, 1, 10),'-','') as bigint), 10000101) as date_from,       
        coalesce(cast(replace(substring(DATETO, 1, 10),'-','') as bigint), 99991231) as date_to,
        meta_src_folder
    from source   
),

-- deduplication because date exists in source in different formats that evaluate to the same record here. 
dedup as (

    select 
        *,
        row_number() over (partition by org_unit, language, date_from order by meta_src_folder desc) as rownum
    from renamed
),

final as (

    select 
        *,
        current_timestamp() as meta_insert_ts   
    from dedup
    where rownum = 1
)

select * from final
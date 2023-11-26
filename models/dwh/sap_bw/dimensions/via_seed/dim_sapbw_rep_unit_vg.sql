{{ config(
    schema = "restricted_dwh",
    materialized = "table",
    format = "delta"  
) }}
      
select  distinct
    rep_unit_code,
    rep_unit_desc,
    case 
        when rep_unit_code = '1000' then 'DE' 
        when rep_unit_desc like 'VG %' then left(trim(replace(substr(rep_unit_desc, 1,5),'VG','')),2) 
    end as vg
from {{ ref('bw_reporting_unit_seed') }}




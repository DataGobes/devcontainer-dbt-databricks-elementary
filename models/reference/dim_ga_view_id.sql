{{ config(
        materialized='incremental',
        unique_key='view_id',
        incremental_strategy='merge'
) }}

with vghs as (
select distinct
  `View_ID` as view_id,
  `vg` as vg,
  `vg_ga_name` as vg_ga_name, 
  `Platform` as platform_website,
  `from_dt` as from_dt,
  `to_dt` as to_dt,
  cast(replace(`from_dt`,'-') as bigint) as from_dt_yyyymmdd,
  cast(replace(`to_dt`,'-') as bigint) as to_dt_yyyymmdd,
  `is_active` as is_active,
  date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss.SSS z') as meta_insert_ts 
    from  {{ ref('vg_history_seed') }}
),


inserts as (
    select * from vghs
    {% if is_incremental() %}
     where view_id not IN (select view_id from {{ this }})
    {% endif %}
),

updates as (
    select * from vghs
    {% if is_incremental() %}
     where view_id IN (select view_id from {{ this }})
    {% endif %}
)


SELECT * FROM inserts 
 union 
SELECT * FROM updates 
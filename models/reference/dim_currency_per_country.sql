{{ config(
        materialized='incremental',
        unique_key='country_name',
        incremental_strategy='merge'
) }}

with cps as (
select 
    `Country` as Country_Name,  
    Country_2Letter_Code as  Country_Code2     ,
    Currency_Name           ,
    CURRENCY_CODE_ISO			,
    `Fractional_Unit` as  Fractional_Unit       ,
    Currency_to_fu
from {{ ref('Currencies_per_country_seed') }}
),

inserts as (
    select * from cps
    {% if is_incremental() %}
     where country_name not IN (select country_name from {{ this }})
    {% endif %}
),

updates as (
    select * from cps
    {% if is_incremental() %}
     where country_name IN (select country_name from {{ this }})
    {% endif %}
)


SELECT * FROM inserts 
 union 
SELECT * FROM updates 
{{ config(
        materialized='incremental',
        unique_key='country_name',
        incremental_strategy='merge'
) }}


with seed_dim_cnt as 
(select * from {{ ref('dim_country_seed') }}),

inserts as (
    select * from seed_dim_cnt
    {% if is_incremental() %}
     where country_name not IN (select country_name from {{ this }})
    {% endif %}
),

updates as (
    select * from seed_dim_cnt
    {% if is_incremental() %}
     where country_name IN (select country_name from {{ this }})
    {% endif %}
)


SELECT * FROM inserts 
 union 
SELECT * FROM updates 


{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['view_id', 'visit_id', 'full_visitor_id', 'visit_date']   
          ) 
}}

with source as (
    select * 
    from {{ source('src_ua_bq_raw_professional', 'ua_bq_professional_geonetwork') }}
               {% if is_incremental() %}
                  where meta_insert_ts > (select max(meta_insert_ts) from {{ this }})
               {% endif %}
               )

select 
     visit_date
    ,visitid as visit_id
    ,fullVisitorId as full_visitor_id
    ,continent
    ,subContinent as sub_continent
    ,country
    ,region
    ,metro
    ,city
    ,cityId as city_id
    ,networkDomain as network_domain
    ,latitude
    ,longitude
    ,networkLocation as network_location
    ,view_id
    ,meta_source
    ,vg
    ,current_timestamp() as meta_insert_ts
    ,{{ dbt_utils.generate_surrogate_key(['continent', 'sub_continent', 'country', 'region', 'metro', 'city', 'city_id', 'network_domain', 'latitude', 'longitude', 'network_location']) }} as geonetwork_key
 from source
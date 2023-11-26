with source as (
    select * from {{ source('src_ga4','ga4_domestic_events') }}
),

add_array as (
    select
        *,
        {{ jsonstring_to_array('geo') }} as geo_array
    from source
),

flattened as (
    select
        *,
        geo_array[0] as continent,
        geo_array[1] as country,
        geo_array[2] as region,
        geo_array[3] as city,
        geo_array[4] as sub_continent,
        geo_array[5] as metro
    from add_array
),

add_keys as (
    select
        *,
        {{ event_key() }} as event_key,
        {{ dbt_utils.generate_surrogate_key(['continent',
                                    'country',
                                    'region',
                                    'city',
                                    'sub_continent',
                                    'metro']) 
        }} as geo_key
    from flattened
),

final as (
    select
        event_key,
        geo_key,
        event_date,
        event_name,
        continent,
        sub_continent,
        country,
        region,
        metro,
        city,
        current_timestamp() as meta_insert_ts
    from add_keys
)

select * from final

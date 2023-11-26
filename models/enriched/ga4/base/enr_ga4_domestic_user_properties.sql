with source as (
    select * from {{ source('src_ga4','ga4_domestic_events') }}
),

add_client_key as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key(['user_pseudo_id', 'stream_id']) }} as client_key
    from source
),

most_recent as (
    select
        *,
        row_number() over (partition by client_key order by event_timestamp desc) as rn
    from add_client_key
),

add_unnested as (
    select
        *,
        {{ unnest_json_string('user_properties') }} as up_unnested
    from most_recent
    where rn = 1
),

add_value_struct as (
    select
        *,
        from_json(up_unnested[1], 'STRUCT<f: ARRAY<STRUCT<v: STRING>>>').f.v as up_value_struct --noqa
    from add_unnested
),

flattened as (
    select
        *,
        up_unnested[0] as user_property,
        up_value_struct[0] as up_value_string,
        up_value_struct[1] as up_value_int,
        up_value_struct[2] as up_value_double,
        up_value_struct[4] as up_value_set_timestamp_micros
    from add_value_struct
),

final as (
    select
        client_key,
        user_property,
        up_value_string,
        up_value_int,
        up_value_double,
        up_value_set_timestamp_micros,
        coalesce(up_value_string, up_value_int, up_value_double) as up_value,
        current_timestamp() as meta_insert_ts
    from flattened
)

select * from final

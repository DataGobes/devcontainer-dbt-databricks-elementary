{{ config (
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['event_key','event_parameter']
  )
}}

with source as (
    select * from {{ source('src_ga4','ga4_domestic_events') }}
    {% if is_incremental() %}
        where meta_insert_ts >= (select max(meta_insert_ts) from {{ this }})
    {% endif %}
),

add_event_key as (
    select
        *,
        {{ event_key() }} as event_key
    from source
),

add_unnested as (
    select
        *,
        {{ unnest_json_string('event_params') }} as ep_unnested
    from add_event_key
),

add_value_struct as (
    select
        *,
        from_json(ep_unnested[1], 'STRUCT<f: ARRAY<STRUCT<v: STRING>>>') as ep_value_struct
    from add_unnested
),

flattened as (
    select
        *,
        ep_unnested[0] as event_parameter,
        ep_value_struct.f.v[0] as param_value_string,
        ep_value_struct.f.v[1] as param_value_int,
        ep_value_struct.f.v[2] as param_value_double
    from add_value_struct
),

add_single_value_column as (
    select
        *,
        coalesce(param_value_string, param_value_int, param_value_double) as parameter_value
    from flattened
),

final as (
    select
        event_key,
        event_date,
        event_name,
        event_parameter,
        param_value_string,
        param_value_int,
        param_value_double,
        parameter_value,
        vg,
        property_id,
        current_timestamp() as meta_insert_ts
    from add_single_value_column
)

select * from final

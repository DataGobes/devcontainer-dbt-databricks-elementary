-- This file contains macro's specific for GA4

-- This macro will create a surrogate key for an event. 
-- In a macro because it is used a lot in the enriched layer.
{% macro event_key() %}
    (
        {{ dbt_utils.generate_surrogate_key([
            'event_date', 
            'event_timestamp', 
            'event_name', 
            'event_params', 
            'user_pseudo_id', 
            'stream_id'
        ]) }}
    )
{% endmacro %}

-- Convert a nested GBQ field to an array for further transformations
-- To be used for single level nested fields.
-- 1. Convert to json_string, 2. Apply schema, 3. Navigate to lowest level.
-- Parameters: Column to unnest and schema to apply. If not added then it will take the default.
{% macro jsonstring_to_array(column_name, json_schema = "'STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>'") %}
    from_json({{column_name}}, {{json_schema}}).v.f.v
{% endmacro %}

-- Same as jsonstring_to_array macro but with different schema and navigation path.
-- To be used to unnest and explode repeated fields in the GA4 raw schema
-- 
{% macro unnest_json_string(column_name, json_schema = "'STRUCT<v: ARRAY<STRUCT<v: STRUCT<f: ARRAY<STRUCT<v: STRING>>>>>>'") %}
    explode(from_json({{column_name}}, {{json_schema}}).v.v.f.v)
{% endmacro %}

-- Same as jsonstring_to_array macro but with different schema and navigation path.
-- To be used for 2nd hierarchy nested fields that come as an output of the unnest_json_string macro
{% macro nested_jsonstring_to_array(column_name, json_schema = "'STRUCT<f: ARRAY<STRUCT<v: STRING>>>'") %}
    from_json({{column_name}}, {{json_schema}}).f.v
{% endmacro %}

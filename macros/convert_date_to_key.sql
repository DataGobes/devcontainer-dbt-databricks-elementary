{%- macro convert_date_to_key(date_column) -%}
    cast(date_format({{ date_column}}, 'yyyyMMdd') as int) AS {{ date_column}}_key
{%- endmacro -%}
{%- macro analyze_table() -%}
    ANALYZE TABLE {{ this }} COMPUTE STATISTICS FOR ALL COLUMNS;
{%- endmacro -%}
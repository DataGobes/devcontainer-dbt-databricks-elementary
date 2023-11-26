-- Macro to create a time_key from a timestamp column. 
-- Output is in the format of the key of the table reference.dim_time

{%- macro create_time_key(timestamp_column) -%}
       cast(       
              concat('1', 
                     lpad( hour( {{timestamp_column}} ), 2, '0'), 
                     lpad( minute( {{timestamp_column}} ), 2, '0'), 
                     lpad( second( {{timestamp_column}} ), 2, '0')
              )
       as integer
       )
{%- endmacro -%}
  
{%- macro time_key_from_unix(unix_timestamp_column) -%}
       cast(      
              concat('1', 
                     lpad( hour( {{ dbt_date.from_unixtimestamp( "{{unix_timestamp_column}}" ) }} ), 2, '0'), 
                     lpad( minute( {{ dbt_date.from_unixtimestamp( "{{unix_timestamp_column}}" ) }} ), 2, '0'), 
                     lpad( second( {{ dbt_date.from_unixtimestamp( "{{unix_timestamp_column}}" ) }} ), 2, '0')
              )
       as integer
       )
{%- endmacro -%}
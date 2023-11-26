{% macro delta_tbl_ops(results) %}

  {% if execute %}
  {{ log("========== Begin Summary ==========", info=True) }}
  {% for res in results -%}
  {% set tbl_name =  (res.node.relation_name )  %}
  {{ is_delta_table(tbl_name)}} 
  {% endfor %}
  {{ log("========== End Summary ==========", info=True) }}
  {% endif %}

{% endmacro %}


{% macro is_delta_table(table_name) %}
  
{#-------Generate Queries --------#}
  {% set delta_table_check_query = "SHOW TBLPROPERTIES " ~ table_name %}
  {% set analyze_query = "ANALYZE TABLE "  ~ table_name ~" COMPUTE STATISTICS FOR ALL COLUMNS"%}
  {% set optimize_query = "OPTIMIZE "  ~ table_name %}
  {% set vacuum_query = "VACUUM  "  ~ table_name %}

{#----------Check is it a delta Table ------------------#}
  {% set tbl_properties = run_query(delta_table_check_query) %}
  {% if tbl_properties|length != 0 %}
  {% set first_val = tbl_properties.rows[0].values()%}
 {# {{ log(first_val, info=True) }} #}
  {% set return_val = true if 'delta' in first_val|lower  %}
  {# {{ log(return_val, info=True) }} #}

{#-------If this is delta table execute the operational queries--------------#}
  {% if return_val %}
    {{ log( table_name ~" This is delta table", info=True) }}

    {#-------Execute Vacuum command--------------#}
    {{ log("VACUUM  table command started for " ~ table_name, info=True) }}
    {{ log(vacuum_query,info=True)}}
    {% do run_query(vacuum_query)  %}
    {{ log("VACUUM  table command completed for " ~ table_name, info=True) }}

    {#-------Execute Optimize command--------------#}
    {{ log("OPTIMIZE table command started for "~ table_name, info=True) }}
    {{ log(optimize_query,info=True)}}
    {% do run_query(optimize_query) %}
    {{ log("OPTIMIZE table command completed for "~ table_name, info=True) }}

    {#-------Execute Analyze table command--------------#}
    {{ log("Analyze table command started for "~ table_name, info=True) }}
    {{ log(analyze_query,info=True)}}
    {% do run_query(analyze_query) %}
    {{ log("Analyze table command completed for "~ table_name, info=True) }}


  {% else %}
   {{ log(table_name ~ " is not a delta table", info=True) }}
  {%endif %}

{#-------------If the table is non delta table exit ----------------#}
  {% else %}
  {{ log( table_name ~ " is not a delta table", info=True) }}
  {%endif %}
{% endmacro %}

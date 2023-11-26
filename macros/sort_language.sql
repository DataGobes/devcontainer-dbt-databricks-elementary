{% macro sort_language(language, pk) %}
    ( row_number() over (partition by {{ pk }} order by (
        case when {{ language }} = 'E' then 1
             when {{ language }} = 'D' then 2
             else row_number() over (partition by {{ pk }} order by {{ language }}) + 2
        end )
        )
    )
{% endmacro %}

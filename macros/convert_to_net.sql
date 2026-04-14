{% macro convert_to_net(column_name, tax_rate=0.13) %}
CASE 
        WHEN {{ column_name }} IS NOT NULL 
        THEN round(cast({{ column_name }} / (1 + {{ tax_rate }}) as numeric), 2) 
        ELSE 0 
    END
{% endmacro %}
{% macro clean_text(col) %}
nullif(trim({{ col }}::text), '')
{% endmacro %}
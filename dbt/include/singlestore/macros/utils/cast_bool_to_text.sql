{% macro singlestore__cast_bool_to_text(field) %}

    if(isnull({{ field }}), null, if({{ field }}, "true", "false"))

{% endmacro %}

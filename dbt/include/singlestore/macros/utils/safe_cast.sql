{% macro singlestore__safe_cast(field, type) %}

    ({{field}} !:> {{type}})

{% endmacro %}

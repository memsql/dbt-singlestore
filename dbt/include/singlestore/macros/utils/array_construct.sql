{% macro singlestore__array_construct(inputs, data_type) -%}
    {% if inputs|length > 0 %}
    [ {{ inputs|join(', ') }} ]
    {% else %}
    CAST(NULL AS ARRAY<{{ data_type }}>)
    {% endif %}
{%- endmacro %}

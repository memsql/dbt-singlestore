{% macro singlestore__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}

    {% if limit_num -%}
    substring_index(
        group_concat(
            {{ measure }}
            {%- if order_by_clause %}
                {{ order_by_clause }}
            {%- endif %}
            SEPARATOR {{ delimiter_text }}
            ),
        {{ delimiter_text }},
        {{ limit_num }}
    )
    {%- else %}
    group_concat(
        {{ measure }}
        {%- if order_by_clause %}
            {{ order_by_clause }}
        {%- endif %}
        SEPARATOR {{ delimiter_text }}
    )
    {%- endif %}

{%- endmacro %}

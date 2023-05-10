{% macro singlestore__datediff(first_date, second_date, datepart) -%}

    timestampdiff(
        {{ datepart }},
        {{ first_date }},
        {{ second_date }}
        )

{%- endmacro %}

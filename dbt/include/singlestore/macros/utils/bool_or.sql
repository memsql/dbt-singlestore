{% macro singlestore__bool_or(expression) -%}

    sum({{ expression }})

{%- endmacro %}

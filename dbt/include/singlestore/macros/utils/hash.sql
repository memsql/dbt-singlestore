{% macro singlestore__hash(field) -%}

    md5({{ field }} :> {{ api.Column.translate_type('string') }})

{%- endmacro %}

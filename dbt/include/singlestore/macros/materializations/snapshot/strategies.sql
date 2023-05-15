{% macro singlestore__snapshot_hash_arguments(args) -%}
    md5(concat_ws('|', {%- for arg in args -%}
        coalesce({{ arg }} :> text, '')
        {% if not loop.last %}, {% endif %}
    {%- endfor -%}))
{%- endmacro %}

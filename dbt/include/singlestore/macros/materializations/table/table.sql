{%- macro singlestore__get_table_columns_and_constraints(create_definition_str, undefined_shard_key) -%}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set raw_column_constraints = adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
    {%- set raw_model_constraints = adapter.render_raw_model_constraints(raw_constraints=model['constraints'], undefined_shard_key=undefined_shard_key) -%}
    (
    {% for c in raw_column_constraints -%}
      {{ c }}{{ "," if not loop.last or raw_model_constraints or create_definition_str }}
    {% endfor %}
    {% for c in raw_model_constraints -%}
        {{ c }}{{ "," if not loop.last or create_definition_str }}
    {% endfor -%}
        {{ create_definition_str }}
    )
{% endmacro %}
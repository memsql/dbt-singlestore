{% macro singlestore__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade does nothing in SingleStore)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} add column {{ adapter.quote(tmp_column) }} {{ new_column_type }};
    update {{ relation }} set {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
    alter table {{ relation }} drop column {{ adapter.quote(column_name) }};
    alter table {{ relation }} change {{ adapter.quote(tmp_column) }} {{ adapter.quote(column_name) }}
  {% endcall %}

{% endmacro %}

{% macro singlestore__get_empty_schema_sql(columns) %}
    {%- set col_err = [] -%}
    select
    {% for i in columns %}
      {%- set col = columns[i] -%}
      {%- if col['data_type'] is not defined -%}
        {{ col_err.append(col['name']) }}
      {%- endif -%}
      {% set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] %}
      (null :> {{ col['data_type'] }}) as {{ col_name }}{{ ", " if not loop.last }}
    {%- endfor -%}
    {%- if (col_err | length) > 0 -%}
      {{ exceptions.column_type_missing(column_names=col_err) }}
    {%- endif -%}
{% endmacro %}

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

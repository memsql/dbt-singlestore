{% macro singlestore__snapshot_merge_sql_update(target, source, insert_cols) -%}
  {%- set columns = config.get("snapshot_table_column_names") or get_snapshot_table_column_names() -%}

  update {{ target }},
                      (
                        select
                          {{ columns.dbt_scd_id }} as dbt_scd_id,
                          dbt_change_type,
                          ( {{ columns.dbt_valid_to }} :> datetime ) as dbt_valid_to
                        from {{ source }}) as dbt_internal_source
      set {{ target }}.{{ columns.dbt_valid_to }} = dbt_internal_source.dbt_valid_to
    where dbt_internal_source.dbt_scd_id = {{ target }}.{{ columns.dbt_scd_id }}
      and dbt_internal_source.dbt_change_type in ('update','delete')
    {% if config.get("dbt_valid_to_current") %}
      and ( {{ target }}.{{ columns.dbt_valid_to }} = ( {{ config.get('dbt_valid_to_current') }} :> datetime ) or
          {{ target }}.{{ columns.dbt_valid_to }} is null)
    {% else %}
      and {{ target }}.{{ columns.dbt_valid_to }} is null
    {% endif %}
{% endmacro %}


{% macro singlestore__snapshot_merge_sql_insert(target, source, insert_cols) -%}
  {%- set insert_cols_csv = insert_cols | join(', ') -%}

  insert into {{ target }} ({{ insert_cols_csv }})
  select {% for column in insert_cols -%}
      dbt_internal_source.{{ column }} {% if not loop.last -%}, {% endif -%}
          {% endfor -%}
  from {{ source }} as dbt_internal_source
  where dbt_internal_source.dbt_change_type = 'insert'
{% endmacro %}

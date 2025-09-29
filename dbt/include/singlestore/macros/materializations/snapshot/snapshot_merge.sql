{% macro singlestore__snapshot_merge_sql_update(target, source, insert_cols) -%}
    update {{ target }}, (select dbt_scd_id, dbt_change_type, ( dbt_valid_to :> datetime ) as dbt_valid_to from {{ source }}) as dbt_internal_source
       set {{ target }}.dbt_valid_to = dbt_internal_source.dbt_valid_to
     where dbt_internal_source.dbt_scd_id = {{ target }}.dbt_scd_id
       and dbt_internal_source.dbt_change_type in ('update','delete')
      {% if config.get("dbt_valid_to_current") %}
       and ( {{ target }}.dbt_valid_to = ( {{ config.get('dbt_valid_to_current') }} :> datetime ) or
            {{ target }}.dbt_valid_to is null)
     {% else %}
       and {{ target }}.dbt_valid_to is null
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

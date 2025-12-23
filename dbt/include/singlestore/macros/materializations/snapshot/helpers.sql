{% macro singlestore__snapshot_staging_table_deletes(strategy, source_sql, target_relation, columns) -%}
    with snapshot_query as (
        {{ source_sql }}
    ),
    snapshotted_data as (
        select *, {{ unique_key_fields(strategy.unique_key) }}
        from {{ target_relation }}
        where
            {% if config.get('dbt_valid_to_current') %}
                ( {{ columns.dbt_valid_to }} = ( {{ config.get('dbt_valid_to_current') }} :> datetime ) or {{ columns.dbt_valid_to }} is null)
            {% else %}
                {{ columns.dbt_valid_to }} is null
            {% endif %}
    ),
    deletes_source_data as (
        select *, {{ unique_key_fields(strategy.unique_key) }}
        from snapshot_query
    )
    select
            'delete' as dbt_change_type,
            source_data.*,
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_from }},
            {{ snapshot_get_time() }} as {{ columns.dbt_updated_at }},
            {{ snapshot_get_time() }} as {{ columns.dbt_valid_to }},
            snapshotted_data.{{ columns.dbt_scd_id }}
        {%- if strategy.hard_deletes == 'new_record' -%}
            , snapshotted_data.{{ columns.dbt_is_deleted }}
        {%- endif %}

    from snapshotted_data
    left join deletes_source_data as source_data
        on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
    where {{ unique_key_is_null(strategy.unique_key, "source_data") }}
    {%- if strategy.hard_deletes == 'new_record' %}
      and not (
        snapshotted_data.{{ columns.dbt_is_deleted }} = 'True'
        and
        {% if config.get('dbt_valid_to_current') -%}
            snapshotted_data.{{ columns.dbt_valid_to }} = ( {{ config.get('dbt_valid_to_current') }} :> datetime )
        {%- else -%}
            snapshotted_data.{{ columns.dbt_valid_to }} is null
        {%- endif %}
      )
    {%- endif %}
{%- endmacro %}


{% macro singlestore__snapshot_staging_table_updates(strategy, source_sql, target_relation, columns) %}
    with snapshot_query as (
        {{ source_sql }}
    ),
    snapshotted_data as (
        select *, {{ unique_key_fields(strategy.unique_key) }}
          from {{ target_relation }}
        where
            {% if config.get('dbt_valid_to_current') %}
                ( {{ columns.dbt_valid_to }} = ( {{ config.get('dbt_valid_to_current') }} :> datetime ) or {{ columns.dbt_valid_to }} is null)
            {% else %}
                {{ columns.dbt_valid_to }} is null
            {% endif %}
    ),
    updates_source_data as (
            select *, {{ unique_key_fields(strategy.unique_key) }},
            {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_to }}
        from snapshot_query
    )
    select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.{{ columns.dbt_scd_id }}
        {%- if strategy.hard_deletes == 'new_record' -%}
            , snapshotted_data.{{ columns.dbt_is_deleted }}
        {%- endif %}
    from updates_source_data as source_data
    join snapshotted_data
        on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
    where ({{ strategy.row_changed }})
    {%- if strategy.hard_deletes == 'new_record' -%}
        or snapshotted_data.{{ columns.dbt_is_deleted }} = 'True'
    {%- endif -%}
{%- endmacro %}


{% macro singlestore__snapshot_staging_table_insertions(strategy, source_sql, target_relation, columns) %}
    with snapshot_query as (
        {{ source_sql }}
    ),
    snapshotted_data as (
        select *, {{ unique_key_fields(strategy.unique_key) }}
          from {{ target_relation }}
        where
            {% if config.get('dbt_valid_to_current') %}
                ( {{ columns.dbt_valid_to }} = ( {{ config.get('dbt_valid_to_current') }} :> datetime ) or {{ columns.dbt_valid_to }} is null)
            {% else %}
                {{ columns.dbt_valid_to }} is null
            {% endif %}
    ),
    insertions_source_data as (
            select *, {{ unique_key_fields(strategy.unique_key) }},
            {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
            {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
            {{ singlestore__get_dbt_valid_to_current(strategy, columns) }},
            {{ strategy.scd_id }} as {{ columns.dbt_scd_id }}
        from snapshot_query
    )
    select
            'insert' as dbt_change_type,
            source_data.*
        {%- if strategy.hard_deletes == 'new_record' -%}
            ,'False' as {{ columns.dbt_is_deleted }}
        {%- endif %}
    from insertions_source_data as source_data
    left outer join snapshotted_data
        on {{ unique_key_join_on(strategy.unique_key, "snapshotted_data", "source_data") }}
    where {{ unique_key_is_null(strategy.unique_key, "snapshotted_data") }}
       or (
            {{ unique_key_is_not_null(strategy.unique_key, "snapshotted_data") }}
            and ({{ strategy.row_changed }})
            {%- if strategy.hard_deletes == 'new_record' -%}
                or snapshotted_data.{{ columns.dbt_is_deleted }} = 'True'
            {%- endif -%}
       )
{% endmacro %}


{% macro singlestore__snapshot_staging_table_deletion_records(strategy, source_sql, target_relation, columns) -%}
    {# Fresh SCD id for the new 'deleted' record #}
    {% set new_scd_id = snapshot_hash_arguments([columns.dbt_scd_id, snapshot_get_time()]) %}

    {# Column schemas for safe, ordered projection #}
    {% set snapshotted_cols = get_list_of_column_names(get_columns_in_relation(target_relation)) %}
    {% set source_sql_cols = get_column_schema_from_query(source_sql) %}

    with snapshot_query as (
        {{ source_sql }}
    ),
    snapshotted_data as (
        select *, {{ unique_key_fields(strategy.unique_key) }}
        from {{ target_relation }}
        where
            {% if config.get('dbt_valid_to_current') %}
                ( {{ columns.dbt_valid_to }} = ( {{ config.get('dbt_valid_to_current') }} :> datetime )
                  or {{ columns.dbt_valid_to }} is null )
            {% else %}
                {{ columns.dbt_valid_to }} is null
            {% endif %}
    ),
    deletes_source_data as (
        select *, {{ unique_key_fields(strategy.unique_key) }}
        from snapshot_query
    )
    select
        'insert' as dbt_change_type,

        {%- for col in source_sql_cols -%}
            {%- if col.name in snapshotted_cols -%}
                snapshotted_data.{{ adapter.quote(col.column) }},
            {%- else -%}
                NULL as {{ adapter.quote(col.column) }},
            {%- endif -%}
        {%- endfor -%}

        {%- if strategy.unique_key | is_list -%}
            {%- for key in strategy.unique_key -%}
                snapshotted_data.{{ key }} as dbt_unique_key_{{ loop.index }},
            {%- endfor -%}
        {%- else -%}
            snapshotted_data.dbt_unique_key as dbt_unique_key,
        {%- endif -%}

        {{ snapshot_get_time() }} as {{ columns.dbt_updated_at }},
        {{ snapshot_get_time() }} as {{ columns.dbt_valid_from }},
        snapshotted_data.{{ columns.dbt_valid_to }} as {{ columns.dbt_valid_to }},
        {{ new_scd_id }} as {{ columns.dbt_scd_id }},
        'True' as {{ columns.dbt_is_deleted }}

    from snapshotted_data
    left join deletes_source_data as source_data
      on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key

    {# Only when key is missing in source, and avoid duplicate open 'deleted' rows #}
    where source_data.dbt_unique_key is null
      and not (
        snapshotted_data.{{ columns.dbt_is_deleted }} = 'True'
        and
        {% if config.get('dbt_valid_to_current') -%}
            snapshotted_data.{{ columns.dbt_valid_to }} = ( {{ config.get('dbt_valid_to_current') }} :> datetime )
        {%- else -%}
            snapshotted_data.{{ columns.dbt_valid_to }} is null
        {%- endif %}
      )
{%- endmacro %}


{% macro singlestore__insert_select(relation, select_query) %}
    insert into {{ relation.include(database=True) }}
    {{ select_query }}
{% endmacro %}


{% macro singlestore__build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}
    {% set tmp_relation = make_temp_relation(target_relation) -%}

    {% set select_inserts = singlestore__snapshot_staging_table_insertions(strategy, sql, target_relation, columns) -%}
    {% call statement('create_snapshot_with_insertions') %}
        {{ create_table_as(True, tmp_relation, select_inserts) }}
    {% endcall %}

    {% set select_updates = singlestore__snapshot_staging_table_updates(strategy, sql, target_relation, columns) -%}
    {% call statement('insert_updates_to_snapshot') %}
        {{ singlestore__insert_select(tmp_relation, select_updates) }}
    {% endcall %}

    {%- if strategy.hard_deletes == 'invalidate' or strategy.hard_deletes == 'new_record' %}
        {% set select_deletes = singlestore__snapshot_staging_table_deletes(strategy, sql, target_relation, columns) -%}
        {% call statement('insert_deletes_to_snapshot') %}
            {{ singlestore__insert_select(tmp_relation, select_deletes) }}
        {% endcall %}
    {%- endif %}

    {%- if strategy.hard_deletes == 'new_record' %}
        {% set select_deletion_records = singlestore__snapshot_staging_table_deletion_records(strategy, sql, target_relation, columns) -%}
        {% call statement('insert_deletion_records_to_snapshot') %}
            {{ singlestore__insert_select(tmp_relation, select_deletion_records) }}
        {% endcall %}
    {%- endif %}

    {% do return(tmp_relation) %}
{% endmacro %}


{% macro singlestore__build_snapshot_table(strategy, sql) %}
    {% set columns = config.get('snapshot_table_column_names') or get_snapshot_table_column_names() %}

    select *,
        {{ strategy.scd_id }} as {{ columns.dbt_scd_id }},
        {{ strategy.updated_at }} as {{ columns.dbt_updated_at }},
        {{ strategy.updated_at }} as {{ columns.dbt_valid_from }},
        {{ singlestore__get_dbt_valid_to_current(strategy, columns) }}
      {%- if strategy.hard_deletes == 'new_record' -%}
        , 'False' as {{ columns.dbt_is_deleted }}
      {% endif -%}
    from (
        {{ sql }}
    ) sbq

{% endmacro %}


{% macro singlestore__get_dbt_valid_to_current(strategy, columns) %}
  {% set dbt_valid_to_current = config.get('dbt_valid_to_current') or "null" %}
  (coalesce(
    nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}),
    ({{ dbt_valid_to_current }}) :> datetime)
    :> datetime)
  as {{ columns.dbt_valid_to }}
{% endmacro %}


{% macro singlestore__scd_id_expr(strategy) %}
  {% if strategy.unique_key | is_list %}
    {% set joined_pk = "concat(" ~ (strategy.unique_key | join(" , '-', ")) ~ ")" %}
    {{ snapshot_hash_arguments([joined_pk, strategy.updated_at]) }}
  {% else %}
    {{ snapshot_hash_arguments([strategy.unique_key, strategy.updated_at]) }}
  {% endif %}
{% endmacro %}

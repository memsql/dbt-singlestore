{% macro singlestore__snapshot_staging_table_deletes(strategy, source_sql, target_relation) -%}
    with snapshot_query as (
        {{ source_sql }}
    ),
    snapshotted_data as (
        select *,
            {{ strategy.unique_key }} as dbt_unique_key
        from {{ target_relation }}
        where dbt_valid_to is null
    ),
    deletes_source_data as (
        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key
        from snapshot_query
    )
    select
        'delete' as dbt_change_type,
        source_data.*,
        {{ snapshot_get_time() }} as dbt_valid_from,
        {{ snapshot_get_time() }} as dbt_updated_at,
        {{ snapshot_get_time() }} as dbt_valid_to,
        snapshotted_data.dbt_scd_id

    from snapshotted_data
    left join deletes_source_data as source_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
    where source_data.dbt_unique_key is null
{%- endmacro %}


{% macro singlestore__snapshot_staging_table_updates(strategy, source_sql, target_relation) %}
    with snapshot_query as (
        {{ source_sql }}
    ),
    snapshotted_data as (
        select *, {{ strategy.unique_key }} as dbt_unique_key
          from {{ target_relation }}
         where dbt_valid_to is null
    ),
    updates_source_data as (
        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            {{ strategy.updated_at }} as dbt_valid_to
        from snapshot_query
    )
    select
        'update' as dbt_change_type,
        source_data.*,
        snapshotted_data.dbt_scd_id
    from updates_source_data as source_data
    join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
    where ({{ strategy.row_changed }})
{%- endmacro %}


{% macro singlestore__snapshot_staging_table_insertions(strategy, source_sql, target_relation) %}
    with snapshot_query as (
        {{ source_sql }}
    ),
    snapshotted_data as (
        select *, {{ strategy.unique_key }} as dbt_unique_key
          from {{ target_relation }}
         where dbt_valid_to is null
    ),
    insertions_source_data as (
        select
            *,
            {{ strategy.unique_key }} as dbt_unique_key,
            {{ strategy.updated_at }} as dbt_updated_at,
            {{ strategy.updated_at }} as dbt_valid_from,
            nullif({{ strategy.updated_at }}, {{ strategy.updated_at }}) as dbt_valid_to,
            {{ strategy.scd_id }} as dbt_scd_id
        from snapshot_query
    )
    select
        'insert' as dbt_change_type,
        source_data.*
    from insertions_source_data as source_data
    left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
    where snapshotted_data.dbt_unique_key is null
       or (
            snapshotted_data.dbt_unique_key is not null
            and ({{ strategy.row_changed }})
       )
{% endmacro %}


{% macro singlestore__insert_select(relation, select_query) %}
    insert into {{ relation.include(database=False) }}
    {{ select_query }}
{% endmacro %}


{% macro singlestore__build_snapshot_staging_table(strategy, sql, target_relation) %}
    {% set tmp_relation = make_temp_relation(target_relation) -%}

    {% set select_inserts = singlestore__snapshot_staging_table_insertions(strategy, sql, target_relation) -%}
    {% call statement('create_snapshot_with_insertions') %}
        {{ create_table_as(True, tmp_relation, select_inserts) }}
    {% endcall %}

    {% set select_updates = singlestore__snapshot_staging_table_updates(strategy, sql, target_relation) -%}
    {% call statement('insert_updates_to_snapshot') %}
        {{ singlestore__insert_select(tmp_relation, select_updates) }}
    {% endcall %}

    {%- if strategy.invalidate_hard_deletes %}
        {% set select_deletes = singlestore__snapshot_staging_table_deletes(strategy, sql, target_relation) -%}
        {% call statement('insert_deletes_to_snapshot') %}
            {{ singlestore__insert_select(tmp_relation, select_deletes) }}
        {% endcall %}
    {%- endif %}

    {% do return(tmp_relation) %}
{% endmacro %}

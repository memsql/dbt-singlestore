{% macro singlestore__get_incremental_append_sql(get_incremental_append_sql) %}
    {% do return (default__get_incremental_append_sql(get_incremental_append_sql)) %}
{% endmacro %}

{% macro singlestore__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
    {% do return (default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates)) %}
{% endmacro %}

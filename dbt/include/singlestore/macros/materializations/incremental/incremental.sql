{% macro singlestore__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
    {% do return (default__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates)) %}
{% endmacro %}

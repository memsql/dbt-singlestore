{% macro singlestore__get_show_grant_sql(relation) %}

    select privilege_type, substring(grantee, 2, length(grantee) - length(substring_index(grantee, '@', -1)) - 3) as grantee
    from information_schema.table_privileges
    where table_schema = '{{ relation.database }}'
      and table_name = '{{ relation.identifier }}'
{% endmacro %}

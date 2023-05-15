{% macro singlestore__get_show_grant_sql(relation) %}
  -- usernames are stored in the format 'username'@'host' in the column table_privileges.grantee,
  -- dbt is interested in username only, so we extract it from grantee with the assumption that
  -- hostname doesn't contain '@' (but username is allowed to contain '@')
  select privilege_type, substring(grantee, 2, length(grantee) - length(substring_index(grantee, '@', -1)) - 3) as grantee
    from information_schema.table_privileges
   where table_schema = '{{ relation.database }}'
     and table_name = '{{ relation.identifier }}'
{% endmacro %}

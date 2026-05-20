{#
Copyright 2021-2026 SingleStore, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
#}

{% macro singlestore__get_show_grant_sql(relation) %}
  -- usernames are stored in the format 'username'@'host' in the column table_privileges.grantee,
  -- dbt is interested in username only, so we extract it from grantee with the assumption that
  -- hostname doesn't contain '@' (but username is allowed to contain '@')
  select privilege_type, substring(grantee, 2, length(grantee) - length(substring_index(grantee, '@', -1)) - 3) as grantee
    from information_schema.table_privileges
   where table_schema = '{{ relation.database }}'
     and table_name = '{{ relation.identifier }}'
{% endmacro %}

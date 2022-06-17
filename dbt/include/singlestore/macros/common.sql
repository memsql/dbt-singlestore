{% macro singlestore__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        select distinct schema_name
        from information_schema.schemata
    {% endcall -%}
    {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro singlestore__create_schema(relation) -%}
    {# no-op #}
    {%- call statement('create_schema') -%}
        SELECT 'create_schema'
    {%- endcall -%}
{% endmacro %}


{% macro singlestore__check_schema_exists(database, schema) -%}
    {# no-op #}
{% endmacro %}


{% macro singlestore__truncate_relation(relation) -%}
    {% call statement('truncate_relation') -%}
        truncate table {{ relation }}
    {% endcall -%}
{% endmacro %}


{% macro singlestore__create_table_as(temporary, relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
	{%- set storage_type = config.get('storage_type', 'columnstore') -%}
    {{ sql_header if sql_header is not none }}
    create {% if storage_type == 'rowstore': -%} rowstore {%- endif %} {% if temporary: -%} temporary{%- endif %} table
        {{ relation.include(database=True) }}
    as
        {{ sql }}
{% endmacro %}


{% macro singlestore__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        show columns from {{ relation.include(database=True) }}
    {% endcall %}

    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro singlestore__list_relations_without_caching(schema_relation) %}
    {% if schema_relation.database is not none -%}
       {% set database = schema_relation.database -%}
    {% else -%}
       {% set query = 'select database()' -%}
       {% set result = run_query(query) -%}
       {% set database = result[0][0] -%}
    {% endif -%}
    {% call statement('list_relations_without_caching', fetch_result=True) -%}
        /* list_relations_without_caching for relation */
        /* database: {{ schema_relation.database }}, schema: {{ schema_relation.schema }}, name: {{ schema_relation.identifier }} */
        select
            table_schema as "database",
            table_name as "name",
            '{{ schema_relation.schema }}' as "schema",
            case when table_type = 'BASE TABLE' then 'table'
                 when table_type = 'VIEW' then 'view'
                 else table_type
            end as "table_type"
        from information_schema.tables
        where table_schema = '{{ database }}'
    {% endcall %}
    {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro singlestore__drop_schema(relation) -%}
    {%- call statement('drop_database') -%}
        drop database if exists {{ relation.database }}
    {% endcall %}
{% endmacro %}


{% macro singlestore__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        drop {{ relation.type }} if exists {{ relation.include(database=True) }}
    {%- endcall %}
{% endmacro %}


{% macro singlestore__replace_view_definition(from_relation, to_relation) -%}
    {% set query = 'show create view {}'.format(from_relation) -%}
    {% set result = run_query(query) -%}
    {% set create_query = result[0][1] -%}
    {% if create_query is none or create_query is undefined -%}
        {%- do exceptions.raise_compiler_error('Could not get view definition for {}'.format(from_relation.identifier)) -%}
    {%- endif %}
    {{ create_query|replace('`{}`'.format(from_relation.identifier), to_relation, 1) }}
{% endmacro %}


{% macro singlestore__real_relation_type(relation) -%}
    {% set query = 'show full tables from {} like \'{}\''.format(relation.database, relation.identifier) -%}
    {% set result = run_query(query) -%}
    {% if result | length -%}
        {% if result[0][1] == 'VIEW' -%}
            {{ 'view' }}
        {% elif result[0][1] == 'BASE TABLE' -%}
            {{ 'table' }}
        {% else -%}
            {%- do exceptions.raise_compiler_error('Unknown relation type for {}'.format(relation.identifier)) -%}
        {% endif -%}
    {% else -%}
        {{ '' }}
    {% endif -%}
{% endmacro -%}}


{% macro singlestore__rename_relation(from_relation, to_relation) -%}
    {#
      2-step process is needed:
      1. Drop the existing to_relation
      2. Rename from_relation to to_relation
    #}
    {% set from_type = singlestore__real_relation_type(from_relation).strip() -%}
    {% set to_type = singlestore__real_relation_type(to_relation).strip() -%}
    {% if to_type -%}
        {% call statement('drop_relation') %}
            drop {{ to_type }} if exists {{ to_relation.include(database=True) }}
        {% endcall %}
    {% endif -%}
    {% call statement('rename_relation') %}
        {% if from_type == 'table' %}
            alter table {{ from_relation }} rename to {{ to_relation }}
        {% elif from_type == 'view' %}
            {{ singlestore__replace_view_definition(from_relation, to_relation) }}
        {% else %}
            {%- do exceptions.raise_compiler_error(
                'singlestore__real_relation_type for {} returned "{}" - must be "view" or "table"'.format(
                    from_relation.identifier, from_type)) -%}
        {% endif %}
    {% endcall %}
    {% call statement('drop_relation') %}
        {% if from_type == 'view' -%}
            drop view {{ from_relation }}
        {% endif -%}
    {% endcall %}
{% endmacro %}


{% macro singlestore__current_timestamp() -%}
    current_timestamp()
{%- endmacro %}


{% macro singlestore__create_view_as(relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {{ sql_header if sql_header is not none }}
    create view {{ relation }} as
        {{ sql }}
{%- endmacro %}

{% macro singlestore__alter_column_type(relation, column_name, new_column_type) -%}
  {#
    1. Create a new column (w/ temp name and correct type)
    2. Copy data over to it
    3. Drop the existing column (cascade!)
    4. Rename the new column to existing column
  #}
  {%- set tmp_column = column_name + "__dbt_alter" -%}

  {% call statement('alter_column_type') %}
    alter table {{ relation }} add column {{ adapter.quote(tmp_column) }} {{ new_column_type }};
    update {{ relation }} set {{ adapter.quote(tmp_column) }} = {{ adapter.quote(column_name) }};
    alter table {{ relation }} drop column {{ adapter.quote(column_name) }} cascade;
    alter table {{ relation }} change {{ adapter.quote(tmp_column) }} {{ adapter.quote(column_name) }}
  {% endcall %}

{% endmacro %}

{% macro singlestore__type_string() %}
    char
{% endmacro %}

{% macro singlestore__datediff(first_date, second_date, datepart) %}

    TIMESTAMPDIFF (
        {{ datepart }},
        {{ first_date }},
        {{ second_date }}
        )

{% endmacro %}

{% macro singlestore__dateadd(datepart, interval, from_date_or_timestamp) %}

    TIMESTAMPADD (
        {{ datepart }},
        {{ interval }},
        {{ from_date_or_timestamp }}
        )

{% endmacro %}

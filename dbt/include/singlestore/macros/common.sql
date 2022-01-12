{% macro singlestore__list_schemas(database) %}
    {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}
        select distinct schema_name
        from information_schema.schemata
    {% endcall -%}
    {{ return(load_result('list_schemas').table) }}
{% endmacro %}


{% macro singlestore__create_schema(relation) -%}
    {%- call statement('create_schema') -%}
        create database if not exists {{ relation.without_identifier() }}
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
    {{ sql_header if sql_header is not none }}
    create {% if temporary: -%}rowstore temporary{%- endif %} table
        {{ relation.include(database=False) }}
    as
        {{ sql }}
{% endmacro %}


{% macro singlestore__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        show columns from {{ relation.schema }}.{{ relation.identifier }}
    {% endcall %}

    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}


{% macro singlestore__list_relations_without_caching(schema_relation) %}
    {% call statement('list_relations_without_caching', fetch_result=True) -%}
        select
            null as "database",
            table_name as name,
            table_schema as "schema",
            case when table_type = 'BASE TABLE' then 'table'
                 when table_type = 'VIEW' then 'view'
                 else table_type
            end as table_type
        from information_schema.tables
        where table_schema = '{{ schema_relation.schema }}'
    {% endcall %}
    {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro singlestore__drop_schema(relation) -%}
    {%- call statement('drop_schema') -%}
        drop database if exists {{ relation.schema }}
    {% endcall %}
{% endmacro %}


{% macro singlestore__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}
        drop {{ relation.type }} if exists {{ relation.schema }}.{{ relation.name }}
    {%- endcall %}
{% endmacro %}


{% macro replace_view_definition(from_relation, to_relation) -%}
    {% set query = 'show create view {}'.format(from_relation) -%}
    {% set result = run_query(query) -%}
    {% set create_query = result[0][1] -%}
    {% if create_query is none or create_query is undefined -%}
        {%- do exceptions.raise_compiler_error('Could not get view definition for {}'.format(from_relation.identifier)) -%}
    {%- endif %}
    {{ create_query|replace(from_relation.identifier, to_relation.identifier, 1) }}
{% endmacro %}


{% macro real_relation_type(relation) -%}
    {% set query = 'show full tables like \'{}\''.format(relation.identifier) -%}
    {% set result = run_query(query) -%}
    {% if result|length -%}
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
    {% set from_type = real_relation_type(from_relation).strip() -%}
    {% set to_type = real_relation_type(to_relation).strip() -%}
    {% if to_type -%}
        {% call statement('drop_relation') %}
            drop {{ to_type }} if exists {{ to_relation.identifier }}
        {% endcall %}
    {% endif -%}
    {% call statement('rename_relation') %}
        {% if from_type == 'table' %}
            alter table {{ from_relation }} rename to {{ to_relation }}
        {% elif from_type == 'view' %}
            {{ replace_view_definition(from_relation, to_relation) }}
        {% else %}
            {%- do exceptions.raise_compiler_error('Unknown relation type for {}: {}'.format(from_relation.identifier), from_type) -%}
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


{% macro singlestore__generate_database_name(custom_database_name=none, node=none) -%}
    {% do return(None) %}
{%- endmacro %}


{% macro singlestore__create_view_as(relation, sql) -%}
    {%- set sql_header = config.get('sql_header', none) -%}
    {{ sql_header if sql_header is not none }}
    create view {{ relation }} as
        {{ sql }}
{%- endmacro %}
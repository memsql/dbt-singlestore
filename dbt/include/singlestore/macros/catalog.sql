{% macro singlestore__get_catalog(information_schema, schemas) -%}
    {% set database = information_schema.database %}
    {%- call statement('catalog', fetch_result=True) -%}
    select
        columns.table_database,
        columns.table_schema,
        columns.table_name,
        tables.table_type,
        nullif(columns.table_comment, ''),
        tables.table_owner,
        columns.column_name,
        columns.column_index,
        columns.column_type,
        nullif(columns.column_comment, '')
    from
        ({{singlestore__get_catalog_tables_sql(information_schema)}}) as tables
    join
        ({{singlestore__get_catalog_columns_sql(information_schema)}}) as columns
    using (table_database, table_name)
    where table_database = '{{ database }}'

    order by column_index
    {%- endcall -%}

    {{ return(load_result('catalog').table) }}

{%- endmacro %}


{% macro singlestore__get_catalog_relations(information_schema, relations) -%}
    {% set database = information_schema.database %}
    {%- call statement('catalog', fetch_result=True) -%}
    select
        columns.table_database,
        columns.table_schema,
        columns.table_name,
        tables.table_type,
        nullif(columns.table_comment, ''),
        tables.table_owner,
        columns.column_name,
        columns.column_index,
        columns.column_type,
        nullif(columns.column_comment, '')
    from
        ({{singlestore__get_catalog_tables_sql(information_schema)}}
        {{ singlestore__get_catalog_relations_where_clause_sql(relations) }}) as tables
    join
        ({{singlestore__get_catalog_columns_sql(information_schema)}}
        {{ singlestore__get_catalog_relations_where_clause_sql(relations) }}) as columns
    using (table_database, table_name)
    where table_database = '{{ database }}'

    order by column_index
    {%- endcall -%}

    {{ return(load_result('catalog').table) }}

{%- endmacro %}


{% macro singlestore__get_catalog_tables_sql(information_schema) -%}
    select
            table_schema as "table_database",
            '{{ target.schema }}' as "table_schema",
            table_name as "table_name",
            case when table_type = 'BASE TABLE' then 'table'
                 when table_type = 'VIEW' then 'view'
                 else table_type
            end as "table_type",
            null as "table_owner"

        from information_schema.tables
{%- endmacro %}


{% macro singlestore__get_catalog_columns_sql(information_schema) -%}
    select
            table_schema as "table_database",
            '{{ target.schema }}' as "table_schema",
            table_name as "table_name",
            null as "table_comment",

            column_name as "column_name",
            ordinal_position as "column_index",
            data_type as "column_type",
            nullif(column_comment, '') as "column_comment"

        from information_schema.columns
{%- endmacro %}


{% macro singlestore__catalog_equals(field, value) %}
    UPPER({{ field }}) = UPPER('{{ value }}')
{% endmacro %}

{% macro singlestore__get_catalog_relations_where_clause_sql(relations) -%}
    where (
        {%- for relation in relations -%}
            {% if relation.identifier %}
                (
                    {{ singlestore__catalog_equals('table_name', relation.identifier) }}
                )
            {% else %}
                {% do exceptions.raise_compiler_error(
                    '`get_catalog_relations` requires a list of relations, each with an identifier'
                ) %}
            {% endif %}

            {%- if not loop.last %} or {% endif -%}
        {%- endfor -%}
    )
{%- endmacro %}


{% macro singlestore__get_catalog(information_schema, schemas) -%}
    {% set database = information_schema.database %}
    {%- call statement('catalog', fetch_result=True) -%}
    select
        columns.table_database,
        columns.table_schema,
        columns.table_name,
        tables.table_type,
        columns.table_comment,
        tables.table_owner,
        columns.column_name,
        columns.column_index,
        columns.column_type,
        columns.column_comment
    from
    (
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
    )
    as tables
    join
    (
        select
            table_schema as "table_database",
            '{{ target.schema }}' as "table_schema",
            table_name as "table_name",
            null as "table_comment",

            column_name as "column_name",
            ordinal_position as "column_index",
            data_type as "column_type",
            null as "column_comment"

        from information_schema.columns
    )
    as columns using (table_database, table_name)
    where table_database = '{{ database }}'

    order by column_index
    {%- endcall -%}

    {{ return(load_result('catalog').table) }}

{%- endmacro %}

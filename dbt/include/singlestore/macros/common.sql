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
    {%- set primary_key = config.get('primary_key', []) -%} {# PRIMARY KEY (primary_key) #}
    {%- set sort_key = config.get('sort_key', []) -%} {# SORT KEY (sort_key) #}
    {%- set shard_key = config.get('shard_key', []) -%} {# SHARD KEY (shard_key) #}
    {%- set unique_table_key = config.get('unique_table_key', []) -%} {# UNIQUE KEY (unique_table_key) #}
    {%- set charset = config.get('charset', none) -%} {# CHARACTER SET charset #}
    {%- set collation = config.get('collation', none) -%} {# COLLATE collation #}

    {%- set create_definition_list = [] %}
    {% if primary_key | length -%}
        {% set quoted = [] -%}
            {%- for col in primary_key -%}
                {%- do quoted.append(adapter.quote(col)) -%}
            {%- endfor %}
        {% do create_definition_list.append('PRIMARY KEY ({})'.format(", ".join(quoted))) -%}
    {% endif -%}
    {% if sort_key | length -%}
        {% set quoted = [] -%}
            {%- for col in sort_key -%}
                {%- do quoted.append(adapter.quote(col)) -%}
            {%- endfor %}
        {% do create_definition_list.append('SORT KEY ({})'.format(", ".join(quoted))) -%}
    {% endif -%}
    {% if shard_key | length -%}
        {% set quoted = [] -%}
            {%- for col in shard_key -%}
                {%- do quoted.append(adapter.quote(col)) -%}
            {%- endfor %}
        {% do create_definition_list.append('SHARD KEY ({})'.format(", ".join(quoted))) -%}
    {% elif unique_table_key | length -%}
        {% set quoted = [] -%}
            {%- for col in unique_table_key -%}
                {%- do quoted.append(adapter.quote(col)) -%}
            {%- endfor %}
        {% do create_definition_list.append('SHARD KEY ({})'.format(", ".join(quoted))) -%}
    {% endif -%}
    {% if unique_table_key | length -%}
        {% set quoted = [] -%}
            {%- for col in unique_table_key -%}
                {%- do quoted.append(adapter.quote(col)) -%}
            {%- endfor %}
        {% do create_definition_list.append('UNIQUE KEY ({})'.format(", ".join(quoted))) -%}
    {% endif -%}

    {% if create_definition_list | length -%}
        {% set create_definition_str = '(' + create_definition_list|join(", ") + ')' -%}
    {% else -%}
        {% set create_definition_str = '(SHARD KEY ())' -%}
    {% endif -%}

    {%- set charset_definition_str = ' ' %}
    {% if charset is not none -%}
        {% set charset_definition_str = charset_definition_str + 'CHARACTER SET ' + charset + ' ' -%}
    {% endif -%}
    {% if collation is not none -%}
        {% set charset_definition_str = charset_definition_str + 'COLLATE ' + collation + ' ' -%}
    {% endif -%}

    {{ sql_header if sql_header is not none }}

    {% if temporary -%}
        {% set storage_type = 'rowstore temporary' -%}
    {% elif config.get('storage_type') == 'rowstore' -%}
        {% set storage_type = 'rowstore' -%}
    {% else -%}
        {% set storage_type = '' -%}
    {% endif -%}

    create {{ storage_type }} table
        {{ relation.include(database=True) }} {{create_definition_str }} {{ charset_definition_str }}
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
{% endmacro -%}


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


{% macro singlestore__get_create_index_sql(relation, index_dict) -%}
    {%- set index_config = adapter.parse_index(index_dict) -%}

    {%- set quoted = [] -%}
    {% for col in index_config.columns -%}
        {% do quoted.append(adapter.quote(col)) -%}
    {% endfor -%}
    {%- set comma_separated_columns = ", ".join(quoted) -%}

    {%- set index_name = index_config.render(relation) -%}

    create {% if index_config.unique -%} unique {%- endif %}
    index {{ index_name }}
    on {{ relation }} ({{ comma_separated_columns }})
    {% if index_config.type -%}
        using {{ index_config.type }}
    {%- endif %}
{%- endmacro %}

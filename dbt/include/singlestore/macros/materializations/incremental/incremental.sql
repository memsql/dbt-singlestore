{% macro singlestore__get_incremental_append_sql(get_incremental_append_sql) %}
    {% do return (default__get_incremental_append_sql(get_incremental_append_sql)) %}
{% endmacro %}

{% macro singlestore__get_delete_insert_merge_sql(target, source, unique_key, dest_columns, incremental_predicates) %}
    /*
        The default dbt implementation uses syntax not compatible with SingleStore. We adjusted the `DELETE` statement:

            delete from {{ target }}
                using {{ source }}

        to the following syntax required by SingleStore:

            delete {{ target.identifier }} from {{ target }}
                join {{ source }}
    */
    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    {% if unique_key %}
        {% if unique_key is sequence and unique_key is not string %}
            delete {{ target.identifier }} from {{ target }}
            join {{ source }}
            where (
                {% for key in unique_key %}
                    {{ source }}.{{ key }} = {{ target }}.{{ key }}
                    {{ "and " if not loop.last}}
                {% endfor %}
                {% if incremental_predicates %}
                    {% for predicate in incremental_predicates %}
                        and {{ predicate }}
                    {% endfor %}
                {% endif %}
            );
        {% else %}
            delete from {{ target }}
            where (
                {{ unique_key }}) in (
                select ({{ unique_key }})
                from {{ source }}
            )
            {%- if incremental_predicates %}
                {% for predicate in incremental_predicates %}
                    and {{ predicate }}
                {% endfor %}
            {%- endif -%};

        {% endif %}
    {% endif %}

    insert into {{ target }} ({{ dest_cols_csv }})
    (
        select {{ dest_cols_csv }}
        from {{ source }}
    )

{% endmacro %}

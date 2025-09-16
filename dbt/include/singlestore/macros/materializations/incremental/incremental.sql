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


{% macro singlestore__get_incremental_microbatch_sql(arg_dict) %}
    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = [] if arg_dict.get('incremental_predicates') is none else arg_dict.get('incremental_predicates') -%}

    {# Append microbatch window predicates for SingleStore #}
    {% if model.config.get("__dbt_internal_microbatch_event_time_start") -%}
      {% do incremental_predicates.append(
        model.config.event_time ~ " >= CAST('" ~ model.config.__dbt_internal_microbatch_event_time_start ~ "' AS DATETIME)"
      ) %}
    {% endif %}
    {% if model.config.__dbt_internal_microbatch_event_time_end -%}
      {% do incremental_predicates.append(
        model.config.event_time ~ " < CAST('" ~ model.config.__dbt_internal_microbatch_event_time_end ~ "' AS DATETIME)"
      ) %}
    {% endif %}
    {% do arg_dict.update({'incremental_predicates': incremental_predicates}) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    START TRANSACTION;

      {# 1. Delete just the microbatch slice from target #}
      delete from {{ target }}
        where
        {%- for predicate in incremental_predicates %}
                {%- if not loop.first %}and {% endif -%} {{ predicate }}
        {%- endfor %};

      {# 2. Insert the microbatch rows #}
      insert into {{ target }} ({{ dest_cols_csv }})
      (
        select {{ dest_cols_csv }}
        from {{ source }};
      )

    COMMIT;
{% endmacro %}

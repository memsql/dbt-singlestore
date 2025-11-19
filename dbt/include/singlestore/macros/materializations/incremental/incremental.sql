{% macro singlestore__get_incremental_append_sql(arg_dict) %}
    {{ default__get_incremental_append_sql(arg_dict) }}
{% endmacro %}


{% macro singlestore__validate_unique_key_columns(unique_key, dest_columns) %}
    {%- set dest_col_names = dest_columns | map(attribute='name') | list -%}

    {%- if unique_key %}
        {# normalize unique_key to a list #}
        {%- if unique_key is sequence and unique_key is not string -%}
            {%- set keys = unique_key -%}
        {%- else -%}
            {%- set keys = [unique_key] -%}
        {%- endif -%}

        {%- for k in keys %}
            {%- if k not in dest_col_names %}
                {{ exceptions.raise_compiler_error(
                    "Incremental model unique_key '" ~ k ~ "' not found in model columns: "
                    ~ dest_col_names | join(', ')
                ) }}
            {%- endif %}
        {%- endfor %}
    {%- endif %}
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


{# --- helper: ISO-ish -> 'YYYY-MM-DD HH:MM:SS' (naive) --- #}
{% macro singlestore__to_naive_datetime(dt_str) -%}
  {%- if dt_str is none -%}{%- do return('') -%}{%- endif -%}
  {%- set s = dt_str|string -%}
  {# take the first 19 chars (YYYY-MM-DDTHH:MM:SS), then replace T with space #}
  {%- set base = s[0:19] -%}
  {{ base.replace('T', ' ') }}
{%- endmacro %}


{% macro singlestore__get_incremental_microbatch_sql(arg_dict) %}
    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = [] if arg_dict.get('incremental_predicates') is none else arg_dict.get('incremental_predicates') -%}

    {%- if incremental_predicates is string -%}
      {%- set incremental_predicates = [incremental_predicates] -%}
    {%- endif -%}

    {# Append microbatch window predicates for SingleStore #}
    {% if model.config.get("__dbt_internal_microbatch_event_time_start") -%}
    {% set _start = singlestore__to_naive_datetime(model.config.__dbt_internal_microbatch_event_time_start) %}
      {% do incremental_predicates.append(
        model.config.event_time ~ " >= ('" ~ _start ~ "' :> TIMESTAMP)"
      ) %}
    {% endif %}
    {% if model.config.__dbt_internal_microbatch_event_time_end -%}
    {% set _end = singlestore__to_naive_datetime(model.config.__dbt_internal_microbatch_event_time_end) %}
      {% do incremental_predicates.append(
        model.config.event_time ~ " < ('" ~ _end ~ "' :> TIMESTAMP)"
      ) %}
    {% endif %}
    {% do arg_dict.update({'incremental_predicates': incremental_predicates}) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    START TRANSACTION;

      {# 1. Delete just the microbatch slice from target #}
      {% if incremental_predicates | length > 0 %}
        delete from {{ target }} where (
            {%- for predicate in incremental_predicates %}
                    {%- if not loop.first %} and {% endif -%} {{ predicate }}
            {%- endfor %}
        );
      {% else %}
        /* No microbatch predicates -> skip delete */
      {% endif %}

      {# 2. Insert the microbatch rows #}
      insert into {{ target }} ({{ dest_cols_csv }})
        select {{ dest_cols_csv }}
        from {{ source }}

    COMMIT;
{% endmacro %}


{% macro singlestore__get_incremental_default_sql(arg_dict) %}
  {% if arg_dict["unique_key"] %}
    {{ singlestore__validate_unique_key_columns(arg_dict["unique_key"], arg_dict["dest_columns"]) }}
    {{ default__get_incremental_delete_insert_sql(arg_dict) }}
  {% else %}
    {{ default__get_incremental_append_sql(arg_dict) }}
  {% endif %}
{% endmacro %}

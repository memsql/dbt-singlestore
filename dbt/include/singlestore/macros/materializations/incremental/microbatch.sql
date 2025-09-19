{% macro singlestore__get_incremental_microbatch_sql(arg_dict) %}
    {% do exceptions.warn('USING singlestore__get_incremental_microbatch_sql') %}  {# tripwire for pytest logs #}
    {%- set target = arg_dict["target_relation"] -%}
    {%- set source = arg_dict["temp_relation"] -%}
    {%- set dest_columns = arg_dict["dest_columns"] -%}
    {%- set incremental_predicates = [] if arg_dict.get('incremental_predicates') is none else arg_dict.get('incremental_predicates') -%}

    CREATE TABLE LOLA_SOSKA;

    {# Append microbatch window predicates for SingleStore #}
    {% if model.config.get("__dbt_internal_microbatch_event_time_start") -%}
      {% do incremental_predicates.append(
        model.config.event_time ~ " >= ('" ~ model.config.__dbt_internal_microbatch_event_time_start ~ "' :> DATETIME)"
      ) %}
    {% endif %}
    {% if model.config.__dbt_internal_microbatch_event_time_end -%}
      {% do incremental_predicates.append(
        model.config.event_time ~ " < ('" ~ model.config.__dbt_internal_microbatch_event_time_end ~ "' :> DATETIME)"
      ) %}
    {% endif %}
    {% do arg_dict.update({'incremental_predicates': incremental_predicates}) %}

    {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}

    CREATE TABLE IF NOT EXISTS {{ target }} LIKE {{ source }};

    -- DEBUG microbatch: start={{ model.config.get("__dbt_internal_microbatch_event_time_start") }} end={{ model.config.get("__dbt_internal_microbatch_event_time_end") }} field={{ model.config.event_time }}
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
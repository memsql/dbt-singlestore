{% macro singlestore__get_replace_sql(existing_relation, target_relation, sql) %}
  {% if existing_relation is not none and target_relation.is_view %}
    {% set full = target_relation.database ~ "." ~ target_relation.identifier %}
    {% call statement('alter_view') %}
      ALTER VIEW {{ full }}
      AS
      {{ sql | trim }};
    {% endcall %}
  {% else %}
    {{ adapter.dispatch('get_replace_sql','dbt')
       (existing_relation, target_relation, sql) }}
  {% endif %}
{% endmacro %}

{% macro singlestore__get_replace_view_sql(relation, sql) %}
  {{ log("⏳ Replacing view via ALTER VIEW instead of backup-swap", info=True) }}
  {% set full_name = relation.database ~ '.' ~ relation.identifier %}
  {%- call statement('alter_view') -%}
    ALTER VIEW {{ full_name }}
    AS
    {{ sql | trim }};
  {%- endcall -%}
{% endmacro %}
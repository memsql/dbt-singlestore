{% macro singlestore__safe_cast(field, type) %}

    ({{ field }} !:> {{ singlestore__translate_safe_cast_type(type) }})

{% endmacro %}


{% macro singlestore__translate_safe_cast_type(type) -%}
    {#-
        Translate Postgres-flavored type names that arrive from dbt-core
        (notably from `Column.data_type` via `string_type`/`numeric_type`)
        into SingleStore-native equivalents that the `!:>` cast operator can
        parse.

        Background:
          dbt-core's unit-test fixture rendering passes
          `safe_cast(value, column.data_type)` for every value, and the
          base `Column.data_type` normalises any varchar/text column to the
          Postgres-style "character varying(N)". dbt-core 1.11+ additionally
          strips the length when the value is a string (dbt-labs/dbt-core
          GH-11974, to prevent silent truncation), producing a bare
          "character varying" form. SingleStore rejects both forms in `!:>`,
          and a bare `!:> char` silently truncates to a single character —
          so the unbounded cast is mapped to `longtext` (TEXT family) which
          preserves the full value.

        Translation table:
          character varying(N) -> varchar(N)
          character varying    -> longtext        (unbounded; no truncation)
          character(N)         -> char(N)
          character            -> longtext        (unbounded; no truncation)
          everything else      -> pass-through    (numeric / json / temporal
                                                   types are already valid)
    -#}
    {%- set raw = (type or '') | string | trim -%}
    {%- set lowered = raw | lower -%}
    {%- if lowered == 'character varying' or lowered == 'character' -%}
        longtext
    {%- elif lowered.startswith('character varying(') -%}
        {{- raw | replace('character varying', 'varchar') | replace('CHARACTER VARYING', 'varchar') -}}
    {%- elif lowered.startswith('character(') -%}
        {{- raw | replace('character', 'char') | replace('CHARACTER', 'char') -}}
    {%- else -%}
        {{- raw -}}
    {%- endif -%}
{%- endmacro %}

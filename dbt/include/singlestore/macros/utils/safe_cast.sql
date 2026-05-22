{#
Copyright 2021-2026 SingleStore, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
#}

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
    {#- Replacements operate on `lowered`, not `raw`: the branch selection is
        case-insensitive (via `startswith`/`==` on `lowered`), so any mixed-case
        input (e.g. `Character Varying(10)`) reaches the right branch. Replacing
        on `raw` with hard-coded all-lower / all-upper literals would miss those
        mixed-case forms and let the Postgres-flavored type leak through to the
        `!:>` cast unchanged. SingleStore type names are case-insensitive, so
        normalising to lower-case in the output is harmless. -#}
    {%- if lowered == 'character varying' or lowered == 'character' -%}
        longtext
    {%- elif lowered.startswith('character varying(') -%}
        {{- lowered | replace('character varying', 'varchar') -}}
    {%- elif lowered.startswith('character(') -%}
        {{- lowered | replace('character', 'char') -}}
    {%- else -%}
        {{- raw -}}
    {%- endif -%}
{%- endmacro %}

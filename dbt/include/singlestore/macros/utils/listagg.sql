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

{% macro singlestore__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}

    {% if limit_num -%}
    substring_index(
        group_concat(
            {{ measure }}
            {%- if order_by_clause %}
                {{ order_by_clause }}
            {%- endif %}
            SEPARATOR {{ delimiter_text }}
            ),
        {{ delimiter_text }},
        {{ limit_num }}
    )
    {%- else %}
    group_concat(
        {{ measure }}
        {%- if order_by_clause %}
            {{ order_by_clause }}
        {%- endif %}
        SEPARATOR {{ delimiter_text }}
    )
    {%- endif %}

{%- endmacro %}

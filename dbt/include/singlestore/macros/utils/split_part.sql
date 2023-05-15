{% macro singlestore__split_part(string_text, delimiter_text, part_number) %}

    if(
        length(split(
            {{ string_text }},
            {{ delimiter_text }}
            ) :> ARRAY(TEXT)) >= {{ part_number }},
        (split(
            {{ string_text }},
            {{ delimiter_text }}
            ) :> ARRAY(TEXT))[{{ part_number - 1 }}],
        NULL)

{% endmacro %}

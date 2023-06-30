{% macro singlestore__current_timestamp() -%}
    CURRENT_TIMESTAMP()
{%- endmacro %}


{%- macro singlestore__current_timestamp_in_utc_backcompat() -%}
    UTC_TIMESTAMP()
{%- endmacro -%}
    
    
{% macro singlestore__snapshot_string_as_time(timestamp) -%}
    {%- set result = "'" ~ timestamp ~ "' :> datetime" -%}
    {{ return(result) }}
{%- endmacro %}

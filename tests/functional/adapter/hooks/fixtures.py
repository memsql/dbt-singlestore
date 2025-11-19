macros__before_and_after = """
{% macro custom_run_hook(state, target, run_started_at, invocation_id) %}

   insert into on_run_hook (
        test_state,
        target_dbname,
        target_host,
        target_name,
        target_schema,
        target_type,
        target_user,
        target_pass,
        target_threads,
        run_started_at,
        invocation_id,
        thread_id
   ) VALUES (
    '{{ state }}',
    '{{ target.database }}',
    '{{ target.host }}',
    '{{ target.name }}',
    '{{ target.schema }}',
    '{{ target.type }}',
    '{{ target.user }}',
    '{{ target.get("pass", "") }}',
    {{ target.threads }},
    '{{ run_started_at }}',
    '{{ invocation_id }}',
    '{{ thread_id }}'
   )

{% endmacro %}
"""

models__hooks_configured = """
{{
    config({
        "pre_hook": "\
            insert into on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'start',\
                '{{ target.database }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
        )",
        "post-hook": "\
            insert into on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id

            ) VALUES (\
                'end',\
                '{{ target.database }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
            )"
    })
}}

select 3 as id
"""

models__hooks_error = """
{{
    config({
        "pre_hook": "\
            insert into on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'start',\
                '{{ target.database }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'
        )",
        "pre-hook": "\
            insert into on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'start',\
                '{{ target.database }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'
        )",
        "post-hook": "\
            insert into on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'end',\
                '{{ target.database }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
            )"
    })
}}

select 3 as id
"""

models__hooks_kwargs = """
{{
    config(
        pre_hook="\
            insert into on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id
            ) VALUES (\
                'start',\
                '{{ target.database }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
        )",
        post_hook="\
            insert into on_model_hook (\
                test_state,\
                target_dbname,\
                target_host,\
                target_name,\
                target_schema,\
                target_type,\
                target_user,\
                target_pass,\
                target_threads,\
                run_started_at,\
                invocation_id,\
                thread_id\
            ) VALUES (\
                'end',\
                '{{ target.database }}',\
                '{{ target.host }}',\
                '{{ target.name }}',\
                '{{ target.schema }}',\
                '{{ target.type }}',\
                '{{ target.user }}',\
                '{{ target.get(\\"pass\\", \\"\\") }}',\
                {{ target.threads }},\
                '{{ run_started_at }}',\
                '{{ invocation_id }}',\
                '{{ thread_id }}'\
            )"
    )
}}

select 3 as id
"""

models__hooked = """
{{
    config({
        "pre_hook": "\
            insert into on_model_hook select
                test_state,
                '{{ target.database }}' as target_dbname,\
                '{{ target.host }}' as target_host,\
                '{{ target.name }}' as target_name,\
                '{{ target.schema }}' as target_schema,\
                '{{ target.type }}' as target_type,\
                '{{ target.user }}' as target_user,\
                '{{ target.get(\\"pass\\", \\"\\") }}' as target_pass,\
                {{ target.threads }} as target_threads,\
                '{{ run_started_at }}' as run_started_at,\
                '{{ invocation_id }}' as invocation_id,\
                '{{ thread_id }}' as thread_id
                from {{ ref('pre') }}\
                "
    })
}}
select 1 as id
"""

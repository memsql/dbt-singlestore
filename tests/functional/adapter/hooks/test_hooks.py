from pathlib import Path
import os
import pytest
from dbt.tests.adapter.hooks.test_model_hooks import (
    BasePrePostModelHooks,
    BaseHookRefs,
    BasePrePostModelHooksOnSeeds,
    BaseHooksRefsOnSeeds,
    BasePrePostModelHooksOnSeedsPlusPrefixed,
    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
    BasePrePostModelHooksOnSnapshots,
    BasePrePostModelHooksInConfig,
    BasePrePostModelHooksInConfigWithCount,
    BasePrePostModelHooksInConfigKwargs,
    BasePrePostSnapshotHooksInConfigKwargs,
    BaseDuplicateHooksInConfigs,
)
from dbt.tests.adapter.hooks.test_run_hooks import (
    BasePrePostRunHooks,
    BaseAfterRunHooks,
)
from fixtures import (
    macros__before_and_after,
    models__hooks_configured,
    models__hooks_kwargs,
    models__hooks_error,
    models__hooked,
)
from dbt.tests.adapter.hooks import fixtures


MODEL_PRE_HOOK = """
   insert into on_model_hook (
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
    'start',
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
"""

MODEL_POST_HOOK = """
   insert into on_model_hook (
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
    'end',
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
"""


class SingleStoreBaseTestPrePost:
    """
    Adapter override of BaseTestPrePost behavior:
    - MySQL-style quoting (`\`` instead of `"`)
    - no schema qualifier on on_model_hook
    - different expectations for target fields (dbname, threads, type, etc.)
    """

    def get_ctx_vars(self, state, count, project):
        fields = [
            "test_state",
            "target_dbname",
            "target_host",
            "target_name",
            "target_schema",
            "target_threads",
            "target_type",
            "target_user",
            "target_pass",
            "run_started_at",
            "invocation_id",
            "thread_id",
        ]

        field_list = ", ".join([f"`{f}`" for f in fields])
        query = f"select {field_list} from on_model_hook where test_state = '{state}'"

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into hooks table"
        assert len(vals) >= count, "too few rows in hooks table"
        assert len(vals) <= count, "too many rows in hooks table"
        return [{k: v for k, v in zip(fields, val)} for val in vals]

    def check_hooks(self, state, project, host, count=1):
        ctxs = self.get_ctx_vars(state, count=count, project=project)
        for ctx in ctxs:
            assert ctx["test_state"] == state
            assert ctx["target_dbname"] == "dbt_test"
            assert ctx["target_host"] == host
            assert ctx["target_name"] == "default"
            assert ctx["target_schema"] == project.test_schema
            assert ctx["target_threads"] == 1
            assert ctx["target_type"] == "singlestore"
            assert ctx["target_user"] == "root"
            assert ctx["target_pass"] == ""

            assert (
                ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
            ), "run_started_at was not set"
            assert (
                ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
            ), "invocation_id was not set"
            assert ctx["thread_id"].startswith("Thread-")


class TestPrePostModelHooks(SingleStoreBaseTestPrePost, BasePrePostModelHooks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "select 1 as noop", "transaction": False},
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {"sql": "select 1 as noop", "transaction": False},
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }
    pass


class TestPrePostModelHooksUnderscores(SingleStoreBaseTestPrePost, BasePrePostModelHooks):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre_hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "select 1 as noop", "transaction": False},
                    ],
                    "post_hook": [
                        # outside transaction (runs second)
                        {"sql": "select 1 as noop", "transaction": False},
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }


class TestHookRefs(SingleStoreBaseTestPrePost, BaseHookRefs):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "hooked.sql": models__hooked,
            "post.sql": fixtures.models__post,
            "pre.sql": fixtures.models__pre,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "hooked": {
                        "post-hook": [
                            """
                        insert into on_model_hook select
                        test_state,
                        '{{ target.database }}' as target_dbname,
                        '{{ target.host }}' as target_host,
                        '{{ target.name }}' as target_name,
                        '{{ target.schema }}' as target_schema,
                        '{{ target.type }}' as target_type,
                        '{{ target.user }}' as target_user,
                        '{{ target.get(pass, "") }}' as target_pass,
                        {{ target.threads }} as target_threads,
                        '{{ run_started_at }}' as run_started_at,
                        '{{ invocation_id }}' as invocation_id,
                        '{{ thread_id }}' as thread_id
                        from {{ ref('post') }}""".strip()
                        ],
                    }
                },
            }
        }
    pass


class TestPrePostModelHooksOnSeeds(BasePrePostModelHooksOnSeeds):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "seed-paths": ["seeds"],
            "models": {},
            "seeds": {
                "post-hook": [
                    "alter table {{ this }} add column new_col int",
                    "update {{ this }} set new_col = 1",
                    # call any macro to track dependency: https://github.com/dbt-labs/dbt-core/issues/6806
                    "select (null :> {{ dbt.type_int() }}) as id",
                ],
                "quote_columns": False,
            },
        }
    pass


class TestHooksRefsOnSeeds(BaseHooksRefsOnSeeds):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixed(BasePrePostModelHooksOnSeedsPlusPrefixed):
    pass


class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace(BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace):
    pass


class TestPrePostModelHooksOnSnapshots(BasePrePostModelHooksOnSnapshots):
    pass


class TestPrePostModelHooksInConfig(SingleStoreBaseTestPrePost, BasePrePostModelHooksInConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_configured}
    pass


class TestPrePostModelHooksInConfigWithCount(SingleStoreBaseTestPrePost, BasePrePostModelHooksInConfigWithCount):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_configured}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "pre-hook": [
                        # inside transaction (runs second)
                        MODEL_PRE_HOOK,
                        # outside transaction (runs first)
                        {"sql": "select 1 as noop", "transaction": False},
                    ],
                    "post-hook": [
                        # outside transaction (runs second)
                        {"sql": "select 1 as noop", "transaction": False},
                        # inside transaction (runs first)
                        MODEL_POST_HOOK,
                    ],
                }
            }
        }
    pass


class TestPrePostModelHooksInConfigKwargs(SingleStoreBaseTestPrePost, BasePrePostModelHooksInConfigKwargs):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_kwargs}
    pass


class TestPrePostSnapshotHooksInConfigKwargs(BasePrePostSnapshotHooksInConfigKwargs):
    pass


class TestDuplicateHooksInConfigs(BaseDuplicateHooksInConfigs):
    @pytest.fixture(scope="class")
    def models(self):
        return {"hooks.sql": models__hooks_error}
    pass


class TestPrePostRunHooks(BasePrePostRunHooks):
    @pytest.fixture(scope="function")
    def setUp(self, project):
        project.run_sql_file(project.test_data_dir / Path("seed_run.sql"))
        project.run_sql(f"drop table if exists used_schemas")
        project.run_sql(f"drop table if exists db_schemas")
        os.environ["TERM_TEST"] = "TESTING"
    
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "hook.sql": fixtures.macros__hook,
            "before-and-after.sql": macros__before_and_after,
        }
    
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            # The create and drop table statements here validate that these hooks run
            # in the same order that they are defined. Drop before create is an error.
            # Also check that the table does not exist below.
            "on-run-start": [
                "{{ custom_run_hook('start', target, run_started_at, invocation_id) }}",
                "create table start_hook_order_test ( id int )",
                "drop table start_hook_order_test",
                "{{ log(env_var('TERM_TEST'), info=True) }}",
            ],
            "on-run-end": [
                "{{ custom_run_hook('end', target, run_started_at, invocation_id) }}",
                "create table end_hook_order_test ( id int )",
                "drop table end_hook_order_test",
                "create table used_schemas ( used_schema text )",
                "insert into used_schemas (used_schema) values {% for schema in schemas %}( '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
                "create table db_schemas ( db text, used_schema text )",
                "insert into db_schemas (db, used_schema) values {% for db, schema in database_schemas %}('{{ db }}', '{{ schema }}' ){% if not loop.last %},{% endif %}{% endfor %}",
            ],
            "seeds": {
                "quote_columns": False,
            },
        }

    def get_ctx_vars(self, state, project):
        fields = [
            "test_state",
            "target_dbname",
            "target_host",
            "target_name",
            "target_schema",
            "target_threads",
            "target_type",
            "target_user",
            "target_pass",
            "run_started_at",
            "invocation_id",
            "thread_id",
        ]
        field_list = ", ".join([f"`{f}`" for f in fields])
        query = f"select {field_list} from on_run_hook where test_state = '{state}'"

        vals = project.run_sql(query, fetch="all")
        assert len(vals) != 0, "nothing inserted into on_run_hook table"
        assert len(vals) == 1, "too many rows in hooks table"
        ctx = dict([(k, v) for (k, v) in zip(fields, vals[0])])

        return ctx

    def assert_used_schemas(self, project):
        schemas_query = "select * from used_schemas"
        results = project.run_sql(schemas_query, fetch="all")
        assert len(results) == 1
        assert results[0][0] == project.test_schema

        db_schemas_query = "select * from db_schemas"
        results = project.run_sql(db_schemas_query, fetch="all")
        assert len(results) == 1
        assert results[0][0] == project.database
        assert results[0][1] == project.test_schema

    def check_hooks(self, state, project, host):
        ctx = self.get_ctx_vars(state, project)

        assert ctx["test_state"] == state
        assert ctx["target_dbname"] == "dbt_test"
        assert ctx["target_host"] == host
        assert ctx["target_name"] == "default"
        assert ctx["target_schema"] == project.test_schema
        assert ctx["target_threads"] == 1
        assert ctx["target_type"] == "singlestore"
        assert ctx["target_user"] == "root"
        assert ctx["target_pass"] == ""

        assert (
            ctx["run_started_at"] is not None and len(ctx["run_started_at"]) > 0
        ), "run_started_at was not set"
        assert (
            ctx["invocation_id"] is not None and len(ctx["invocation_id"]) > 0
        ), "invocation_id was not set"
        assert ctx["thread_id"].startswith("Thread-") or ctx["thread_id"] == "MainThread"
    pass


class TestAfterRunHooks(BaseAfterRunHooks):
    pass

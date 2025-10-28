import pytest
from dbt.tests.adapter.hooks.test_model_hooks import (
    BasePrePostModelHooks,
    BaseHookRefs,
    BasePrePostModelHooksOnSeeds,
    BaseHooksRefsOnSeeds,
    #BasePrePostModelHooksOnSeedsPlusPrefixed,
    #BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace,
    #BasePrePostModelHooksOnSnapshots,
    #BasePrePostModelHooksInConfig,
    #BasePrePostModelHooksInConfigWithCount,
    #BasePrePostModelHooksInConfigKwargs,
    BasePrePostSnapshotHooksInConfigKwargs,
    BaseDuplicateHooksInConfigs,
)
from dbt.tests.adapter.hooks.test_run_hooks import (
    BasePrePostRunHooks,
    BaseAfterRunHooks,
)
from fixtures import (
    models__hooked,
)
from dbt.tests.adapter.hooks import fixtures
from tests.utils.sql_patch_helpers import SqlGlobalOverrideMixin


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


#class TestPrePostModelHooksOnSeedsPlusPrefixed(BasePrePostModelHooksOnSeedsPlusPrefixed):
#    pass


#class TestPrePostModelHooksOnSeedsPlusPrefixedWhitespace(
#    BasePrePostModelHooksOnSeedsPlusPrefixedWhitespace
#):
#    pass


#class TestPrePostModelHooksOnSnapshots(BasePrePostModelHooksOnSnapshots):
#    pass


#class TestPrePostModelHooksInConfig(BasePrePostModelHooksInConfig):
#    pass


#class TestPrePostModelHooksInConfigWithCount(BasePrePostModelHooksInConfigWithCount):
#    pass


#class TestPrePostModelHooksInConfigKwargs(BasePrePostModelHooksInConfigKwargs):
#    pass


class TestPrePostSnapshotHooksInConfigKwargs(BasePrePostSnapshotHooksInConfigKwargs):
    pass


class TestDuplicateHooksInConfigs(BaseDuplicateHooksInConfigs):
    pass


class TestPrePostRunHooks(BasePrePostRunHooks):
    pass


class TestAfterRunHooks(BaseAfterRunHooks):
    pass

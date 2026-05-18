import pytest

from dbt.tests.adapter.empty.test_empty import BaseTestEmpty, BaseTestEmptyInlineSourceRef
from dbt.tests.util import run_dbt


class TestSingleStoreEmpty(BaseTestEmpty):
    pass


class TestSingleStoreEmptyInlineSourceRef(BaseTestEmptyInlineSourceRef):
    pass


# Regression test for the multi-CTE --empty rendering bug.
# `dbt run --empty` rewrites every {{ ref(...) }} as a `_dbt_limit_subq_<n>`
# limited subquery. The singlestore__strip_db_limit_aliases macro must clean
# those tokens up *only on their own physical SQL line*; an earlier
# implementation searched forward globally for the trailing ` as ` and, in
# multi-CTE models, matched the next CTE's `as (` instead, splicing across
# CTE boundaries and producing invalid SQL such as `(...) as (` with the
# next CTE's alias swallowed.
_upstream_a_sql = "select 1 as id, 'a' as name"
_upstream_b_sql = "select 1 as id, 10 as score"

_multi_cte_model_sql = """
with a as (
    select id, name from {{ ref('upstream_a') }}
),

b as (
    select id, score from {{ ref('upstream_b') }}
)

select a.id, a.name, b.score
from a
join b on a.id = b.id
"""


class TestSingleStoreEmptyMultiCTE:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "upstream_a.sql": _upstream_a_sql,
            "upstream_b.sql": _upstream_b_sql,
            "multi_cte_model.sql": _multi_cte_model_sql,
        }

    def test_run_empty_on_multi_cte_model(self, project):
        # baseline run should always succeed
        run_dbt(["run"])
        # --empty must compile + execute the multi-CTE view without splicing
        # CTE boundaries when stripping the _dbt_limit_subq_ aliases
        run_dbt(["run", "--empty"])

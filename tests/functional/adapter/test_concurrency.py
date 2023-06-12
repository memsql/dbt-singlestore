import pytest
from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
    rm_file,
    write_file,
    run_dbt_and_capture
)
from dbt.tests.adapter.concurrency.test_concurrency import (
    BaseConcurrency,
    seeds__update_csv,
    models__dep_sql,
    models__view_with_conflicting_cascade_sql,
    models__skip_sql
)


# There is no 
models__invalid_sql = """
{{
  config(
    materialized = "table"
  )
}}

select a_field_that_does_not_exist from seed

"""

models__table_a_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from seed

"""

models__table_b_sql = """
{{
  config(
    materialized = "table"
  )
}}

select * from seed

"""

models__view_model_sql = """
{{
  config(
    materialized = "view"
  )
}}

select * from seed

"""


class TestConcurrency(BaseConcurrency):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "invalid.sql": models__invalid_sql,
            "table_a.sql": models__table_a_sql,
            "table_b.sql": models__table_b_sql,
            "view_model.sql": models__view_model_sql,
            "dep.sql": models__dep_sql,
            "view_with_conflicting_cascade.sql": models__view_with_conflicting_cascade_sql,
            "skip.sql": models__skip_sql,
        }

    def test_conncurrency_singlestore(self, project):
        run_dbt(["seed", "--select", "seed"])
        results = run_dbt(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])

        rm_file(project.project_root, "seeds", "seed.csv")
        write_file(seeds__update_csv, project.project_root, "seeds", "seed.csv")

        results, output = run_dbt_and_capture(["run"], expect_pass=False)
        assert len(results) == 7
        check_relations_equal(project.adapter, ["seed", "view_model"])
        check_relations_equal(project.adapter, ["seed", "dep"])
        check_relations_equal(project.adapter, ["seed", "table_a"])
        check_relations_equal(project.adapter, ["seed", "table_b"])

        assert "PASS=5 WARN=0 ERROR=1 SKIP=1 TOTAL=7" in output

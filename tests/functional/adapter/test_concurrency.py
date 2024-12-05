import pytest
from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
    check_table_does_not_exist,
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


# Previously, SELECT-command was trying to access this.schema, but
# SingleStore doesn't have one
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

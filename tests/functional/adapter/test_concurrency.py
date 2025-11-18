import pytest
from dbt.tests.util import (
    run_dbt,
    check_relations_equal,
    check_table_does_not_exist,
    rm_file,
    write_file,
    run_dbt_and_capture
)
from dbt.tests.adapter.concurrency.test_concurrency import BaseConcurrency
from tests.utils.sql_patch_helpers import SqlGlobalOverrideMixin


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


class TestConcurrency(SqlGlobalOverrideMixin, BaseConcurrency):
    BASE_TEST_CLASS = BaseConcurrency
    SQL_GLOBAL_OVERRIDES = {
        "models__invalid_sql": models__invalid_sql,
        "models__table_a_sql": models__table_a_sql,
        "models__table_b_sql": models__table_b_sql,
        "models__view_model_sql": models__view_model_sql,
    }
    pass

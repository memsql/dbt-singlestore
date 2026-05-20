# Copyright 2021-2026 SingleStore, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

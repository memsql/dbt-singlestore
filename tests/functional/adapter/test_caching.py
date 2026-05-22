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
from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingUppercaseModel,
    BaseCachingSelectedSchemaOnly,
    BaseNoPopulateCache,
)
from tests.utils.sql_patch_helpers import SqlGlobalOverrideMixin


# we don't support custom schema in models in a way dbt expects, so we override this model
another_schema_model_sql = """
{{
    config(
        materialized='table'
    )
}}
select 1 as id
"""


class TestCachingLowerCaseModel(BaseCachingLowercaseModel):
    pass


class TestCachingSelectedSchemaOnly(SqlGlobalOverrideMixin, BaseCachingSelectedSchemaOnly):
    BASE_TEST_CLASS = BaseCachingSelectedSchemaOnly
    SQL_GLOBAL_OVERRIDES = {
        "another_schema_model_sql": another_schema_model_sql,
    }
    pass


class TestNoPopulateCache(BaseNoPopulateCache):
    pass

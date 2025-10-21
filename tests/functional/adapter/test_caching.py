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

# TODO double-check this one
#class TestCachingUppercaseModel(BaseCachingUppercaseModel):
#    pass


class TestCachingSelectedSchemaOnly(SqlGlobalOverrideMixin, BaseCachingSelectedSchemaOnly):
    BASE_TEST_CLASS = BaseCachingSelectedSchemaOnly
    SQL_GLOBAL_OVERRIDES = {
        "another_schema_model_sql": another_schema_model_sql,
    }
    pass


class TestNoPopulateCache(BaseNoPopulateCache):
    pass

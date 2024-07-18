import pytest
from dbt.tests.adapter.caching.test_caching import (
    BaseCachingLowercaseModel,
    BaseCachingSelectedSchemaOnly,
    TestNoPopulateCache,
    model_sql,
)

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


class TestCachingSelectedSchemaOnly(BaseCachingSelectedSchemaOnly):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": model_sql,
            "another_schema_model.sql": another_schema_model_sql,
        }
    pass

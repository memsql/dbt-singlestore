import pytest
from dbt.tests.adapter.simple_copy.test_simple_copy import (
    SimpleCopyBase,
    EmptyModelsArentRunBase,
)


class TestSimpleCopyBase(SimpleCopyBase):
    def test_simple_copy_with_materialized_views(self, project):
        pytest.skip("SingleStore does not support materialized views")
    pass


class TestEmptyModelsArentRun(EmptyModelsArentRunBase):
    pass

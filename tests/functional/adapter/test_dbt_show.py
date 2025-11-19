import pytest

from dbt_common.exceptions import DbtRuntimeError
from dbt.tests.util import run_dbt
from dbt.tests.adapter.dbt_show.test_dbt_show import (
     BaseShowSqlHeader,
     BaseShowLimit,
     BaseShowDoesNotHandleDoubleLimit
)


class TestSingleStoreShowLimit(BaseShowLimit):
    pass


class TestSingleStoreShowSqlHeader(BaseShowSqlHeader):
    pass


class TestShowDoesNotHandleDoubleLimit(BaseShowDoesNotHandleDoubleLimit):
    DATABASE_ERROR_MESSAGE = "You have an error in your SQL syntax"

    def test_double_limit_throws_syntax_error(self, project):
        with pytest.raises(DbtRuntimeError) as e:
            run_dbt(["show", "--limit", "1", "--inline", "select 1 limit 1"])

        assert self.DATABASE_ERROR_MESSAGE in str(e.value)
    pass

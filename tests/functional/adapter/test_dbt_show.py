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

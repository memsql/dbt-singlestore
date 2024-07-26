import pytest
from dbt.tests.adapter.store_test_failures_tests import basic
from dbt.tests.adapter.store_test_failures_tests.test_store_test_failures import (
    TestStoreTestFailures
)
from dbt.tests.adapter.store_test_failures_tests import _files
from dbt.contracts.results import TestStatus
from collections import namedtuple


class TestSingleStoreStoreTestFailures(TestStoreTestFailures):
    pass


class TestStoreTestFailuresAsInteractions(basic.StoreTestFailuresAsInteractions):
    pass


class TestStoreTestFailuresAsProjectLevelOff(basic.StoreTestFailuresAsProjectLevelOff):
    pass


class TestStoreTestFailuresAsProjectLevelView(basic.StoreTestFailuresAsProjectLevelView):
    pass


class TestStoreTestFailuresAsGeneric(basic.StoreTestFailuresAsGeneric):
    pass


class StoreTestFailuresAsProjectLevelEphemeral(basic.StoreTestFailuresAsBase):
    pass


class TestStoreTestFailuresAsExceptions(basic.StoreTestFailuresAsExceptions):
    pass

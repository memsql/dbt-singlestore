import pytest

from dbt.tests.adapter.unit_testing.test_types import BaseUnitTestingTypes
from dbt.tests.adapter.unit_testing.test_case_insensitivity import BaseUnitTestCaseInsensivity
from dbt.tests.adapter.unit_testing.test_invalid_input import BaseUnitTestInvalidInput


class TestSingleStoreUnitTestingTypes(BaseUnitTestingTypes):
    @pytest.fixture
    def data_types(self):
        # sql_value, yaml_value
        return [
            ["1", "1"],
            ["'1'", "1"],
            ["('true' :> bool)", "true"],
            ["1.0", "1.0"],
            ["'string value'", "string value"],
            ["(1 :> bigint)", 1],
            ["('2019-01-01' :> date)", "2019-01-01"],
            ["('2013-11-03 00:00:00-07' :> timestamp)", "2013-11-03 00:00:00-07"],
            ["('POINT(-74.044514 40.689244)' :> GEOGRAPHY)", "POINT(-74.04451396 40.68924403)"],
            # Add vector and maybe smth else? ["('[12, 11, 1997]' :> VECTOR(3, I32))", "[12, 11, 1997]"],
        ]


class TestSingleStoreUnitTestCaseInsensitivity(BaseUnitTestCaseInsensivity):
    pass


class TestSingleStoreUnitTestInvalidInput(BaseUnitTestInvalidInput):
    pass

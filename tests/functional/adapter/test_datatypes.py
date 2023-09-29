import pytest
from dbt.tests.adapter.utils.data_types.test_type_bigint import BaseTypeBigInt
from dbt.tests.adapter.utils.data_types.test_type_float import BaseTypeFloat
from dbt.tests.adapter.utils.data_types.test_type_int import BaseTypeInt
from dbt.tests.adapter.utils.data_types.test_type_numeric import BaseTypeNumeric
from dbt.tests.adapter.utils.data_types.test_type_string import BaseTypeString
from dbt.tests.adapter.utils.data_types.test_type_timestamp import BaseTypeTimestamp
from dbt.tests.adapter.utils.data_types.test_type_boolean import BaseTypeBoolean


seeds__typeint_expected_csv = """int_col
12345678
""".lstrip()

models__typeint_actual_sql = """
select ('12345678' :> {{ type_int() }}) as int_col
"""


class TestTypeInt(BaseTypeInt):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected.csv": seeds__typeint_expected_csv}
    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__typeint_actual_sql, "type_int")}
    pass


seeds__typestring_expected_csv = """string_col
"Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
""".lstrip()

models__typestring_actual_sql = """
select ('Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.'
:> {{ type_string() }}) as string_col
"""


class TestTypeString(BaseTypeString):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"expected.csv": seeds__typestring_expected_csv}
    @pytest.fixture(scope="class")
    def models(self):
        return {"actual.sql": self.interpolate_macro_namespace(models__typestring_actual_sql, "type_string")}
    pass

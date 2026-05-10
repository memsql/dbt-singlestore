import pytest

from dbt.tests.adapter.unit_testing.test_types import (
    BaseUnitTestingTypes,
    BaseUnitTestingVarcharFixtureNoTruncation,
    _length_model_sql,
    _length_test_yml,
    my_upstream_model_sql,
)
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


# Regression test for unit-test fixture casts on string columns.
# When the upstream model declares a narrow varchar (here: varchar(5)) and the
# unit-test fixture provides a longer string, dbt-core 1.11+ strips the length
# from the cast type to avoid silent truncation (dbt-labs/dbt-core GH-11974)
# and calls singlestore__safe_cast(value, "character varying"). Without the
# Postgres-style -> SingleStore-native translation the rendered SQL is either
# rejected (`!:> varchar` requires a length) or silently truncates to one
# character (`!:> char`). The fix maps the unbounded form to `longtext`.
#
# The base test's upstream model uses ANSI `cast(x as varchar(5))`, which
# SingleStore's CAST does not accept. We override the model to use the
# SingleStore-native `:>` cast operator while preserving the test's intent
# (a narrow varchar column whose fixture must not be truncated).
class TestSingleStoreUnitTestingVarcharFixtureNoTruncation(
    BaseUnitTestingVarcharFixtureNoTruncation
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": _length_model_sql,
            "my_upstream_model.sql": my_upstream_model_sql.format(
                sql_value="('short' :> varchar(5))"
            ),
            "schema.yml": _length_test_yml,
        }

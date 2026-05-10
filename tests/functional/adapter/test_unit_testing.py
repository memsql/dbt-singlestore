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


# Direct unit test for the singlestore__translate_safe_cast_type helper.
# In practice dbt-core only ever passes a fully lower-case Postgres-flavored
# type into safe_cast (since Column.string_type/numeric_type produce lower-case
# strings), so the BaseUnitTestingVarcharFixtureNoTruncation regression above
# only exercises the lower-case path. This test pins down the *case-insensitive*
# contract of the translator so a future "simplification" of the macro can't
# silently let mixed-case Postgres types leak through to the `!:>` cast (which
# would fail at runtime with a SingleStore parse error).
class TestSingleStoreTranslateSafeCastType:
    @pytest.fixture(scope="class")
    def models(self):
        # A trivial model is required for the project fixture to bootstrap;
        # we don't materialise it.
        return {"placeholder.sql": "select 1 as one"}

    @pytest.mark.parametrize(
        "input_type,expected",
        [
            # Lower-case Postgres-flavored inputs (what dbt-core actually emits).
            ("character varying(30)", "varchar(30)"),
            ("character varying", "longtext"),
            ("character(10)", "char(10)"),
            ("character", "longtext"),
            # Mixed / upper case inputs: branch selection is case-insensitive,
            # so the translation MUST also be case-insensitive end-to-end.
            ("Character Varying(30)", "varchar(30)"),
            ("CHARACTER VARYING(30)", "varchar(30)"),
            ("Character(10)", "char(10)"),
            ("CHARACTER(10)", "char(10)"),
            # SingleStore-native and unrelated types must pass through untouched.
            ("varchar(30)", "varchar(30)"),
            ("bigint(20)", "bigint(20)"),
            ("JSON", "JSON"),
            ("timestamp", "timestamp"),
        ],
    )
    def test_translates_postgres_string_types_case_insensitively(
        self, project, input_type, expected
    ):
        with project.adapter.connection_named("_test"):
            actual = project.adapter.execute_macro(
                "singlestore__translate_safe_cast_type",
                kwargs={"type": input_type},
            )
        assert actual.strip() == expected, (
            f"singlestore__translate_safe_cast_type({input_type!r}) "
            f"returned {actual.strip()!r}; expected {expected!r}"
        )

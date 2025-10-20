import pytest
from dbt.tests.adapter.aliases.fixtures import MACROS__EXPECT_VALUE_SQL
from dbt.tests.adapter.aliases.test_aliases import (
    BaseAliases,
    BaseAliasErrors,
)


MACROS__CAST_SQL = """
{% macro singlestore__string_literal(s) -%}
  ('{{ s }}' :> text)
{%- endmacro %}
"""


class TestAliases(BaseAliases):
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "cast.sql": MACROS__CAST_SQL,
            "expect_value.sql": MACROS__EXPECT_VALUE_SQL,
        }
    pass


class TestAliasErrors(BaseAliasErrors):
    pass

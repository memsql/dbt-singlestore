import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
)


my_model_contract_sql_header_sql = """
{{
  config(
    materialized = "table"
  )
}}
{% call set_sql_header(config) %}
SET MY_VARIABLE='test';
{% endcall %}
SELECT $MY_VARIABLE as column_name
"""

my_model_incremental_contract_sql_header_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change="append_new_columns"
  )
}}
{% call set_sql_header(config) %}
SET MY_VARIABLE='test';
{% endcall %}
SELECT $MY_VARIABLE as column_name
"""

_expected_sql_snowflake = """
create or replace transient table <model_identifier> (
    id integer not null primary key references <foreign_key_model_identifier> (id) unique,
    color text,
    date_day text
) as ( select
        id,
        color,
        date_day from
    (
    -- depends_on: <foreign_key_model_identifier>
    select
        'blue' as color,
        1 as id,
        '2019-01-01' as date_day
    ) as model_subq
);
"""


class SingleStoreColumnEqualSetup:

    @pytest.fixture
    def data_types(self, int_type, schema_int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["true", "boolean", "BOOLEAN"],
            ["'2013-11-03 00:00:00-07'::timestamptz", "timestamp_tz", "TIMESTAMP_TZ"],
            ["'2013-11-03 00:00:00-07'::timestamp", "timestamp", "TIMESTAMP_NTZ"],
            ["ARRAY_CONSTRUCT('a','b','c')", "array", "ARRAY"],
            ["ARRAY_CONSTRUCT(1,2,3)", "array", "ARRAY"],
            [
                """TO_VARIANT(PARSE_JSON('{"key3": "value3", "key4": "value4"}'))""",
                "variant",
                "VARIANT",
            ],
        ]


class TestTableConstraintsColumnsEqual(SingleStoreColumnEqualSetup, BaseTableConstraintsColumnsEqual):
    pass


class TestViewConstraintsColumnsEqual(SingleStoreColumnEqualSetup, BaseViewConstraintsColumnsEqual):
    pass


class TestIncrementalConstraintsColumnsEqual(SingleStoreColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual):
    pass
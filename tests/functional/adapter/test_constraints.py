import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual
)

# base mode definitions
my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  1 as id,
  'blue' as color,
  '2019-01-01' as date_day
"""

my_model_view_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  1 as id,
  'blue' as color,
  '2019-01-01' as date_day
"""

my_incremental_model_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  1 as id,
  'blue' as color,
  '2019-01-01' as date_day
"""

# model columns in a different order to schema definitions
my_model_wrong_order_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""

my_model_view_wrong_order_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""

my_model_incremental_wrong_order_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  'blue' as color,
  1 as id,
  '2019-01-01' as date_day
"""

# model columns name different to schema definitions
my_model_wrong_name_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  'blue' as color,
  1 as error,
  '2019-01-01' as date_day
"""

my_model_view_wrong_name_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  'blue' as color,
  1 as error,
  '2019-01-01' as date_day
"""

my_model_incremental_wrong_name_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  'blue' as color,
  1 as error,
  '2019-01-01' as date_day
"""

# model columns data types different to schema definitions
my_model_data_type_sql = """
{{{{
  config(
    materialized = "table"
  )
}}}}

select
  {sql_value} as wrong_data_type_column_name
"""

# model breaking constraints
my_model_with_nulls_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  -- null value for 'id'
  cast(null as SIGNED) as id,
  -- change the color as well (to test rollback)
  'red' as color,
  '2019-01-01' as date_day
"""

my_model_view_with_nulls_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  -- null value for 'id'
  cast(null as SIGNED) as id,
  -- change the color as well (to test rollback)
  'red' as color,
  '2019-01-01' as date_day
"""

my_model_incremental_with_nulls_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'  )
}}

select
  -- null value for 'id'
  cast(null as SIGNED) as id,
  -- change the color as well (to test rollback)
  'red' as color,
  '2019-01-01' as date_day
"""

model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    columns:
      - name: id
        quote: true
        data_type: signed integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: text
  - name: my_model_error
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: signed integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: text
  - name: my_model_wrong_order
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: signed integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: text
  - name: my_model_wrong_name
    config:
      contract:
        enforced: true
    columns:
      - name: id
        data_type: signed integer
        description: hello
        constraints:
          - type: not_null
          - type: primary_key
          - type: check
            expression: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: text
"""

constrained_model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      contract:
        enforced: true
    constraints:
      - type: check
        expression: (id > 0)
      - type: primary_key
        columns: [ id ]
      - type: unique
        columns: [ color, date_day ]
        name: strange_uniqueness_requirement
    columns:
      - name: id
        quote: true
        data_type: signed integer
        description: hello
        constraints:
          - type: not_null
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: text
"""


model_data_type_schema_yml = """
version: 2
models:
  - name: my_model_data_type
    config:
      contract:
        enforced: true
    columns:
      - name: wrong_data_type_column_name
        data_type: {data_type}
"""


class SingleStoreColumnEqualSetup:
    @pytest.fixture
    def int_type(self):
        return "SIGNED"

    @pytest.fixture
    def schema_int_type(self):
        return "INT"

    @pytest.fixture
    def string_type(self):
        return "LONGTEXT"

    @pytest.fixture
    def data_types(self, int_type, schema_int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["1", schema_int_type, int_type],
            ["'1'", string_type, string_type],
            ["true", "bool", "BOOL"],
            ["cast('2019-01-01' as date)", "date", "DATE"],
            ["cast('2013-11-03 00:00:00-07' as datetime(6))", "datetime", "DATETIME"],
            ["'1'::numeric", "numeric", "DECIMAL"],
            ["""'{"bar": "baz", "balance": 7.77, "active": false}'::json""", "json", "JSON"],
        ]


class TestTableConstraintsColumnsEqual(SingleStoreColumnEqualSetup, BaseTableConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }

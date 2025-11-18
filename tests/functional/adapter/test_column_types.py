import pytest
from dbt.tests.adapter.column_types.test_column_types import BaseColumnTypes


_MODEL_SQL = """
{{ config(materialized='table') }}
select
    (1   :> TINYINT)          as tinyint_col,
    (2   :> SMALLINT)         as smallint_col,
    (3   :> MEDIUMINT)        as mediumint_col,
    (4   :> INT)              as int_col,
    (5   :> BIGINT)           as bigint_col,

    (6.0 :> FLOAT)            as float_col,
    (7.0 :> DOUBLE)           as double_col,
    (8.0 :> REAL)             as real_col,

    (9.0  :> DECIMAL(18,4))   as decimal_col,
    (10.0 :> NUMERIC(18,4))   as numeric_col,

    ('x' :> TEXT)             as textish_col,   
    ('y' :> VARCHAR(20))      as varchar_col
"""

_SCHEMA_YML = """
version: 2
models:
  - name: model
    data_tests:
      - is_type:
          column_map:
            tinyint_col:   ['integer', 'number']
            smallint_col:  ['integer', 'number']
            mediumint_col: ['integer', 'number']
            int_col:       ['integer', 'number']
            bigint_col:    ['integer', 'number']

            float_col:     ['float',   'number']
            double_col:    ['float',   'number']
            real_col:      ['float',   'number']

            decimal_col:   ['numeric', 'number']
            numeric_col:   ['numeric', 'number']

            textish_col:   ['string',  'not number']
            varchar_col:   ['string',  'not number']
"""


class TestSingleStoreColumnTypes(BaseColumnTypes):
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": _MODEL_SQL, "schema.yml": _SCHEMA_YML}

    def test_run_and_test(self, project):
        self.run_and_test()

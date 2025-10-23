import pytest
from dbt.tests.adapter.column_types.test_column_types import BaseColumnTypes


macro_test_is_type_sql = """
{% macro simple_type_check_column(column, check) %}
    {% if check == 'string' %}
        {{ return(column.is_string()) }}
    {% elif check == 'float' %}
        {{ return(column.is_float()) }}
    {% elif check == 'number' %}
        {{ return(column.is_number()) }}
    {% elif check == 'numeric' %}
        {{ return(column.is_numeric()) }}
    {% elif check == 'integer' %}
        {{ return(column.is_integer()) }}
    {% else %}
        {% do exceptions.raise_compiler_error('invalid type check value: ' ~ check) %}
    {% endif %}
{% endmacro %}

{% macro type_check_column(column, type_checks) %}
    {% set failures = [] %}
    {% for type_check in type_checks %}
        {% if type_check.startswith('not ') %}
            {% if simple_type_check_column(column, type_check[4:]) %}
                {% do log('simple_type_check_column got ', True) %}
                {% do failures.append(type_check) %}
            {% endif %}
        {% else %}
            {% if not simple_type_check_column(column, type_check) %}
                {% do failures.append(type_check) %}
            {% endif %}
        {% endif %}
    {% endfor %}
    {% if (failures | length) > 0 %}
        {% do log('column ' ~ column.name ~ ' had failures: ' ~ failures, info=True) %}
    {% endif %}
    {% do return((failures | length) == 0) %}
{% endmacro %}

{% test is_type(model, column_map) %}
    {% if not execute %}
        {{ return(None) }}
    {% endif %}
    {% if not column_map %}
        {% do exceptions.raise_compiler_error('test_is_type must have a column name') %}
    {% endif %}
    {% set columns = adapter.get_columns_in_relation(model) %}
    {% if (column_map | length) != (columns | length) %}
        {% set column_map_keys = (column_map | list | string) %}
        {% set column_names = (columns | map(attribute='name') | list | string) %}
        {% do exceptions.raise_compiler_error('did not get all the columns/all columns not specified:\n' ~ column_map_keys ~ '\nvs\n' ~ column_names) %}
    {% endif %}
    {% set bad_columns = [] %}
    {% for column in columns %}
        {% set column_key = (column.name | lower) %}
        {% if column_key in column_map %}
            {% set type_checks = column_map[column_key] %}
            {% if not type_checks %}
                {% do exceptions.raise_compiler_error('no type checks?') %}
            {% endif %}
            {% if not type_check_column(column, type_checks) %}
                {% do bad_columns.append(column.name) %}
            {% endif %}
        {% else %}
            {% do exceptions.raise_compiler_error('column key ' ~ column_key ~ ' not found in ' ~ (column_map | list | string)) %}
        {% endif %}
    {% endfor %}
    {% do log('bad columns: ' ~ bad_columns, info=True) %}

    {% if bad_columns | length == 0 %}
      select * from (select 1 limit 0) as nothing
    {% else %}
      {% for bad_column in bad_columns %}
        select '{{ bad_column }}' as bad_column
        {{ 'union all' if not loop.last }}
      {% endfor %}
    {% endif %}
{% endtest %}
"""

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
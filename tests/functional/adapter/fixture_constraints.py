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
        data_type: integer
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
        data_type: integer
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
        data_type: integer
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
        data_type: integer
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
    columns:
      - name: id
        quote: true
        data_type: integer
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


# base mode definitions
my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  (1 :> {{ dbt.type_int() }}) as id,
  ('blue' :> {{ dbt.type_string() }}) as color,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

my_model_view_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  (1 :> {{ dbt.type_int() }}) as id,
  ('blue' :> {{ dbt.type_string() }}) as color,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

my_incremental_model_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  (1 :> {{ dbt.type_int() }}) as id,
  ('blue' :> {{ dbt.type_string() }}) as color,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

# model columns in a different order to schema definitions
my_model_wrong_order_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  ('blue' :> {{ dbt.type_string() }}) as color,
  (1 :> {{ dbt.type_int() }}) as id,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

my_model_view_wrong_order_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  ('blue' :> {{ dbt.type_string() }}) as color,
  (1 :> {{ dbt.type_int() }}) as id,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

my_model_incremental_wrong_order_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  ('blue' :> {{ dbt.type_string() }}) as color,
  (1 :> {{ dbt.type_int() }}) as id,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

# model columns name different to schema definitions
my_model_wrong_name_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  ('blue' :> {{ dbt.type_string() }}) as color,
  (1 :> {{ dbt.type_int() }}) as error,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

my_model_view_wrong_name_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  ('blue' :> {{ dbt.type_string() }}) as color,
  (1 :> {{ dbt.type_int() }}) as error,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

my_model_incremental_wrong_name_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'
  )
}}

select
  ('blue' :> {{ dbt.type_string() }}) as color,
  (1 :> {{ dbt.type_int() }}) as error,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
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
  (null :> {{ dbt.type_int() }}) as id,
  -- change the color as well (to test rollback)
  ('red' :> {{ dbt.type_string() }}) as color,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

my_model_view_with_nulls_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  -- null value for 'id'
  (null :> {{ dbt.type_int() }}) as id,
  -- change the color as well (to test rollback)
  ('red' :> {{ dbt.type_string() }}) as color,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""

my_model_incremental_with_nulls_sql = """
{{
  config(
    materialized = "incremental",
    on_schema_change='append_new_columns'  )
}}

select
  -- null value for 'id'
  (null :> {{ dbt.type_int() }}) as id,
  -- change the color as well (to test rollback)
  ('red' :> {{ dbt.type_string() }}) as color,
  ('2019-01-01' :> {{ dbt.type_string() }}) as date_day
"""


_expected_sql_singlestore = """
create table <model_identifier> (
    id integer not null primary key,
    color text,
    date_day text
) as select
        id,
        color,
        date_day from
    (
    select
        ('blue' :> TEXT) as color,
        (1 :> INT) as id,
        ('2019-01-01' :> TEXT) as date_day
    ) as model_subq
"""

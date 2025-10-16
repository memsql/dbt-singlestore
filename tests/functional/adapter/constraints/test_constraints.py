import pytest

from dbt.tests.adapter.constraints.test_constraints import (
    BaseTableConstraintsColumnsEqual,
    BaseViewConstraintsColumnsEqual,
    BaseIncrementalConstraintsColumnsEqual,
    BaseConstraintsRuntimeDdlEnforcement,
    BaseConstraintsRollback,
    BaseIncrementalConstraintsRuntimeDdlEnforcement,
    BaseIncrementalConstraintsRollback,
    BaseModelConstraintsRuntimeEnforcement,
    BaseConstraintQuotedColumn,
)

from dbt.tests.adapter.constraints.fixtures import (
    model_contract_header_schema_yml,
    model_quoted_column_schema_yml,
)

from fixture_constraints import (
    my_model_sql,
    my_incremental_model_sql,
    my_model_wrong_order_sql,
    my_model_wrong_name_sql,
    my_model_view_wrong_order_sql,
    my_model_view_wrong_name_sql,
    my_model_incremental_wrong_order_sql,
    my_model_incremental_wrong_name_sql,
    my_model_with_nulls_sql,
    my_model_with_quoted_column_name_sql,
    model_schema_yml,
    constrained_model_schema_yml,
    _expected_sql_singlestore
)


class SingleStoreColumnEqualSetup:
    @pytest.fixture
    def int_type(self):
        return "LONG"

    @pytest.fixture
    def schema_int_type(self):
        return "INT"

    @pytest.fixture
    def string_type(self):
        return "BLOB"

    @pytest.fixture
    def data_types(self, int_type, schema_int_type, string_type):
        # sql_column_value, schema_data_type, error_data_type
        return [
            ["(1 :> int)", schema_int_type, int_type],
            ["('1' :> text)", "TEXT", string_type],
            ["(true :> bool)", "bool", "TINY"],
            ["('2019-01-01' :> date)", "date", "DATE"],
            ["('2013-11-03 12:00:00' :> datetime)", "datetime", "DATETIME"],
            ["('1' :> numeric)", "numeric", "DECIMAL"],
            ["""('{"bar": "baz", "balance": 7.77, "active": false}' :> json)""", "json", "JSON"],
        ]


class TestTableConstraintsColumnsEqual(SingleStoreColumnEqualSetup, BaseTableConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }
    pass


class TestViewConstraintsColumnsEqual(SingleStoreColumnEqualSetup, BaseViewConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_view_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_view_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }
    pass


class TestIncrementalConstraintsColumnsEqual(SingleStoreColumnEqualSetup, BaseIncrementalConstraintsColumnsEqual):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_incremental_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_incremental_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }
    pass


class TestTableConstraintsDdlEnforcement(BaseConstraintsRuntimeDdlEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_singlestore
    pass


class TestIncrementalConstraintsDdlEnforcement(
    BaseIncrementalConstraintsRuntimeDdlEnforcement
):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_wrong_order_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return _expected_sql_singlestore
    pass


class TestTableConstraintsRollback(BaseConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return my_model_with_nulls_sql

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Column 'id' cannot be null"]
    pass


class TestIncrementalConstraintsRollback(BaseIncrementalConstraintsRollback):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_incremental_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def null_model_sql(self):
        return my_model_with_nulls_sql

    @pytest.fixture(scope="class")
    def expected_error_messages(self):
        return ["Column 'id' cannot be null"]
    pass


class TestModelConstraintsRuntimeEnforcement(BaseModelConstraintsRuntimeEnforcement):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": constrained_model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> (
    id integer not null,
    color text,
    date_day text,
    primary key (id)
) as select
        id,
        color,
        date_day from
    (
    select
        (1 :> INTEGER) as id,
        ('blue' :> TEXT) as color,
        ('2019-01-01' :> TEXT) as date_day
    ) as model_subq
"""
    pass


class TestConstraintQuotedColumn(BaseConstraintQuotedColumn):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_with_quoted_column_name_sql,
            "constraints_schema.yml": model_quoted_column_schema_yml,
        }
    @pytest.fixture(scope="class")
    def expected_sql(self):
        return """
create table <model_identifier> (
    id integer not null,
    `from` text not null,
    date_day text,
    shard key()
) as
    select id, `from`, date_day
    from (
        select
          ('blue' :> TEXT) as `from`,
          (1 :> INTEGER) as id,
          ('2019-01-01' :> TEXT) as date_day
    ) as model_subq
"""
    pass

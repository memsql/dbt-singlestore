import pytest
import os
from dbt.tests.util import run_dbt

pytest_plugins = ["dbt.tests.fixtures.project"]


class TestSingleStoreReferenceTables:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "ref_table.sql": "{{ config(materialized='table', reference=true) }} select 1 as id",
            "rowstore_ref_table.sql": "{{ config(materialized='table', reference=true, storage_type='rowstore', primary_key=['id']) }} select 1 as id",
        }

    def test_reference_tables_created(self, project):
        results = run_dbt(["run"])
        assert len(results) == 2

        ref_table_relation = project.adapter.get_relation(
            database=project.database,
            schema=project.test_schema,
            identifier="ref_table",
        )

        ref_ddl_query = f"SHOW CREATE TABLE {ref_table_relation.render()}"
        ref_ddl_result = project.run_sql(ref_ddl_query, fetch="one")
        ref_ddl_sql = ref_ddl_result[1]

        assert (
            "CREATE REFERENCE TABLE" in ref_ddl_sql.upper()
        ), f"Expected standard REFERENCE table, got DDL: {ref_ddl_sql}"

        rowstore_ref_relation = project.adapter.get_relation(
            database=project.database,
            schema=project.test_schema,
            identifier="rowstore_ref_table",
        )
        rowstore_ddl_query = f"SHOW CREATE TABLE {rowstore_ref_relation.render()}"
        rowstore_ddl_result = project.run_sql(rowstore_ddl_query, fetch="one")
        rowstore_ddl_sql = rowstore_ddl_result[1]

        assert (
            "CREATE ROWSTORE REFERENCE TABLE" in rowstore_ddl_sql.upper()
        ), f"Expected ROWSTORE REFERENCE table, got DDL: {rowstore_ddl_sql}"

import pytest
from dbt.contracts.results import RunStatus
from dbt.tests.adapter.basic.test_docs_generate import (
    BaseDocsGenerate,
    BaseDocsGenReferences,
    models__model_sql,
    models__schema_yml,
    models__readme_md
)
from dbt.tests.adapter.basic.expected_catalog import (
    base_expected_catalog,
    no_stats,
    expected_references_catalog,
)
from dbt.tests.util import run_dbt

class TestDocsGenReferences(BaseDocsGenReferences):
    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        return expected_references_catalog(
            project,
            role=None,
            id_type="int",
            text_type="text",
            time_type="datetime",
            bigint_type="bigint",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
        )

# we don't support custom schema in models in a way dbt expects, so we override second model
models__second_model_sql = """
{{
    config(
        materialized='view',
    )
}}

select * from {{ ref('seed') }}
"""

class TestDocsGenerate(BaseDocsGenerate):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "schema.yml": models__schema_yml,
            "second_model.sql": models__second_model_sql,
            "readme.md": models__readme_md,
            "model.sql": models__model_sql,
        }

    @pytest.fixture(scope="class")
    def expected_catalog(self, project, profile_user):
        catalog = base_expected_catalog(
            project,
            role=None,
            id_type="int",
            text_type="text",
            time_type="datetime",
            view_type="view",
            table_type="table",
            model_stats=no_stats(),
        )
        catalog["nodes"]["model.test.second_model"]["metadata"]["schema"] = project.test_schema
        return catalog


model_sql = """
select 1 as id
"""

override_macros_sql = """
{% macro get_catalog_relations(information_schema, relations) %}
    {{ return(singlestore__get_catalog_relations(information_schema, relations)) }}
{% endmacro %}
"""


class TestDocsGenerateOverride:
    @pytest.fixture(scope="class")
    def models(self):
        return {"model.sql": model_sql}

    @pytest.fixture(scope="class")
    def macros(self):
        return {"override_macros_sql.sql": override_macros_sql}

    def test_generate_docs(
        self,
        project,
    ):
        results = run_dbt(["run"])
        assert len(results) == 1

        docs_generate = run_dbt(["--warn-error", "docs", "generate"])
        assert len(docs_generate._compile_results.results) == 1
        assert docs_generate._compile_results.results[0].status == RunStatus.Success
        assert docs_generate.errors is None
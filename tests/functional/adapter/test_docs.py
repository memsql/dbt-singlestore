import pytest
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

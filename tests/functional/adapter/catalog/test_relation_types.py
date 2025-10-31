from dbt.artifacts.schemas.catalog import CatalogArtifact
import pytest

from dbt.tests.adapter.catalog import files
from dbt.tests.util import run_dbt

# TODO: uncomment materialized view part once PLAT-6917 is implemented
class TestCatalogRelationTypes:
    @pytest.fixture(scope="class", autouse=True)
    def seeds(self):
        return {"my_seed.csv": files.MY_SEED}

    @pytest.fixture(scope="class", autouse=True)
    def models(self):
        yield {
            "my_table.sql": files.MY_TABLE,
            "my_view.sql": files.MY_VIEW,
            # "my_materialized_view.sql": files.MY_MATERIALIZED_VIEW,
        }

    @pytest.fixture(scope="class", autouse=True)
    def docs(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])
        yield run_dbt(["docs", "generate"])

    @pytest.mark.parametrize(
        "node_name,relation_type",
        [
            # TODO: singlestore__get_catalog_tables_sql what was the goal to returning then lowercase? Updated the test here: BASE TABLE -> table, VIEW -> view
            ("seed.test.my_seed", "table"),
            ("model.test.my_table", "table"),
            ("model.test.my_view", "view"),
            # ("model.test.my_materialized_view", "MATERIALIZED VIEW"),
        ],
    )
    def test_relation_types_populate_correctly(
        self, docs: CatalogArtifact, node_name: str, relation_type: str
    ):
        assert node_name in docs.nodes
        node = docs.nodes[node_name]
        assert node.metadata.type == relation_type

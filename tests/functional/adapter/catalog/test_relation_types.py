# Copyright 2021-2026 SingleStore, Inc.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

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

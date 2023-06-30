import pytest
from dbt.tests.util import run_dbt, check_relations_equal
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
    ResultHolder)


# we don't support custom schema in models in a way dbt expects,
# that's why we updated the statement that is called to get the row_count_query value
class SingleStoreBaseIncrementalPredicates:
    def get_test_fields(
            self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    ):

        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
        # pass on kwarg
        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = "select * from {}".format(seed)
        # project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
        )


class TestIncrementalPredicatesDeleteInsert(SingleStoreBaseIncrementalPredicates, BaseIncrementalPredicates):
    pass


class TestPredicatesDeleteInsert(SingleStoreBaseIncrementalPredicates, BaseIncrementalPredicates):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+predicates": [
                    "id != 2"
                ],
                "+incremental_strategy": "delete+insert"
            }
        }


from pathlib import Path
import pytest

from dbt.tests.util import run_dbt, check_relations_equal
from dbt.tests.adapter.incremental.test_incremental_predicates import (
    BaseIncrementalPredicates,
    ResultHolder)
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import BaseIncrementalOnSchemaChange
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey

from fixture_incremental import (
    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
    _MODELS__INCREMENTAL_IGNORE,
    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
    _MODELS__INCREMENTAL_IGNORE_TARGET,
    _MODELS__INCREMENTAL_FAIL,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
    _MODELS__A,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
)


class SingleStoreIncrementalOnSchemaChangeSetup:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_sync_remove_only.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
            "incremental_ignore.sql": _MODELS__INCREMENTAL_IGNORE,
            "incremental_sync_remove_only_target.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
            "incremental_ignore_target.sql": _MODELS__INCREMENTAL_IGNORE_TARGET,
            "incremental_fail.sql": _MODELS__INCREMENTAL_FAIL,
            "incremental_sync_all_columns.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
            "incremental_append_new_columns_remove_one.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
            "model_a.sql": _MODELS__A,
            "incremental_append_new_columns_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
            "incremental_append_new_columns.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
            "incremental_sync_all_columns_target.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
            "incremental_append_new_columns_remove_one_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
        }


class TestIncrementalOnSchemaChange(SingleStoreIncrementalOnSchemaChangeSetup, BaseIncrementalOnSchemaChange):
    pass


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


seeds__duplicate_insert_sql = """
-- Insert statement which when applied to seed.csv triggers the inplace
--   overwrite strategy of incremental models. Seed and incremental model
--   diverge.

-- insert new row, which should not be in incremental model
--  with primary or first three columns unique
insert into seed
    (state, county, city, last_visit_date)
values ('CT','Hartford','Hartford','2022-02-14');

"""

seeds__seed_csv = """state,county,city,last_visit_date
CT,Hartford,Hartford,2020-09-23
MA,Suffolk,Boston,2020-02-12
NJ,Mercer,Trenton,2022-01-01
NY,Kings,Brooklyn,2021-04-02
NY,New York,Manhattan,2021-04-01
PA,Philadelphia,Philadelphia,2021-05-21
CO,Denver,,2021-06-18
"""

seeds__add_new_rows_sql = """
-- Insert statement which when applied to seed.csv sees incremental model
--   grow in size while not (necessarily) diverging from the seed itself.

-- insert two new rows, both of which should be in incremental model
--   with any unique columns
insert into seed
    (state, county, city, last_visit_date)
values ('WA','King','Seattle','2022-02-01');

insert into seed
    (state, county, city, last_visit_date)
values ('CA','Los Angeles','Los Angeles','2022-02-01');

"""


class TestIncrementalUniqueKey(BaseIncrementalUniqueKey):
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "duplicate_insert.sql": seeds__duplicate_insert_sql,
            "seed.csv": seeds__seed_csv,
            "add_new_rows.sql": seeds__add_new_rows_sql,
        }

    def get_test_fields(
        self, project, seed, incremental_model, update_sql_file, opt_model_count=None
    ):
        """build a test case and return values for assertions
        [INFO] Models must be in place to test incremental model
        construction and merge behavior. Database touches are side
        effects to extract counts (which speak to health of unique keys)."""
        # idempotently create some number of seeds and incremental models'''

        seed_count = len(run_dbt(["seed", "--select", seed, "--full-refresh"]))

        model_count = len(run_dbt(["run", "--select", incremental_model, "--full-refresh"]))
        # pass on kwarg
        relation = incremental_model
        # update seed in anticipation of incremental model update
        row_count_query = "select * from {}".format(seed)
        project.run_sql_file(Path("seeds") / Path(update_sql_file + ".sql"))
        seed_rows = len(project.run_sql(row_count_query, fetch="all"))

        # propagate seed state to incremental model according to unique keys
        inc_test_model_count = self.update_incremental_model(incremental_model=incremental_model)

        return ResultHolder(
            seed_count, model_count, seed_rows, inc_test_model_count, opt_model_count, relation
        )
    pass

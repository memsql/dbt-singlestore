import datetime
import importlib
import pytest

from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSnapshotCheck, BaseSimpleSnapshot
from dbt.tests.adapter.simple_snapshot.test_ephemeral_snapshot_hard_deletes import BaseSnapshotEphemeralHardDeletes
from dbt.tests.adapter.simple_snapshot.test_various_configs import (
    BaseSnapshotDbtValidToCurrent,
    BaseSnapshotColumnNamesFromDbtProject,
    BaseSnapshotColumnNames,
    BaseSnapshotInvalidColumnNames,
    BaseSnapshotMultiUniqueKey,
)
from dbt.tests.util import (
    run_dbt,
    run_sql_with_adapter,
    check_relations_equal,
)
from fixture_snapshots import (
    create_seed_sql,
    create_snapshot_expected_sql,
    seed_insert_sql,
    populate_snapshot_expected_valid_to_current_sql,
    populate_snapshot_expected_sql,
    snapshot_actual_sql,
    snapshots_valid_to_current_yml,
    invalidate_sql,
    update_sql,
    update_with_current_sql,
    model_seed_sql,
    create_multi_key_seed_sql,
    create_multi_key_snapshot_expected_sql,
    seed_multi_key_insert_sql,
    populate_multi_key_snapshot_expected_sql,
    invalidate_multi_key_sql,
    update_multi_key_sql,
)


class TestSnapshot(BaseSimpleSnapshot):
    def test_updates_are_captured_by_snapshot(self, project):
        """
        Update the last 5 records. Show that all ids are current, but the last 5 reflect updates.
        """
        self.update_fact_records(
            {"updated_at": "updated_at + interval 1 day"}, "id between 16 and 20"
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(16, 21),
        )

    def test_new_column_captured_by_snapshot(self, project):
        """
        Add a column to `fact` and populate the last 10 records with a non-null value.
        Show that all ids are current, but the last 10 reflect updates and the first 10 don't
        i.e. if the column is added, but not updated, the record doesn't reflect that it's updated
        """
        self.add_fact_column("full_name", "varchar(200) default null")
        self.update_fact_records(
            {
                "full_name": "concat(first_name, ' ', last_name)",
                "updated_at": "updated_at + interval 1 day",
            },
            "id between 11 and 20",
        )
        run_dbt(["snapshot"])
        self._assert_results(
            ids_with_current_snapshot_records=range(1, 21),
            ids_with_closed_out_snapshot_records=range(11, 21),
        )
    pass


class TestSnapshotCheck(BaseSnapshotCheck):
    pass


# Source table creation statement
_source_create_sql = """
create table {database}.src_customers (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    updated_at TIMESTAMP
);
"""

# Initial data for source table
_source_insert_sql = """
insert into {database}.src_customers (id, first_name, last_name, email, updated_at) values
(1, 'John', 'Doe', 'john.doe@example.com', '2023-01-01 10:00:00'),
(2, 'Jane', 'Smith', 'jane.smith@example.com', '2023-01-02 11:00:00'),
(3, 'Bob', 'Johnson', 'bob.johnson@example.com', '2023-01-03 12:00:00');
"""

# SQL to add a dummy column to source table (simulating schema change)
_source_alter_sql = """
alter table {database}.src_customers add column dummy_column VARCHAR(50) default 'dummy_value';
"""


class TestSnapshotEphemeralHardDeletes(BaseSnapshotEphemeralHardDeletes):
    @pytest.fixture(scope="class")
    def source_create_sql(self):
        return _source_create_sql

    @pytest.fixture(scope="class")
    def source_insert_sql(self):
        return _source_insert_sql

    @pytest.fixture(scope="class")
    def source_alter_sql(self):
        return _source_alter_sql
    pass


class TestSnapshotColumnNamesFromDbtProject(BaseSnapshotColumnNamesFromDbtProject):
    @pytest.fixture(autouse=True, scope="class")
    def _patch_sql_globals(self, request):
        base_mod = importlib.import_module(BaseSnapshotColumnNames.__module__)

        mp = pytest.MonkeyPatch()
        mp.setattr(base_mod, "snapshot_actual_sql", snapshot_actual_sql, raising=False)
        mp.setattr(base_mod, "create_seed_sql", create_seed_sql, raising=False)
        mp.setattr(base_mod, "create_snapshot_expected_sql", create_snapshot_expected_sql, raising=False)
        mp.setattr(base_mod, "seed_insert_sql", seed_insert_sql, raising=False)
        mp.setattr(base_mod, "populate_snapshot_expected_sql", populate_snapshot_expected_sql, raising=False)
        mp.setattr(base_mod, "invalidate_sql", invalidate_sql, raising=False)
        mp.setattr(base_mod, "update_sql", update_sql, raising=False)
        request.addfinalizer(mp.undo) 
    pass


class TestSnapshotColumnNames(BaseSnapshotColumnNames):
    @pytest.fixture(autouse=True, scope="class")
    def _patch_sql_globals(self, request):
        base_mod = importlib.import_module(BaseSnapshotColumnNames.__module__)

        mp = pytest.MonkeyPatch()
        mp.setattr(base_mod, "snapshot_actual_sql", snapshot_actual_sql, raising=False)
        mp.setattr(base_mod, "create_seed_sql", create_seed_sql, raising=False)
        mp.setattr(base_mod, "create_snapshot_expected_sql", create_snapshot_expected_sql, raising=False)
        mp.setattr(base_mod, "seed_insert_sql", seed_insert_sql, raising=False)
        mp.setattr(base_mod, "populate_snapshot_expected_sql", populate_snapshot_expected_sql, raising=False)
        mp.setattr(base_mod, "invalidate_sql", invalidate_sql, raising=False)
        mp.setattr(base_mod, "update_sql", update_sql, raising=False)
        request.addfinalizer(mp.undo) 
    pass


class TestSnapshotInvalidColumnNames(BaseSnapshotInvalidColumnNames):
    @pytest.fixture(autouse=True, scope="class")
    def _patch_sql_globals(self, request):
        base_mod = importlib.import_module(BaseSnapshotColumnNames.__module__)

        mp = pytest.MonkeyPatch()
        mp.setattr(base_mod, "snapshot_actual_sql", snapshot_actual_sql, raising=False)
        mp.setattr(base_mod, "create_seed_sql", create_seed_sql, raising=False)
        mp.setattr(base_mod, "create_snapshot_expected_sql", create_snapshot_expected_sql, raising=False)
        mp.setattr(base_mod, "seed_insert_sql", seed_insert_sql, raising=False)
        mp.setattr(base_mod, "populate_snapshot_expected_sql", populate_snapshot_expected_sql, raising=False)
        mp.setattr(base_mod, "invalidate_sql", invalidate_sql, raising=False)
        mp.setattr(base_mod, "update_sql", update_sql, raising=False)
        request.addfinalizer(mp.undo) 
    pass


'''
class TestSnapshotDbtValidToCurrent(BaseSnapshotDbtValidToCurrent):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": snapshots_valid_to_current_yml,
            "ref_snapshot.sql": ref_snapshot_sql,
        }

    def test_valid_to_current(self, project):
        project.run_sql(create_seed_sql)
        project.run_sql(create_snapshot_expected_sql)
        project.run_sql(seed_insert_sql)
        project.run_sql(populate_snapshot_expected_valid_to_current_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        original_snapshot = run_sql_with_adapter(
            project.adapter,
            "select id, test_scd_id, test_valid_to from snapshot_actual",
            "all",
        )
        assert original_snapshot[0][2] == datetime.datetime(2099, 12, 31, 0, 0)
        assert original_snapshot[9][2] == datetime.datetime(2099, 12, 31, 0, 0)

        project.run_sql(invalidate_sql)
        project.run_sql(update_with_current_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        updated_snapshot = run_sql_with_adapter(
            project.adapter,
            "select id, test_scd_id, test_valid_to from snapshot_actual",
            "all",
        )
        assert updated_snapshot[0][2] == datetime.datetime(2099, 12, 31, 0, 0)
        # Original row that was updated now has a non-current (2099/12/31) date
        assert updated_snapshot[9][2] == datetime.datetime(2016, 8, 20, 16, 44, 49)
        # Updated row has a current date
        assert updated_snapshot[20][2] == datetime.datetime(2099, 12, 31, 0, 0)

        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])
    pass'''


class TestSnapshotMultiUniqueKey(BaseSnapshotMultiUniqueKey):
    @pytest.fixture(autouse=True, scope="class")
    def _patch_sql_globals(self, request):
        base_mod = importlib.import_module(BaseSnapshotColumnNames.__module__)

        mp = pytest.MonkeyPatch()
        mp.setattr(base_mod, "model_seed_sql", model_seed_sql, raising=False)
        mp.setattr(base_mod, "create_multi_key_seed_sql", create_multi_key_seed_sql, raising=False)
        mp.setattr(base_mod, "create_multi_key_snapshot_expected_sql", create_multi_key_snapshot_expected_sql, raising=False)
        mp.setattr(base_mod, "seed_multi_key_insert_sql", seed_multi_key_insert_sql, raising=False)
        mp.setattr(base_mod, "populate_multi_key_snapshot_expected_sql", populate_multi_key_snapshot_expected_sql, raising=False)
        mp.setattr(base_mod, "invalidate_multi_key_sql", invalidate_multi_key_sql, raising=False)
        mp.setattr(base_mod, "update_multi_key_sql", update_multi_key_sql, raising=False)
        request.addfinalizer(mp.undo) 
    pass
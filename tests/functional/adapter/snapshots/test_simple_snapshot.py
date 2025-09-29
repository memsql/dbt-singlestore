import datetime

import pytest

from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSnapshotCheck, BaseSimpleSnapshot
from dbt.tests.adapter.simple_snapshot.test_various_configs import BaseSnapshotDbtValidToCurrent
from dbt.tests.adapter.simple_snapshot.fixtures import (
    ref_snapshot_sql,
)
from dbt.tests.util import (
    run_dbt,
    run_sql_with_adapter,
)
from fixture_snapshots import (
    create_seed_sql,
    create_snapshot_expected_sql,
    seed_insert_sql,
    populate_snapshot_expected_valid_to_current_sql,
    snapshot_actual_sql,
    snapshots_valid_to_current_yml,
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
    pass
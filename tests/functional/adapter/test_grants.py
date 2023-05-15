import pytest

from dbt.tests.util import (
    get_connection,
    get_manifest,
    relation_from_name,
    run_dbt,
    run_dbt_and_capture,
    write_file,
)

from dbt.tests.adapter.grants.test_model_grants import BaseModelGrants
from dbt.tests.adapter.grants.test_incremental_grants import (
    BaseIncrementalGrants,
    user2_incremental_model_schema_yml
)
from dbt.tests.adapter.grants.test_invalid_grants import (
    BaseInvalidGrants,
    invalid_user_table_model_schema_yml,
    invalid_privilege_table_model_schema_yml
)
from dbt.tests.adapter.grants.test_seed_grants import BaseSeedGrants
from dbt.tests.adapter.grants.test_snapshot_grants import (
    BaseSnapshotGrants,
    snapshot_schema_yml
)


class TestModelGrants(BaseModelGrants):
    pass


class TestIncrementalGrants(BaseIncrementalGrants):
    def test_incremental_grants(self, project, get_test_users):
        # we want the test to fail, not silently skip
        test_users = get_test_users
        select_privilege_name = self.privilege_grantee_name_overrides()["select"]
        assert len(test_users) == 3

        # Incremental materialization, single select grant
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_incremental_model"
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[0]]}
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Incremental materialization, run again without changes
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " not in log_output
        assert "grant " not in log_output  # with space to disambiguate from 'show grants'
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Incremental materialization, change select grant user
        updated_yaml = self.interpolate_name_overrides(user2_incremental_model_schema_yml)
        write_file(updated_yaml, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " in log_output
        manifest = get_manifest(project.project_root)
        model = manifest.nodes[model_id]
        assert model.config.materialized == "incremental"
        expected = {select_privilege_name: [test_users[1]]}
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Incremental materialization, same config, now with --full-refresh
        run_dbt(["--debug", "run", "--full-refresh"])
        assert len(results) == 1
        # whether grants or revokes happened will vary by adapter
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)

        # Now drop the schema (with the table in it)
        adapter = project.adapter
        relation = relation_from_name(adapter, "my_incremental_model")
        with get_connection(adapter):
            adapter.drop_schema(relation)

        # Incremental materialization, same config, rebuild now that table is missing
        (results, log_output) = run_dbt_and_capture(["--debug", "run"])
        assert len(results) == 1
        assert "revoke " not in log_output
        self.assert_expected_grants_match_actual(project, "my_incremental_model", expected)
    pass


class TestSeedGrants(BaseSeedGrants):
    pass


my_snapshot_sql = """
{% snapshot my_snapshot %}
    {{ config(
        check_cols='all', unique_key='id', strategy='check',
        target_database=database, target_schema=schema
    ) }}
    select 1 as id, 'blue' :> {{ type_string() }} as color
{% endsnapshot %}
""".strip()


class TestSnapshotGrants(BaseSnapshotGrants):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "my_snapshot.sql": my_snapshot_sql,
            "schema.yml": self.interpolate_name_overrides(snapshot_schema_yml),
        }
    pass


class TestInvalidGrants(BaseInvalidGrants):
    # SingleStore creates the user and sets a warning:
    #   Creation of users via GRANT is deprecated. Use CREATE USER.
    #   In future versions, the NO_AUTO_CREATE_USER flag will be enabled by default
    def grantee_does_not_exist_error(self):
        return ""

    def privilege_does_not_exist_error(self):
        return "You have an error in your SQL syntax"

    def test_invalid_grants(self, project, get_test_users, logs_dir):
        # failure when grant to a user/role that doesn't exist
        yaml_file = self.interpolate_name_overrides(invalid_user_table_model_schema_yml)
        write_file(yaml_file, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"], expect_pass=True)
        assert self.grantee_does_not_exist_error() in log_output

        # failure when grant to a privilege that doesn't exist
        yaml_file = self.interpolate_name_overrides(invalid_privilege_table_model_schema_yml)
        write_file(yaml_file, project.project_root, "models", "schema.yml")
        (results, log_output) = run_dbt_and_capture(["--debug", "run"], expect_pass=False)
        assert self.privilege_does_not_exist_error() in log_output
    pass

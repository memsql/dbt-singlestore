import importlib
import pytest

from dbt.tests.adapter.simple_snapshot.new_record_dbt_valid_to_current import (
    BaseSnapshotNewRecordDbtValidToCurrent
)

from dbt.tests.util import run_dbt
from tests.utils.sql_patch_helpers import SqlGlobalOverrideMixin


_seed_new_record_mode_statements = [
    "create table {database}.seed (id INTEGER, first_name VARCHAR(50));",
    "insert into {database}.seed (id, first_name) values (1, 'Judith'), (2, 'Arthur');",
]

_snapshot_actual_sql = """
{% snapshot snapshot_actual %}
    select * from {{target.database}}.seed
{% endsnapshot %}
"""

_delete_sql = """
delete from {database}.seed where id = 1
"""

# If the deletion worked correctly, this should return one row (and not more) where dbt_is_deleted is True
_delete_check_sql = """
select dbt_scd_id from snapshot_actual where id = 1 and dbt_is_deleted = 'True'
"""


class TestSnapshotNewRecordDbtValidToCurrent(SqlGlobalOverrideMixin, BaseSnapshotNewRecordDbtValidToCurrent):
    BASE_TEST_CLASS = BaseSnapshotNewRecordDbtValidToCurrent
    SQL_GLOBAL_OVERRIDES = {
        "_seed_new_record_mode_statements": _seed_new_record_mode_statements,
        "_snapshot_actual_sql": _snapshot_actual_sql,
        "_delete_sql": _delete_sql,
        "_delete_check_sql": _delete_check_sql,
    }
    pass


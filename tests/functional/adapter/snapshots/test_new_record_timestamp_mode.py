import pytest

from dbt.tests.util import check_relations_equal, run_dbt
from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import BaseSnapshotNewRecordTimestampMode
from tests.utils.sql_patch_helpers import SqlGlobalOverrideMixin


_seed_new_record_mode_statements = [
    """create table {database}.seed (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),
    updated_at TIMESTAMP);""",
    """create table {database}.snapshot_expected (
    id INTEGER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(50),
    gender VARCHAR(50),
    ip_address VARCHAR(20),

    -- snapshotting fields
    updated_at TIMESTAMP,
    dbt_valid_from TIMESTAMP,
    dbt_valid_to   TIMESTAMP,
    dbt_scd_id     TEXT,
    dbt_updated_at TIMESTAMP,
    dbt_is_deleted TEXT
    );""",
    # seed inserts
    #  use the same email for two users to verify that duplicated check_cols values
    # are handled appropriately
    """insert into {database}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', '2015-12-24 12:19:28'),
(2, 'Arthur', 'Kelly', '(not provided)', 'Male', '62.56.24.215', '2015-10-28 16:22:15'),
(3, 'Rachel', 'Moreno', 'rmoreno2@msu.edu', 'Female', '31.222.249.23', '2016-04-05 02:05:30'),
(4, 'Ralph', 'Turner', 'rturner3@hp.com', 'Male', '157.83.76.114', '2016-08-08 00:06:51'),
(5, 'Laura', 'Gonzales', 'lgonzales4@howstuffworks.com', 'Female', '30.54.105.168', '2016-09-01 08:25:38'),
(6, 'Katherine', 'Lopez', 'klopez5@yahoo.co.jp', 'Female', '169.138.46.89', '2016-08-30 18:52:11'),
(7, 'Jeremy', 'Hamilton', 'jhamilton6@mozilla.org', 'Male', '231.189.13.133', '2016-07-17 02:09:46'),
(8, 'Heather', 'Rose', 'hrose7@goodreads.com', 'Female', '87.165.201.65', '2015-12-29 22:03:56'),
(9, 'Gregory', 'Kelly', 'gkelly8@trellian.com', 'Male', '154.209.99.7', '2016-03-24 21:18:16'),
(10, 'Rachel', 'Lopez', 'rlopez9@themeforest.net', 'Female', '237.165.82.71', '2016-08-20 15:44:49'),
(11, 'Donna', 'Welch', 'dwelcha@shutterfly.com', 'Female', '103.33.110.138', '2016-02-27 01:41:48'),
(12, 'Russell', 'Lawrence', 'rlawrenceb@qq.com', 'Male', '189.115.73.4', '2016-06-11 03:07:09'),
(13, 'Michelle', 'Montgomery', 'mmontgomeryc@scientificamerican.com', 'Female', '243.220.95.82', '2016-06-18 16:27:19'),
(14, 'Walter', 'Castillo', 'wcastillod@pagesperso-orange.fr', 'Male', '71.159.238.196', '2016-10-06 01:55:44'),
(15, 'Robin', 'Mills', 'rmillse@vkontakte.ru', 'Female', '172.190.5.50', '2016-10-31 11:41:21'),
(16, 'Raymond', 'Holmes', 'rholmesf@usgs.gov', 'Male', '148.153.166.95', '2016-10-03 08:16:38'),
(17, 'Gary', 'Bishop', 'gbishopg@plala.or.jp', 'Male', '161.108.182.13', '2016-08-29 19:35:20'),
(18, 'Anna', 'Riley', 'arileyh@nasa.gov', 'Female', '253.31.108.22', '2015-12-11 04:34:27'),
(19, 'Sarah', 'Knight', 'sknighti@foxnews.com', 'Female', '222.220.3.177', '2016-09-26 00:49:06'),
(20, 'Phyllis', 'Fox', null, 'Female', '163.191.232.95', '2016-08-21 10:35:19');""",
    # populate snapshot table
    """insert into {database}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id,
    dbt_is_deleted
)
select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    NULL as dbt_valid_to,
    updated_at as dbt_updated_at,
    MD5(CONCAT(id, '-', first_name, '|', updated_at)) as dbt_scd_id,
    'False' as dbt_is_deleted
from {database}.seed;""",
]

_snapshot_actual_sql = """
{% snapshot snapshot_actual %}

    {{
        config(
            unique_key="concat(id, '-', first_name)",
        )
    }}

    select * from {{target.database}}.seed

{% endsnapshot %}
"""

_invalidate_sql_statements = [
    """-- Update records 11 - 21. Change email and updated_at field.
update seed set
    updated_at = DATE_ADD(updated_at, INTERVAL 1 HOUR),
    email      =  case
                    when id = 20 then 'pfoxj@creativecommons.org'
                    else CONCAT('new_', email)
                  end
where id between 10 and 20;""",
    """-- Update the expected snapshot data to reflect the changes we expect to the snapshot on the next run
update snapshot_expected set
    dbt_valid_to   = DATE_ADD(updated_at, INTERVAL 1 HOUR)
where id between 10 and 20;
""",
]

_update_sql = """
-- insert v2 of the 11 - 21 records

insert into {database}.snapshot_expected (
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    dbt_valid_from,
    dbt_valid_to,
    dbt_updated_at,
    dbt_scd_id,
    dbt_is_deleted
)

select
    id,
    first_name,
    last_name,
    email,
    gender,
    ip_address,
    updated_at,
    -- fields added by snapshotting
    updated_at as dbt_valid_from,
    null as dbt_valid_to,
    updated_at as dbt_updated_at,
    MD5(CONCAT(id, '-', first_name, '|', updated_at)) as dbt_scd_id,
    'False' as dbt_is_deleted
from {database}.seed
where id between 10 and 20;
"""

# SQL to delete a record from the snapshot source data
_delete_sql = """
delete from {database}.seed where id = 1
"""

# If the deletion worked correctly, this should return two rows, with one of them representing the deletion.
_delete_check_sql = """
select dbt_valid_to, dbt_scd_id, dbt_is_deleted from snapshot_actual where id = 1
"""

# SQL to re-insert the deleted record from the snapshot source data
_reinsert_sql = """
insert into {database}.seed (id, first_name, last_name, email, gender, ip_address, updated_at) values
(1, 'Judith', 'Kennedy', '(not provided)', 'Female', '54.60.24.128', '2200-01-01 12:00:00');
"""

# If the re-insertion worked correctly, this should return three rows:
#   - one for the original record
#   - one for the deletion interval
#   - one for the new record
_reinsert_check_sql = """
select dbt_valid_from, dbt_valid_to, dbt_scd_id, dbt_is_deleted from snapshot_actual where id = 1;
"""


class TestSnapshotNewRecordTimestampMode(SqlGlobalOverrideMixin, BaseSnapshotNewRecordTimestampMode):
    BASE_TEST_CLASS = BaseSnapshotNewRecordTimestampMode
    SQL_GLOBAL_OVERRIDES = {
        "_snapshot_actual_sql": _snapshot_actual_sql,
        "_seed_new_record_mode_statements": _seed_new_record_mode_statements,
        "_invalidate_sql_statements": _invalidate_sql_statements,
        "_update_sql": _update_sql,
        "_delete_sql": _delete_sql,
        "_delete_check_sql": _delete_check_sql,
        "_reinsert_sql": _reinsert_sql,
        "_reinsert_check_sql": _reinsert_check_sql,
    }
    pass


_snapshots_check_yml = """
snapshots:
  - name: snapshot_actual
    config:
      strategy: check
      check_cols: all
      hard_deletes: new_record
"""

_ref_snapshot_check_sql = """
select * from {{ ref('snapshot_actual') }}
"""


class TestSnapshotNewRecordCheckMode(TestSnapshotNewRecordTimestampMode):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _snapshots_check_yml,
            "ref_snapshot.sql": _ref_snapshot_check_sql,
        }
    pass

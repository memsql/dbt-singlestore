from dbt.tests.adapter.basic.test_base import BaseSimpleMaterializations
from dbt.tests.adapter.basic.test_empty import BaseEmpty
from dbt.tests.adapter.basic.test_ephemeral import BaseEphemeral
from dbt.tests.adapter.basic.test_generic_tests import BaseGenericTests
from dbt.tests.adapter.basic.test_incremental import BaseIncremental, BaseIncrementalNotSchemaChange
from dbt.tests.adapter.basic.test_singular_tests import BaseSingularTests
from dbt.tests.adapter.basic.test_singular_tests_ephemeral import BaseSingularTestsEphemeral
from dbt.tests.adapter.basic.test_snapshot_check_cols import BaseSnapshotCheckCols
from dbt.tests.adapter.basic.test_snapshot_timestamp import BaseSnapshotTimestamp
from dbt.tests.adapter.basic.test_table_materialization import BaseTableMaterialization
from dbt.tests.adapter.basic.test_validate_connection import BaseValidateConnection
from tests.utils.sql_patch_helpers import SqlGlobalOverrideMixin


class TestSimpleMaterializationsSingleStore(BaseSimpleMaterializations):
    pass


class TestEmptySingleStore(BaseEmpty):
    pass


class TestEphemeralSingleStore(BaseEphemeral):
    pass


class TestGenericTestsSingleStore(BaseGenericTests):
    pass


class TestIncrementalSingleStore(BaseIncremental):
    pass


class TestBaseIncrementalNotSchemaChangeSingleStore(BaseIncrementalNotSchemaChange):
    pass


class TestSingularTestsSingleStore(BaseSingularTests):
    pass


class TestSingularTestsEphemeralSingleStore(BaseSingularTestsEphemeral):
    pass


class TestSnapshotCheckColsSingleStore(BaseSnapshotCheckCols):
    pass


class TestSnapshotTimestampSingleStore(BaseSnapshotTimestamp):
    pass


model_sql = """
{{
  config(
    materialized = "table",
    sort = 'first_name',
    dist = 'first_name'
  )
}}

select * from seed
"""


class TestTableMaterializationSingleStore(SqlGlobalOverrideMixin, BaseTableMaterialization):
    BASE_TEST_CLASS = BaseTableMaterialization
    SQL_GLOBAL_OVERRIDES = {
        "model_sql": model_sql,
    }
    pass


class TestValidateConnectionSingleStore(BaseValidateConnection):
    pass


from dbt.tests.adapter.simple_snapshot.test_snapshot import BaseSnapshotCheck, BaseSimpleSnapshot
from dbt.tests.adapter.simple_snapshot.test_various_configs import BaseSnapshotDbtValidToCurrent


class TestSnapshot(BaseSimpleSnapshot):
    pass


class TestSnapshotCheck(BaseSnapshotCheck):
    pass


class TestSnapshotDbtValidToCurrent(BaseSnapshotDbtValidToCurrent):
    pass
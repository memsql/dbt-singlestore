import pytest
from dbt.tests.adapter.sample_mode.test_sample_mode import BaseSampleModeTest


input_model_sql = """
{{ config(materialized='table', event_time='event_time') }}
select 1 as id, ('2025-01-01 01:25:00' :> TIMESTAMP) as event_time
UNION ALL
select 2 as id, ('2025-01-02 13:47:00' :> TIMESTAMP) as event_time
UNION ALL
select 3 as id, ('2025-01-03 01:32:00' :> TIMESTAMP) as event_time
"""


class TestSampleMode(BaseSampleModeTest):
    @pytest.fixture(scope="class")
    def input_model_sql(self) -> str:
        return input_model_sql
    pass

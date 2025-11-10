
import pytest
from dbt.tests.adapter.simple_seed.test_seed_type_override import BaseSimpleSeedColumnOverride
from dbt.tests.adapter.utils.base_utils import run_dbt


_SCHEMA_YML = """
version: 2
seeds:
- name: seed_enabled
  columns:
  - name: birthday
    data_tests:
    - column_type:
        type: date
  - name: seed_id
    data_tests:
    - column_type:
        type: character varying(256)

- name: seed_tricky
  columns:
  - name: seed_id
    data_tests:
    - column_type:
        type: int(11)
  - name: seed_id_str
    data_tests:
    - column_type:
        type: character varying(256)
  - name: a_bool
    data_tests:
    - column_type:
        type: tinyint(1)
  - name: looks_like_a_bool
    data_tests:
    - column_type:
        type: character varying(256)
  - name: a_date
    data_tests:
    - column_type:
        type: datetime(6)
  - name: looks_like_a_date
    data_tests:
    - column_type:
        type: character varying(256)
  - name: relative
    data_tests:
    - column_type:
        type: character varying(256)
  - name: weekday
    data_tests:
    - column_type:
        type: character varying(256)
""".lstrip()


class TestSimpleSeedColumnOverride(BaseSimpleSeedColumnOverride):
    @pytest.fixture(scope="class")
    def models(self):
        return {"models-singlestore.yml": _SCHEMA_YML}

    @staticmethod
    def seed_enabled_types():
        return {
            "seed_id": "TEXT",
            "birthday": "DATE",
        }

    @staticmethod
    def seed_tricky_types():
        return {
            "seed_id_str": "TEXT",
            "looks_like_a_bool": "TEXT",
            "looks_like_a_date": "TEXT",
        }

    def test_simple_seed_with_column_override_singlestore(self, project):
        seed_results = run_dbt(["seed", "--show"])
        assert len(seed_results) == 2
        test_results = run_dbt(["test"])
        assert len(test_results) == 10

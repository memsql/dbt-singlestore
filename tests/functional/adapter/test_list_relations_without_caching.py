import json

import pytest
from dbt.tests.util import run_dbt, run_dbt_and_capture

NUM_VIEWS = 10
NUM_EXPECTED_RELATIONS = 1 + NUM_VIEWS

TABLE_BASE_SQL = """
{{ config(materialized='table') }}

select 1 as id
""".lstrip()

VIEW_X_SQL = """
select id from {{ ref('my_model_base') }}
""".lstrip()

MACROS__VALIDATE__SINGLESTORE__LIST_RELATIONS_WITHOUT_CACHING = """
{% macro validate_list_relations_without_caching(schema_relation) -%}
    {% set relation_list_result = singlestore__list_relations_without_caching(schema_relation) %}
    {% set n_relations = relation_list_result | length %}
    {{ log("n_relations: " ~ n_relations) }}
{% endmacro %}
"""


def parse_json_logs(json_log_output):
    parsed_logs = []
    for line in json_log_output.split("\n"):
        try:
            log = json.loads(line)
        except ValueError:
            continue

        parsed_logs.append(log)

    return parsed_logs


def find_result_in_parsed_logs(parsed_logs, result_name):
    return next(
        (
            item["data"]["msg"]
            for item in parsed_logs
            if result_name in item["data"].get("msg", "msg")
        ),
        False,
    )


def find_exc_info_in_parsed_logs(parsed_logs, exc_info_name):
    return next(
        (
            item["data"]["exc_info"]
            for item in parsed_logs
            if exc_info_name in item["data"].get("exc_info", "exc_info")
        ),
        False,
    )

def get_tables_in_db(project):
    sql = """
            select table_name,
                    case when table_type = 'BASE TABLE' then 'table'
                            when table_type = 'VIEW' then 'view'
                            else table_type
                    end as materialization
            from information_schema.tables
            where {}
            order by table_name
            """
    sql = sql.format("{} like '{}'".format("table_schema", project.database))
    result = project.run_sql(sql, fetch="all")
    return {model_name: materialization for (model_name, materialization) in result}


class TestListRelationsWithoutCachingSingle:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE_BASE_SQL}
        for view in range(0, NUM_VIEWS):
            my_models.update({f"my_model_{view}.sql": VIEW_X_SQL})

        return my_models

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__SINGLESTORE__LIST_RELATIONS_WITHOUT_CACHING,
        }
    
    def test__singlestore__list_relations_without_caching(self, project):
        """
        validates that singlestore__list_relations_without_caching
        macro returns a single record
        """
        run_dbt(["run", "-s", "my_model_base"])

        _, log_output = run_dbt_and_capture(
            [
                "--debug",
                "--log-format=json",
                "run-operation",
                "validate_list_relations_without_caching",
                "--args",
                "{'schema_relation': ''}",
            ]
        )

        parsed_logs = parse_json_logs(log_output)
        n_relations = find_result_in_parsed_logs(parsed_logs, "n_relations")
        assert n_relations == "n_relations: 1"


class TestListRelationsWithoutCachingFull:
    @pytest.fixture(scope="class")
    def models(self):
        my_models = {"my_model_base.sql": TABLE_BASE_SQL}
        for view in range(0, NUM_VIEWS):
            my_models.update({f"my_model_{view}.sql": VIEW_X_SQL})

        return my_models

    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__SINGLESTORE__LIST_RELATIONS_WITHOUT_CACHING,
        }

    def test__singlestore__list_relations_without_caching(self, project):
        # purpose of the first run is to create the replicated views in the target schema
        run_dbt(["run"])

        _, log_output = run_dbt_and_capture(
            [
                "--debug",
                "--log-format=json",
                "run-operation",
                "validate_list_relations_without_caching",
                "--args",
                "{'schema_relation': ''}",
            ]
        )

        parsed_logs = parse_json_logs(log_output)
        n_relations = find_result_in_parsed_logs(parsed_logs, "n_relations")
        assert n_relations == f"n_relations: {NUM_EXPECTED_RELATIONS}"

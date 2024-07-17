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

# TODO - fix the call {% set relation_list_result = fabric__list_relations_without_caching(schema_relation) %}
MACROS__VALIDATE__FABRIC__LIST_RELATIONS_WITHOUT_CACHING = """
{% macro validate_list_relations_without_caching(schema_relation) -%}
    {{ log("schema_relation: ")}}
    {{ log(schema_relation) }}
    {{ log(schema_relation.database) }}
    {% if schema_relation.database is not none -%}
       {% set database = schema_relation.database -%}
    {% else -%}
       {% set query = 'select database()' -%}
       {% set result = run_query(query) -%}
       {% set database = result[0][0] -%}
    {% endif -%}
    {% call statement('list_relations_without_caching', fetch_result=True) -%}
        /* list_relations_without_caching for relation */
        /* database: {{ schema_relation.database }}, schema: {{ schema_relation.schema }}, name: {{ schema_relation.identifier }} */
        select
            table_schema as "database",
            table_name as "name",
            '{{ schema_relation.schema }}' as "schema",
            case when table_type = 'BASE TABLE' then 'table'
                 when table_type = 'VIEW' then 'view'
                 else table_type
            end as "table_type"
        from information_schema.tables
        where table_schema = '{{ database }}'
    {% endcall %}
    
    {% set relation_list_result = load_result('list_relations_without_caching').table %}
    {% set n_relations = relation_list_result | length %}
    {{ log(relation_list_result) }}
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
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__FABRIC__LIST_RELATIONS_WITHOUT_CACHING,
        }
    
    def test__fabric__list_relations_without_caching(self, project):
        """
        validates that fabric__list_relations_without_caching
        macro returns a single record
        """
        run_dbt(["run", "-s", "my_model_base"])

        database = project.database
        tables = get_tables_in_db(project)

        for table in tables:
            schema_relation = {
                "database": database,
                "schema": "",  # SingleStore does not use schemas, so this can be empty
                "identifier": table
            }
            kwargs = {"schema_relation": {"schema_relation": schema_relation}}
            _, log_output = run_dbt_and_capture(
                [
                    "--debug",
                    #"--log-format=json",
                    "run-operation",
                    "validate_list_relations_without_caching",
                    "--args",
                    str(kwargs),
                ]
            )

            parsed_logs = parse_json_logs(log_output)
            n_relations = find_result_in_parsed_logs(parsed_logs, "n_relations")
            import pdb
            pdb.set_trace()
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
            "validate_list_relations_without_caching.sql": MACROS__VALIDATE__FABRIC__LIST_RELATIONS_WITHOUT_CACHING,
        }

    def test__fabric__list_relations_without_caching(self, project):
        # purpose of the first run is to create the replicated views in the target schema
        run_dbt(["run"])

        database = project.database
        schemas = project.created_schemas

        for schema in schemas:
            schema_relation = f"{database}.{schema}"
            kwargs = {"schema_relation": schema_relation}
            _, log_output = run_dbt_and_capture(
                [
                    "--debug",
                    "--log-format=json",
                    "run-operation",
                    "validate_list_relations_without_caching",
                    "--args",
                    str(kwargs),
                ]
            )

            parsed_logs = parse_json_logs(log_output)
            n_relations = find_result_in_parsed_logs(parsed_logs, "n_relations")
            import pdb
            pdb.set_trace()
            assert n_relations == f"n_relations: {NUM_EXPECTED_RELATIONS}"

#!/usr/bin/env bash

source .env3/bin/activate
source ./.github/workflows/setup-cluster.sh start $CLUSTER_TYPE $SINGLESTORE_VERSION

export S2_PASSWORD=$SQL_USER_PASSWORD  # project UI env-var reference

export DBT_TEST_USER_1=user_1
export DBT_TEST_USER_2=user_2
export DBT_TEST_USER_3=user_3

drop_and_create_new_db()
{
  if [ "$CLUSTER_TYPE" = "ciab" ]; then
    mysql -u root -h 127.0.0.1 -P 3306 -p"${SQL_USER_PASSWORD}" --batch -N -e "DROP DATABASE IF EXISTS dbt_test; CREATE DATABASE dbt_test"
  else
    python ./.github/workflows/s2ms_cluster.py update dbt_test
  fi
}

# TODO: make it more beautiful?
pytest ./tests/functional/adapter/test_docs.py
drop_and_create_new_db
pytest -k TestSingleStoreMicrobatch
drop_and_create_new_db
pytest -k TestIncrementalConstraintsRollback
drop_and_create_new_db
pytest -k TestTableConstraintsRollback
drop_and_create_new_db
pytest ./tests/functional/adapter/test_caching.py
drop_and_create_new_db
pytest ./tests/functional/adapter/test_list_relations_without_caching.py
drop_and_create_new_db
pytest -k "not test_docs and not TestSingleStoreMicrobatch and not ConstraintsRollback and not test_caching and not test_list_relations_without_caching"
result_code=$?


./.github/workflows/setup-cluster.sh terminate $CLUSTER_TYPE
exit $result_code

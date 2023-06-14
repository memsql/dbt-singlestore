#!/usr/bin/env bash

source .env3/bin/activate
source ./.circleci/setup-cluster.sh start $CLUSTER_TYPE

export S2_PASSWORD=$SQL_USER_PASSWORD  # project UI env-var reference

export DBT_TEST_USER_1=user_1
export DBT_TEST_USER_2=user_2
export DBT_TEST_USER_3=user_3

pytest ./tests/functional/adapter/test_docs.py
if [ "$CLUSTER_TYPE" = "ciab" ]; then
  mysql -u root -h 127.0.0.1 -P 3306 -p"${SQL_USER_PASSWORD}" --batch -N -e "DROP DATABASE IF EXISTS dbt_test; CREATE DATABASE dbt_test"
else
  python ./.circleci/s2ms_cluster.py update dbt_test
fi
pytest -k "not test_docs"
result_code=$?

./.circleci/setup-cluster.sh terminate $CLUSTER_TYPE
exit $result_code

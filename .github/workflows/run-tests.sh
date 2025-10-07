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


TESTS=(
  "pytest -k TestSingleStoreMicrobatch"
  "pytest -k TestIncrementalConstraintsRollback"
  "pytest -k TestTableConstraintsRollback"
  "pytest -k TestSnapshotNewRecordTimestampMode"
  "pytest -k TestSnapshotNewRecordCheckMode"
  "pytest -k TestSnapshotColumnNames"
  "pytest -k TestSnapshotColumnNamesFromDbtProject"
  "pytest -k TestSnapshotInvalidColumnNames"
  "pytest -k TestSnapshotMultiUniqueKey"
  "pytest ./tests/functional/adapter/test_caching.py"
  "pytest ./tests/functional/adapter/test_docs.py"
  "pytest ./tests/functional/adapter/test_list_relations_without_caching.py"
  "pytest ./tests/functional/adapter/snapshot/test_snapshots.py"
  "pytest -k 'not TestSingleStoreMicrobatch and not ConstraintsRollback and not TestSnapshot
              and not test_caching and not test_docs and not test_list_relations_without_caching and not test_snapshots'"
)

for test in "${TESTS[@]}"; do
  drop_and_create_new_db
  eval "$test" || true
done

result_code=$?


./.github/workflows/setup-cluster.sh terminate $CLUSTER_TYPE
exit $result_code

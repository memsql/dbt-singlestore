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

# Run these suites separately to avoid table name collisions. SingleStore doesn't support schemas, and several tests share table names, so isolating them prevents cross-test interference.
TESTS=(
  "pytest -k TestSingleStoreMicrobatch"

  "pytest -k TestIncrementalConstraintsRollback"
  "pytest -k TestTableConstraintsRollback"

  "pytest -k TestSnapshotNewRecordTimestampMode"
  "pytest -k TestSnapshotNewRecordCheckMode"
  # Use nodeids (not `-k`) to avoid substring collisions: -k TestSnapshotColumnNames also matches `TestSnapshotColumnNamesFromDbtProject`
  "pytest ./tests/functional/adapter/snapshots/test_snapshots.py::TestSnapshotColumnNames"
  "pytest ./tests/functional/adapter/snapshots/test_snapshots.py::TestSnapshotColumnNamesFromDbtProject"
  "pytest -k TestSnapshotInvalidColumnNames"
  "pytest -k TestSnapshotMultiUniqueKey"
  "pytest -k TestSnapshotDbtValidToCurrent"
  "pytest -k TestSnapshotNewRecordDbtValidToCurrent"

  "pytest -k TestBasicSeedTests"
  "pytest -k TestSeedConfigFullRefreshOn"
  "pytest -k TestSeedConfigFullRefreshOff"
  "pytest -k TestSeedCustomSchema"
  "pytest -k TestSeedWithUniqueDelimiter"
  "pytest -k TestSeedWithWrongDelimiter"
  "pytest -k TestSeedWithEmptyDelimiter"
  "pytest -k TestSeedParsing"
  "pytest -k TestSimpleSeedEnabledViaConfig"
  "pytest -k TestSeedSpecificFormats"
  "pytest -k TestEmptySeed"
  "pytest -k TestSimpleSeedColumnOverride"


  "pytest ./tests/functional/adapter/test_caching.py"
  "pytest ./tests/functional/adapter/catalog/test_relation_types.py"
  "pytest ./tests/functional/adapter/test_docs.py"
  "pytest ./tests/functional/adapter/test_ephemeral.py"
  "pytest ./tests/functional/adapter/test_list_relations_without_caching.py"
  "pytest ./tests/functional/adapter/hooks/test_hooks.py"

  # Run the remainder of test_snapshots.py, excluding the suites that are run above
  "pytest ./tests/functional/adapter/snapshots/test_snapshots.py -k 'not TestSnapshotColumnNames and not TestSnapshotColumnNamesFromDbtProject and not TestSnapshotInvalidColumnNames and not TestSnapshotMultiUniqueKey and not TestSnapshotDbtValidToCurrent'"

  # Run everything else except what’s already run separately
  "pytest -k 'not TestSingleStoreMicrobatch and not ConstraintsRollback and not TestSnapshot
              and not test_caching and not test_relation_type and not test_docs and not test_ephemeral
              and not test_list_relations_without_caching and not test_snapshots and not test_hooks
              and not test_simple_seed'"
)

result_code=0

for test in "${TESTS[@]}"; do
  drop_and_create_new_db
  eval $test
  rc=$?
  if [ $rc -ne 0 ] && [ $result_code -eq 0 ]; then
    result_code=$rc
  fi
done

./.github/workflows/setup-cluster.sh terminate $CLUSTER_TYPE
exit $result_code

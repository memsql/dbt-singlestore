#!/usr/bin/env bash

source .env3/bin/activate
source ./.circleci/setup-cluster.sh start $CLUSTER_TYPE

export S2_PASSWORD=$SQL_USER_PASSWORD  # project UI env-var reference
pytest test/singlestore.dbtspec --no-drop-schema
result_code=$?

./.circleci/setup-cluster.sh terminate $CLUSTER_TYPE
exit $result_code

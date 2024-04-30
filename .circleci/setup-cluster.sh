#!/usr/bin/env bash
set -euo pipefail

start-s2ms-cluster() {
  python ./.circleci/s2ms_cluster.py start dbt_test
  export S2_HOST=$(cat WORKSPACE_ENDPOINT_FILE)
  export S2_PORT=3306
  export S2_USER=admin
}

terminate-s2ms-cluster() {
  python ./.circleci/s2ms_cluster.py terminate
}

start-cluster-in-a-box() {
  DEFAULT_IMAGE_NAME="singlestore/cluster-in-a-box:alma-8.1.1-45eec847e2-4.0.11-1.16.0"
  IMAGE_NAME="${SINGLESTORE_IMAGE:-$DEFAULT_IMAGE_NAME}"
  CONTAINER_NAME="singlestore-integration"
  EXTERNAL_MASTER_PORT=3306
  EXTERNAL_LEAF_PORT=3307

  EXISTS=$(docker inspect ${CONTAINER_NAME} >/dev/null 2>&1 && echo 1 || echo 0)

  if [[ "${EXISTS}" -eq 1 ]]; then
    EXISTING_IMAGE_NAME=$(docker inspect -f '{{.Config.Image}}' ${CONTAINER_NAME})
    if [[ "${IMAGE_NAME}" != "${EXISTING_IMAGE_NAME}" ]]; then
      echo "Existing container ${CONTAINER_NAME} has image ${EXISTING_IMAGE_NAME} when ${IMAGE_NAME} is expected; recreating container."
      docker rm -f ${CONTAINER_NAME}
      EXISTS=0
    fi
  fi

  if [[ "${EXISTS}" -eq 0 ]]; then
    docker run -i --init \
          --name ${CONTAINER_NAME} \
          -e LICENSE_KEY=${LICENSE_KEY} \
          -e ROOT_PASSWORD=${SQL_USER_PASSWORD} \
          -p $EXTERNAL_MASTER_PORT:3306 -p $EXTERNAL_LEAF_PORT:3307 \
          ${IMAGE_NAME}
  fi

  docker start ${CONTAINER_NAME}

  singlestore-wait-start() {
    echo -n "Waiting for SingleStore to start..."
    while true; do
      if mysql -u root -h 127.0.0.1 -P $EXTERNAL_MASTER_PORT -p"${SQL_USER_PASSWORD}" -e "select 1" >/dev/null 2>/dev/null; then
          break
      fi
      echo -n "."
      sleep 0.2
    done
    echo ". Success!"
  }

  singlestore-wait-start

  echo
  echo "Ensuring child nodes are connected using container IP"
  CONTAINER_IP=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' ${CONTAINER_NAME})
  CURRENT_LEAF_IP=$(mysql -u root -h 127.0.0.1 -P 3306 -p"${SQL_USER_PASSWORD}" --batch -N -e 'SELECT HOST FROM INFORMATION_SCHEMA.LEAVES')
  if [[ ${CONTAINER_IP} != "${CURRENT_LEAF_IP}" ]]; then
    # remove leaf with current ip
    mysql -u root -h 127.0.0.1 -P $EXTERNAL_MASTER_PORT -p"${SQL_USER_PASSWORD}" --batch -N -e "REMOVE LEAF '${CURRENT_LEAF_IP}':3307"
    # add leaf with correct ip
    mysql -u root -h 127.0.0.1 -P $EXTERNAL_MASTER_PORT -p"${SQL_USER_PASSWORD}" --batch -N -e "ADD LEAF root:'${SQL_USER_PASSWORD}'@'${CONTAINER_IP}':3307"
  fi

  mysql -u root -h 127.0.0.1 -P $EXTERNAL_MASTER_PORT -p"${SQL_USER_PASSWORD}" --batch -N -e "DROP DATABASE IF EXISTS dbt_test; CREATE DATABASE dbt_test"
  mysql -u root -h 127.0.0.1 -P $EXTERNAL_MASTER_PORT -p"${SQL_USER_PASSWORD}" --batch -N -e "CREATE USER user_1; CREATE USER user_2; CREATE USER user_3"

  export S2_HOST=127.0.0.1
  export S2_PORT=$EXTERNAL_MASTER_PORT
  export S2_USER=root
  echo "Done!"
}


if [ $1 == "start" ]
then
  if [ $2 == "ciab" ]
  then
    start-cluster-in-a-box
  else
    start-s2ms-cluster
  fi
else
  if [ $2 == "s2ms" ]
  then
    terminate-s2ms-cluster
  fi
fi

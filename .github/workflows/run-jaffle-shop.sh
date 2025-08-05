#!/usr/bin/env bash

source .env3/bin/activate
source ./.github/workflows/setup-cluster.sh start $CLUSTER_TYPE $CIAB_IMAGE

export S2_PASSWORD=$SQL_USER_PASSWORD  # project UI env-var reference

create_dbs() {
  if [ "$CLUSTER_TYPE" = "ciab" ]; then
    mysql -u root -h 127.0.0.1 -P 3306 -p"${SQL_USER_PASSWORD}" --batch -N -e "
      DROP DATABASE IF EXISTS jaffle_db;
      CREATE DATABASE jaffle_db;
      DROP DATABASE IF EXISTS dbt_custom_db;
      CREATE DATABASE dbt_custom_db;
    "
  else
    python ./.github/workflows/s2ms_cluster.py update jaffle_db
    python ./.github/workflows/s2ms_cluster.py update dbt_custom_db
  fi
}

create_dbs

# Set up dbt profile
mkdir -p ~/.dbt
cat > ~/.dbt/profiles.yml <<EOL
default:
  outputs:
    dev:
      type: singlestore
      host: 127.0.0.1
      port: 3306
      user: root
      password: ${SQL_USER_PASSWORD}
      database: testdb
  target: dev

test:
  outputs:
    dev:
      type: singlestore
      host: 127.0.0.1
      port: 3306
      user: root
      password: ${SQL_USER_PASSWORD}
      database: dbt_test
      schema: pm
  target: dev

jaffle_shop:
  outputs:
    dev:
      type: singlestore
      host: 127.0.0.1
      port: 3306
      user: root
      password: ${SQL_USER_PASSWORD}
      database: jaffle_db
      schema: pm_jaffle
  target: dev
EOL

cd jaffle_shop

dbt seed && dbt run && dbt snapshot && dbt test && dbt docs generate && dbt docs serve
result_code=$?

cd ..

./.github/workflows/setup-cluster.sh terminate $CLUSTER_TYPE
exit $result_code


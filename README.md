# dbt-singlestore

[dbt](https://www.getdbt.com/) adapter for [SingleStore](https://www.singlestore.com/).

## Installation

dbt-singlestore is available on PyPI. To install the latest version via pip, run:

```
pip install dbt-singlestore
```

## Configuring your profile

A sample profile can be found in
[sample_profiles.yml](dbt/include/singlestore/sample_profiles.yml). It is a standard profile for SQL data sources in dbt.
In SingleStore, like in MySQL, `database` and `schema` denote the same concept. In `dbt`, and also in a number of DBMS (e.g. Postgres) `schema` has a different meaning (a namespace within a database). Therefore, `schema` must be specified in dbt profile, but it won't affect database objects by default. However, it can be used as a prefix for table names, as outlined in [dbt docs for SingleStore profile](https://docs.getdbt.com/reference/warehouse-profiles/singlestore-profile).

## Supported Features

Category          | Feature           | Supported? | Tested? 
------------------|-------------------|------------|--------
Materializations  | Table             | Yes        | Yes
&nbsp;            | View              | Yes        | Yes
&nbsp;            | Incremental       | Yes        | Yes
&nbsp;            | Ephemeral         | Yes        | Yes
Resources         | Seeds             | Yes        | Yes
&nbsp;            | Sources           | Yes        | Yes
&nbsp;            | Testing - Bespoke | Yes        | Yes
&nbsp;            | Testing - Generic | Yes        | Yes
&nbsp;            | dbt Documentation | Yes        | Yes
&nbsp;            | Snapshots         | Yes        | Yes
Administration    | Hooks             | Yes        | Yes
Models            | Custom schema     | Limited*   | Yes

* Custom schemas can be defined in a project models, but they can only serve as a modification to the prefix of the corresponding table names.

* SSL connection would be automatically established by dbt-singlestore for SingleStore users created with 'Require SSL' flag.

## Testing and supported versions

Default dbt [test suite](tests/test_basic.py) is used to check the adapter functionality. [Development](Development.md) overview has a section "Run tests" which contains instructions on running the tests. Currently, the tests have been successfully run for the following product versions:

Singlestore | dbt-core | dbt-tests-adapter
-------------|-----------|-------------------
8.5.16       | 1.6.13    | 1.6.13
8.1.1        | 1.6.0     | 1.6.0
8.1.1        | 1.5.0     | 1.5.0
8.1.1        | 1.4.0     | 1.4.0
8.1.1        | 1.3.0     | 1.3.0
7.8.29       | 1.2.2     | 1.2.2

Singlestore | dbt-core | pytest-dbt-adapter
------------|----------|-------------------
7.8.3       | 1.1.1    | 0.6.0
7.6.6       | 1.0.1    | 0.6.0
7.5.12      | 1.0.1    | 0.6.0

To use this adapter, SingleStore must be upgraded to the version 7.5 or newer.

## Changelog

### 1.8.1
- Fix view definition retrieval in replace_view_definition macro

### 1.8.0
- Decoupled dependency on dbt-core
- dbt unit testing support

### 1.7.1
- Resolved issue with delete + insert incremental strategy

### 1.7
- Added metadata freshness check support
- Resolved issue with sort keys and rowstore temporary tables
- Added new tests

### 1.6.1
- Updated singlestoredb connector version

### 1.6
- Added new tests
- Added new incremental tests and `append` incremental strategy support

### 1.5
- Added `model contracts` support (`check` and `foreign` keys constraints aren't supported)
- Modified `create_table_as` to use column ordering subquery

### 1.4.1
- Switched from pymysql connector to singlestoredb
- Implemented `cancel()` function in SingleStoreConnectionManager class
- Added support of connection attributes

### 1.4
- Consolidated timestamp functions & macros
- Tested against Python 3.11
- Replaced deprecated exception functions
- Added support of the `delete+insert` incremental strategy + new tests for incremental predicates

### 1.3
- Added tests introduced in dbt 1.3.0

### 1.2.2
- Implemented connection retry logic
- Fixed index name possibly starting with 0 which is not allowed in SingleStore
- Added support for grants config
- Implemented utils macros and added tests

### 1.1.2
- [Fixed]((https://github.com/memsql/dbt-singlestore/issues/6)) failing materialization with `unique_key`.
- [Fixed](https://github.com/memsql/dbt-singlestore/issues/7) changing a column's data type during incremental materialization.
- [Added support](https://github.com/memsql/dbt-singlestore/issues/5) for `primary_key`, `sort_key`, `shard_key`, `unique_table_key`, `charset`, `collation`, `storage_type`, `indexes` options for creating SingleStore tables in `table` materialization.

## Contributors

We thank [Doug Beatty](https://github.com/dbeatty10) who build an adpater for mysql which has been used to build an initial version of this adapter.

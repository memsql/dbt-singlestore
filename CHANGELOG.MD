## Changelog

### 1.9.1
- Added extensive new adapter tests for hooks, catalogs, aliases, column types, query comments, simple seeds, and other core dbt behaviors

### 1.9.0
- Added microbatch incremental strategy support
- Introduced snapshot improvements:
    - Enable setting a datetime value for `dbt_valid_to` for current records instead of `NULL`
    - Enable `hard_deletes` config to add a metadata column if a record has been deleted
    - Allow customizing names of metadata fields
    - Enable `unique_key` as a list

### 1.8.3
- Resolved the issue with backup views

### 1.8.2
- Fixed logic and import in get_credentials function
- Fixed invalid imports

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
- [Added support](https://github.com/memsql/dbt-singlestore/issues/5) for `primary_key`, `sort_key`, `shard_key`, `unique_table_key`, `charset`, `collation`, `storage_type`, `indexes` options for creating SingleStore tables in `table` materialization
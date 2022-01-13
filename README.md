ee# dbt-singlestore

[dbt](https://www.getdbt.com/) adapter for [SingleStore](https://www.singlestore.com/).

<!-- ## Installation

dbt-singlestore is available on PyPI. To install the latest version via pip,
run:

```
pip install dbt-singlestore
``` -->

## Configuring your profile

A sample profile can be found in
[sample_profiles.yml](dbt/include/singlestore/sample_profiles.yml). It is almost a standard profile for SQL data sources in dbt.
In SingleStore, like in MySQL, `database` and `schema` denote the same concept. Therefore, you only need to specify `schema` in your dbt profile. `database` is set to `NULL` internally.
 
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
Administration    | Hooks             |            | No 
Models            | Custom schema     |            | No

## Testing

Default dbt [test suite](test/singlestore.dbtspec) is used to check the adapter functionality. [Development](Development.md) overview has a section "Run tests" which contains instructions on running the tests. Currently, the tests have been successfully run for the following product versions:

Singlestre | dbt-core | pytest-dbt-adapter
-----------|----------|-------------------
7.6        | 1.0.1    | 0.6.0


## Contributors

We thank [Doug Beatty](https://github.com/dbeatty10) who build an adpater for mysql which has been used to build an initial version of this adapter.

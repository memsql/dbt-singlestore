<p align="center">
    <img
        src="https://raw.githubusercontent.com/dbt-labs/dbt/ec7dee39f793aa4f7dd3dae37282cc87664813e4/etc/dbt-logo-full.svg"
        alt="dbt logo"
        width="500"
    />
</p>

<p align="center">
    <a href="https://pypi.org/project/dbt-singlestore/">
        <img src="https://badge.fury.io/py/dbt-singlestore.svg" />
    </a>
    <a target="_blank" href="https://pypi.org/project/dbt-singlestore/" style="background:none">
        <img src="https://img.shields.io/pypi/pyversions/dbt-singlestore">
    </a>
    <a href="https://github.com/psf/black">
        <img src="https://img.shields.io/badge/code%20style-black-000000.svg" />
    </a>
    <a href="https://github.com/python/mypy">
        <img src="https://www.mypy-lang.org/static/mypy_badge.svg" />
    </a>
    <a href="https://pepy.tech/project/dbt-singlestore">
        <img src="https://static.pepy.tech/badge/dbt-singlestore/month" />
    </a>
</p>


# dbt

**[dbt](https://www.getdbt.com/)** enables data analysts and engineers to transform their data using the same practices that software engineers use to build applications.

dbt is the T in ELT. Organize, cleanse, denormalize, filter, rename, and pre-aggregate the raw data in your warehouse so that it's ready for analysis.


# dbt-singlestore

`dbt-singlestore` enables dbt to work with [SingleStore](https://www.singlestore.com/).
For more information on using dbt with SingleStore, consult [the docs](https://docs.getdbt.com/docs/core/connect-data-platform/singlestore-setup).

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

## SingleStore-specific table options

### Reference tables

SingleStore supports **REFERENCE** tables, which are replicated across the cluster and are useful for small/dimension tables that are frequently joined.

To create a **REFERENCE** table from a dbt model, set `reference=true` on a `table` materialization:

```sql
{{
    config(
        materialized='table',
        reference=true,
    )
}}

select ...
```

When `reference=true` (default `false`), the adapter generates `CREATE REFERENCE TABLE ...` rather than a regular `CREATE TABLE ...`

### Rowstore reference tables

If you want a rowstore reference table, set `storage_type='rowstore'`:

```sql
{{
    config(
        materialized='table',
        reference=true,
        storage_type='rowstore',
    )
}}

select ...
```

This maps to `CREATE ROWSTORE REFERENCE TABLE ...`

### Restrictions / validation

To match SingleStore semantics, `dbt-singlestore` enforces:

- No `shard_key` with `reference=true` — reference tables don’t use sharding; compilation fails if both are set.

- No `temporary=true` with `reference=true` — SingleStore does not support temporary reference tables; compilation fails if both are set.

## Testing and supported versions

Default dbt tests and jaffle_shop project are used to check the adapter functionality. [Development](Development.md) overview has a section "Run tests" which contains instructions on running the tests. Currently, the tests have been successfully run for the following product versions:

SingleStore  | dbt-core  | dbt-tests-adapter
-------------|-----------|-------------------
9.0.9        | 1.10.13   | 1.19.1
8.9.10       | 1.10.13   | 1.19.1
8.7.43       | 1.9.9     | 1.17.0
8.7.43       | 1.8.13    | 1.11.0
8.5.16       | 1.7.15    | 1.7.15
8.5.16       | 1.6.13    | 1.6.13
8.1.1        | 1.6.0     | 1.6.0
8.1.1        | 1.5.0     | 1.5.0
8.1.1        | 1.4.0     | 1.4.0
8.1.1        | 1.3.0     | 1.3.0
7.8.29       | 1.2.2     | 1.2.2

SingleStore | dbt-core | pytest-dbt-adapter
------------|----------|-------------------
7.8.3       | 1.1.1    | 0.6.0
7.6.6       | 1.0.1    | 0.6.0
7.5.12      | 1.0.1    | 0.6.0

To use this adapter, SingleStore must be upgraded to the version 8.5 or newer.

## Contributors

We thank [Doug Beatty](https://github.com/dbeatty10) who build an adapter for mysql which has been used to build an initial version of this adapter.

## Contribute

- Want to help us build `dbt-singlestore`? Check out the [Contributing Guide](CONTRIBUTING.md).
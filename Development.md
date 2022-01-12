
# Adapter development
In the steps below, `~/.env3` is used a virtualenv directory, and `~/github.com` is used as a location of `dbt-singlestore` folder containing these sources. Certain parts of code have been copied from 
https://github.com/dbeatty10/dbt-mysql and susequently modified.

## Prepare environment
```
virtualenv ~/.env3 -p /usr/bin/python3;
source ~/.env3/bin/activate;
pip install dbt-core PyMySQL=1.0.2;
```

## Run tests
1. Install test package
    ```
    pip install pytest-dbt-adapter
    ```
2. Run SingleStore server instance locally or use Managed Service instance.
3. Fill credentails in `test/singlestore.dbtspec` or in ENV variables referenced there.
4. A database named `dbt_test` is used in tests. Snapshot tests expect it to be empty before running the tests. Therefore, prior to running the test suite, the following SQL must be executed:
    ```
    DROP DATABASE IF EXISTS dbt_test; CREATE DATABASE dbt_test;
    ```
5. Run the tests:
    ```
    cd ~/github.com/dbt-singlestore;
    pytest test/singlestore.dbtspec --no-drop-schema
    ```
    - append `--pdb` to debug
    - append `-k test_dbt_base` to run a specific test


## Initial repository setup
In order to get the file structure for this repository, the following commands have been used

```
virtualenv ~/.env3 -p /usr/bin/python3;
source ~/.env3/bin/activate;
pip install dbt-core dbt-postgres;
pip install PyMySQL=1.0.2;
cd <PATH_TO_DBT_CORE>/core/scripts;
python create_adapter_plugins.py --sql --title-case=SingleStore ~/github.com/ singlestore;
mv  ~/github.com/singlestore ~/github.com/dbt-singlestore;
git init;
mkdir -p build  # folder for artifacts and test scripts
```

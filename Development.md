
# Adapter development
In the steps below, `~/.env3` is used a virtualenv directory, and `~/github.com` is used as a location of `dbt-singlestore` folder containing these sources. Certain parts of code have been copied from 
https://github.com/dbeatty10/dbt-mysql and subsequently modified.

## Prepare environment
Replace dbt-core and dbt-tests-adapter values in the dev_requirements.txt file with the version you want to work with 
```
pip install virtualenv &&
virtualenv ~/.env3 -p /usr/bin/python3 &&
source ~/.env3/bin/activate
```

## Run tests
1. Install test packages
    ```
    pip install -r dev_requirements.txt
    ```
2. Run SingleStore server instance locally or use Managed Service instance.
3. Create a file named `test.env` and fill credentials in ENV variables referenced in `tests/conftest.py`.
4. A database named `dbt_test` is used in tests. Snapshot tests expect it to be empty before running the tests. Therefore, prior to running the test suite, the following SQL must be executed:
    ```
    DROP DATABASE IF EXISTS dbt_test; CREATE DATABASE dbt_test;
    ```
5. Run the tests:
    ```
    pytest
    ```
    - append `--pdb` to debug
    - append `-k <test_name>` to run a specific test
    Example: `pytest -k TestSingularTestsMyAdapter --pdb`


## Initial repository setup
In order to get the file structure for this repository, the following commands have been used

```
virtualenv ~/.env3 -p /usr/bin/python3;
source ~/.env3/bin/activate;
pip install dbt-core dbt-postgres;
pip install singlestoredb=1.2.0;
cd <PATH_TO_DBT_CORE>/core/scripts;
python create_adapter_plugins.py --sql --title-case=SingleStore ~/github.com/ singlestore;
mv  ~/github.com/singlestore ~/github.com/dbt-singlestore;
git init;
mkdir -p build  # folder for artifacts and test scripts
```

## Package connector for distribution

```
pip3 install setuptools --upgrade
pip3 install --upgrade build twine
python3 setup.py sdist; python3 setup.py bdist_wheel --universal; twine upload dist/*
```

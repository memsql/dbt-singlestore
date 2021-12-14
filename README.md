## Prepare environemnt
```
virtualenv ~/.env3 -p /usr/bin/python3;
source ~/.env3/bin/activate;
pip install dbt-core dbt-postgres PyMySQL=1.0.2;
```

## Run tests
```
pip install pytest-dbt-adapter
```
Fill credentails in `test/singlestore.dbtspec` or in ENV variables.
Run `pytest test/singlestore.dbtspec --no-drop-schema`.
- append `--pdb` to debug
- append `-k test_dbt_base` to run a specific test


## Initial setup
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

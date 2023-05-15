import pytest
import os

# Import the standard functional fixtures as a plugin
# Note: fixtures with session scope need to be local
pytest_plugins = ["dbt.tests.fixtures.project"]


# The profile dictionary, used to write out profiles.yml
# dbt will supply a unique schema per test, so we do not specify 'schema' here
@pytest.fixture(scope="class")
def dbt_profile_target():
    return {
        'type': 'singlestore',
        'threads': 1,
        'host': os.getenv('S2_HOST', '127.0.0.1'),
        'port': int(os.getenv('S2_PORT', 3306)),
        'user': os.getenv('S2_USER', 'root'),
        'password': os.getenv('S2_PASSWORD', 'p'),
        'database': 'dbt_test',
    }

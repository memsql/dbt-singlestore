import os
import unittest
import pytest

from dbt.adapters.singlestore import (
    SingleStoreCredentials,
    SingleStoreConnectionManager)
from dbt.contracts.connection import Connection
from dbt.adapters.singlestore import __version__


class SingleStoreConnectionManagerTest(unittest.TestCase):
    def setUp(self):
        self.credentials = SingleStoreCredentials(
            host=os.getenv('S2_HOST', '127.0.0.1'),
            port=int(os.getenv('S2_PORT', 3306)),
            user=os.getenv('S2_USER', 'root'),
            password=os.getenv('S2_PASSWORD', 'p'),
            database="dbt_test",
            schema="dbt_test",
            conn_attrs="{'my_key_1': 'my_val_1', 'my_key_2': 'my_val_2'}",
        )
        self.connection = Connection("singlestore", None, self.credentials)

    def test_connection_attributes(self):
        conn = SingleStoreConnectionManager.open(self.connection)
        cursor = conn.handle.cursor()
        sql = "select * from information_schema.mv_connection_attributes"
        cursor.execute(sql)

        actual_attributes = {}
        for row in cursor.fetchall():
            attribute_key = row[2]
            attribute_value = row[3]
            actual_attributes[attribute_key] = attribute_value

        expected_attributes = {
            "_connector_name": "dbt-singlestore",
            "_connector_version": __version__.version,
            "my_key_1": "my_val_1",
            "my_key_2": "my_val_2"
        }

        assert set(expected_attributes.keys()).issubset(actual_attributes.keys())
        assert set(expected_attributes.values()).issubset(actual_attributes.values())
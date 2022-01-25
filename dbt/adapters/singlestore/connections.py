import pymysql

from contextlib import contextmanager
from dataclasses import dataclass
from pymysql.cursors import Cursor

import dbt.exceptions
from dbt.adapters.base import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.contracts.connection import AdapterResponse
from dbt.logger import GLOBAL_LOGGER as logger


DUMMY_RESPONSE_CODE = 0


@dataclass
class SingleStoreCredentials(Credentials):
    # Add credentials members here, like:
    host: str = 'localhost'
    port: int = 3306
    user: str = 'root'
    password: str = ''
    database: str
    schema: str

    ALIASES = {
        'db': 'database',
        'username': 'user'
    }

    @property
    def type(self):
        return 'singlestore'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        # Omit fields like 'password'!
        return 'host', 'port', 'user', 'database', 'schema'


class SingleStoreConnectionManager(SQLConnectionManager):
    TYPE = 'singlestore'

    @classmethod
    def get_credentials(cls, credentials):
        if not credentials.database or not credentials.schema:
            raise dbt.exceptions.Exception("database and schema must be specified in the project config")

        return credentials

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        try:
            handle = pymysql.connect(
                user=credentials.user,
                password=credentials.password,
                host=credentials.host,
                port=credentials.port,
                database=credentials.database
            )

            connection.handle = handle
            connection.state = "open"
        except pymysql.Error as e:
            logger.debug(
                "Got an error when attempting to open a "
                "connection: '{}'".format(e)
            )

            connection.handle = None
            connection.state = "fail"

            raise dbt.exceptions.FailedToConnectException(str(e))

        return connection

    @classmethod
    def get_response(cls, cursor: Cursor) -> AdapterResponse:
        return AdapterResponse(
            _message="{}".format(f"OK. Rows affected: {cursor.rowcount}"),
            rows_affected=cursor.rowcount,
            code=DUMMY_RESPONSE_CODE
        )

    def cancel(self, connection):
        pass

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except pymysql.DatabaseError as e:
            logger.debug('Database error: {}'.format(str(e)))
            raise dbt.exceptions.DatabaseException(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            raise dbt.exceptions.RuntimeException(e) from e

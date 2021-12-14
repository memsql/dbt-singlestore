import pymysql

from contextlib import contextmanager
from dataclasses import dataclass
from pymysql.cursors import Cursor
from typing import Optional

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
    port: Optional[int] = 3306
    username: Optional[str] = 'root'
    password: Optional[str] = ''
    database: Optional[str] = None
    schema: Optional[str] = ''

    ALIASES = {
        'db': 'database',
    }

    @property
    def type(self):
        return 'singlestore'

    def _connection_keys(self):
        # return an iterator of keys to pretty-print in 'dbt debug'.
        # Omit fields like 'password'!
        return 'host', 'port', 'database', 'schema', 'username'


class SingleStoreConnectionManager(SQLConnectionManager):
    TYPE = 'singlestore'

    @classmethod
    def get_credentials(cls, credentials):
        if not credentials.schema:
            credentials.schema = credentials.database
        return credentials

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        try:
            handle = pymysql.connect(
                user=credentials.username,
                password=credentials.password,
                host=credentials.host,
                port=credentials.port,
                database=credentials.schema
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
            _message="{}".format(cursor.description),
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

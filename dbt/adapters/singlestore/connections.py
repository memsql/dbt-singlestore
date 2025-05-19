import ast

import singlestoredb

from contextlib import contextmanager
from dataclasses import dataclass
from singlestoredb.connection import Cursor
import singlestoredb.types as st
from typing import Optional

import dbt_common.exceptions
from dbt.adapters.contracts.connection import Credentials
from dbt.adapters.sql import SQLConnectionManager
from dbt.adapters.contracts.connection import AdapterResponse
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.singlestore import __version__

DUMMY_RESPONSE_CODE = 0

logger = AdapterLogger("SingleStore")

@dataclass
class SingleStoreCredentials(Credentials):
    # Add credentials members here, like:
    host: str = 'localhost'
    port: int = 3306
    user: str = 'root'
    password: str = ''
    database: str
    schema: Optional[str] = None
    retries: int = 1
    conn_attrs: Optional[str] = None

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

    @property
    def unique_field(self):
        return 'SingleStore'


class SingleStoreConnectionManager(SQLConnectionManager):
    TYPE = 'singlestore'

    @classmethod
    def get_credentials(cls, credentials):
        if not credentials.database:
            raise dbt_common.exceptions.DbtConfigError("database must be specified in the project config")

        return credentials

    @classmethod
    def open(cls, connection):
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = cls.get_credentials(connection.credentials)

        parsed_conn_attrs = {}
        if credentials.conn_attrs is not None:
            try:
                parsed_conn_attrs = ast.literal_eval(credentials.conn_attrs)
            except ValueError as e:
                raise dbt_common.exceptions.DbtRuntimeError(
                    "Invalid value for conn_attrs value in SingleStoreCredential class.\nPlease, make sure it is "
                    "formatted as a string that represents a dictionary, e.g. \"{'key1': 'value1', 'key2': 'value2', "
                    "'key3': 'value3'}\""
                )

        def connect():
            return singlestoredb.connect(
                user=credentials.user,
                password=credentials.password,
                host=credentials.host,
                port=credentials.port,
                database=credentials.database,
                conn_attrs={**parsed_conn_attrs, '_connector_name': 'dbt-singlestore', '_connector_version': __version__.version},
                multi_statements=True
            )

        retryable_exceptions = [
            singlestoredb.OperationalError,
            singlestoredb.DatabaseError
        ]

        return cls.retry_connection(
            connection,
            connect=connect,
            logger=logger,
            retry_limit=credentials.retries,
            retryable_exceptions=retryable_exceptions,
        )

    @classmethod
    def get_response(cls, cursor: Cursor) -> AdapterResponse:
        return AdapterResponse(
            _message="{}".format(f"OK. Rows affected: {cursor.rowcount}"),
            rows_affected=cursor.rowcount,
            code=DUMMY_RESPONSE_CODE
        )

    def _get_aggregator_id(self):
        sql = "SELECT @@aggregator_id"
        _, cursor = self.add_query(sql)
        res = cursor.fetchone()[0]
        return res

    def cancel(self, connection):
        connection_name = connection.name
        query_id = connection.handle.thread_id()
        aggregator_id = self._get_aggregator_id()
        kill_sql = f"kill query {query_id} {aggregator_id}"
        logger.debug("Cancelling query {} (internal node id {}) of connection '{}'".format(query_id, aggregator_id, connection_name))
        self.execute(kill_sql)

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except singlestoredb.DatabaseError as e:
            logger.debug('Database error: {}'.format(str(e)))
            raise dbt_common.exceptions.DbtDatabaseError(str(e).strip()) from e

        except Exception as e:
            logger.debug("Error running SQL: {}", sql)
            raise dbt_common.exceptions.DbtRuntimeError(e) from e

    @classmethod
    def data_type_code_to_name(cls, type_code: int) -> str:
        return st.ColumnType.get_name(type_code)

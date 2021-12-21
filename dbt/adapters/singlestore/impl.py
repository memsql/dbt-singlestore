import agate

from concurrent.futures import Future
from typing import List, Iterable, Dict, Any, Optional

from dbt.adapters.singlestore import SingleStoreConnectionManager
from dbt.adapters.singlestore.column import SingleStoreColumn
from dbt.adapters.singlestore.relation import SingleStoreRelation

from dbt.adapters.base import BaseRelation
from dbt.adapters.base.impl import catch_as_completed
from dbt.adapters.sql import SQLAdapter
from dbt.clients.agate_helper import DEFAULT_TYPE_TESTER
from dbt.exceptions import RuntimeException, raise_compiler_error
from dbt.logger import GLOBAL_LOGGER as logger
from dbt.utils import executor


class SingleStoreAdapter(SQLAdapter):
    ConnectionManager = SingleStoreConnectionManager
    Relation = SingleStoreRelation
    Column = SingleStoreColumn

    @classmethod
    def date_function(cls):
        return "NOW()"

    @classmethod
    def convert_datetime_type(
            cls, agate_table: agate.Table, col_idx: int
    ) -> str:
        return "datetime(6)"

    @classmethod
    def is_cancelable(cls):
        return False

    def quote(self, identifier):
        return '`{}`'.format(identifier)

    def get_columns_in_relation(self, relation: SingleStoreRelation
                                ) -> List[SingleStoreColumn]:
        rows: List[agate.Row] = super().get_columns_in_relation(relation)
        return self.parse_show_columns(relation, rows)

    def parse_show_columns(
            self,
            relation: SingleStoreRelation,
            raw_rows: List[agate.Row]) -> List[SingleStoreColumn]:
        return [SingleStoreColumn(
            table_schema=relation.schema,
            table_name=relation.name,
            table_type=relation.type,
            table_owner=None,
            table_stats=None,
            column=column.column,
            column_index=idx,
            dtype=column.dtype,
        ) for idx, column in enumerate(raw_rows)]

    def list_relations_without_caching(
        self, schema_relation: SingleStoreRelation
    ) -> List[SingleStoreRelation]:
        kwargs = {'schema_relation': schema_relation}
        try:
            results = self.execute_macro(
                'list_relations_without_caching',
                kwargs=kwargs
            )
        except RuntimeException as e:
            errmsg = getattr(e, 'msg', '')
            if f"Database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        relations = []
        for row in results:
            if len(row) != 4:
                raise RuntimeException(
                    f'Invalid value from "singlestore__list_relations_without_caching({kwargs})", '
                    f'got {len(row)} values, expected 4'
                )
            _, name, _schema, relation_type = row
            relation = self.Relation.create(
                schema=_schema,
                identifier=name,
                type=relation_type
            )
            relations.append(relation)

        return relations

    def _get_columns_for_catalog(
        self, relation: SingleStoreRelation
    ) -> Iterable[Dict[str, Any]]:
        columns = self.get_columns_in_relation(relation)

        for column in columns:
            # convert SingleStoreColumns into catalog dicts
            as_dict = column.to_dict()
            as_dict['column_name'] = as_dict.pop('column', None)
            as_dict['column_type'] = as_dict.pop('dtype')
            as_dict['table_database'] = None
            yield as_dict

    def get_relation(
        self, database: str, schema: str, identifier: str
    ) -> Optional[BaseRelation]:
        if not self.Relation.include_policy.database:
            database = None

        return super().get_relation(database, schema, identifier)

    def get_catalog(self, manifest):
        schema_map = self._get_catalog_schemas(manifest)
        if len(schema_map) > 1:
            raise_compiler_error(
                f'Expected only one database in get_catalog, found '
                f'{list(schema_map)}'
            )

        with executor(self.config) as tpe:
            futures: List[Future[agate.Table]] = []
            for info, schemas in schema_map.items():
                for schema in schemas:
                    futures.append(tpe.submit_connected(
                        self, schema,
                        self._get_one_catalog, info, [schema], manifest
                    ))
            catalogs, exceptions = catch_as_completed(futures)
        return catalogs, exceptions

    def _get_one_catalog(
        self, information_schema, schemas, manifest,
    ) -> agate.Table:
        if len(schemas) != 1:
            raise_compiler_error(
                f'Expected only one schema in _get_one_catalog, found '
                f'{schemas}'
            )

        database = information_schema.database
        schema = list(schemas)[0]

        columns: List[Dict[str, Any]] = []
        for relation in self.list_relations(database, schema):
            logger.debug("Getting table schema for relation {}", relation)
            columns.extend(self._get_columns_for_catalog(relation))
        return agate.Table.from_object(
            columns, column_types=DEFAULT_TYPE_TESTER
        )

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(
            'list_schemas',
            kwargs={'database': database}
        )

        exists = True if schema in [row[0] for row in results] else False
        return exists

    # Methods used in adapter tests
    def update_column_sql(
        self,
        dst_name: str,
        dst_column: str,
        clause: str,
        where_clause: Optional[str] = None,
    ) -> str:
        clause = f'update {dst_name} set {dst_column} = {clause}'
        if where_clause is not None:
            clause += f' where {where_clause}'
        return clause

    def timestamp_add_sql(
        self, add_to: str, number: int = 1, interval: str = 'hour'
    ) -> str:
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in postgres/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"date_add({add_to}, interval {number} {interval})"

    def string_add_sql(
        self, add_to: str, value: str, location='append',
    ) -> str:
        if location == 'append':
            return f"concat({add_to}, '{value}')"
        elif location == 'prepend':
            return f"concat({value}, '{add_to}')"
        else:
            raise RuntimeException(
                f'Got an unexpected location value of "{location}"'
            )

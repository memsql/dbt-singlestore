import agate
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Any

from dbt.adapters.singlestore import SingleStoreConnectionManager
from dbt.adapters.singlestore.column import SingleStoreColumn
from dbt.adapters.singlestore.relation import SingleStoreRelation

from dbt.adapters.base.meta import available
from dbt.adapters.sql import SQLAdapter
from dbt.dataclass_schema import dbtClassMixin, ValidationError
from dbt.exceptions import DbtRuntimeError, CompilationError
from dbt.logger import GLOBAL_LOGGER as logger

import dbt.utils


@dataclass
class SingleStoreIndexConfig(dbtClassMixin):
    columns: List[str]
    unique: bool = False
    type: Optional[str] = None

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run. See
        # https://github.com/dbt-labs/dbt-core/issues/1945#issuecomment-576714925
        # for an explanation.
        now = datetime.utcnow().isoformat()
        inputs = self.columns + [relation.render(), str(self.unique), str(self.type), now]
        string = "_".join(inputs)
        return "index_" + dbt.utils.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["SingleStoreIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            msg = dbt.exceptions.validator_error_message(exc)
            raise CompilationError(f"Could not parse index config: {msg}")
        except TypeError:
            raise CompilationError(f"Invalid index config:\n  Got: {raw_index}\n"
                                 f"  Expected a dictionary with at minimum a \"columns\" key")


class SingleStoreAdapter(SQLAdapter):
    ConnectionManager = SingleStoreConnectionManager
    Relation = SingleStoreRelation
    Column = SingleStoreColumn

    @classmethod
    def date_function(cls):
        return "now()"

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
        return self._parse_show_columns(relation, rows)

    @available
    def parse_index(self, raw_index: Any) -> Optional[SingleStoreIndexConfig]:
        return SingleStoreIndexConfig.parse(raw_index)

    def _parse_show_columns(
            self,
            relation: SingleStoreRelation,
            raw_rows: List[agate.Row]) -> List[SingleStoreColumn]:
        return [SingleStoreColumn(
            table_database=relation.database,
            table_schema=relation.schema,
            table_name=relation.name,
            table_type=relation.type,
            table_owner=None,
            table_stats=None,
            column=column.column,
            column_index=idx,
            dtype=column.dtype,
        ) for idx, column in enumerate(raw_rows)]

    def drop_schema(self, relation: SingleStoreRelation) -> None:
        # in SingleStore, schema does not have a physical representation in the database
        # so we don't execute DROP SCHEMA macro, only update the cache
        self.cache.drop_schema(relation.database, relation.schema)

    def list_relations_without_caching(
        self, schema_relation: SingleStoreRelation
    ) -> List[SingleStoreRelation]:
        kwargs = {'schema_relation': schema_relation}
        try:
            results = self.execute_macro(
                'list_relations_without_caching',
                kwargs=kwargs
            )
        except DbtRuntimeError as e:
            description = "Error while retrieving information about"
            logger.debug(f"{description} {schema_relation}: {e.msg}")
            return []

        relations = []
        for row in results:
            if len(row) != 4:
                raise DbtRuntimeError(
                    f'Invalid value from "singlestore__list_relations_without_caching({kwargs})", '
                    f'got {len(row)} values, expected 4'
                )
            database, name, schema, relation_type = row
            relation = self.Relation.create(
                database=database,
                schema=schema,
                identifier=name,
                type=relation_type
            )
            relations.append(relation)

        return relations

    def check_schema_exists(self, database, schema):
        return True

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
            raise DbtRuntimeError(
                f'Got an unexpected location value of "{location}"'
            )

    def valid_incremental_strategies(self):
        return ["delete+insert"]

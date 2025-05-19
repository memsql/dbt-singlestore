import agate
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from typing import List, Optional, Any, Dict

from dbt.adapters.singlestore import SingleStoreConnectionManager
from dbt.adapters.singlestore.column import SingleStoreColumn
from dbt.adapters.singlestore.relation import SingleStoreRelation

from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base.meta import available
from dbt.adapters.capability import CapabilityDict, CapabilitySupport, Support, Capability
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import ColumnLevelConstraint, ConstraintType, ModelLevelConstraint
from dbt_common.dataclass_schema import dbtClassMixin, ValidationError
from dbt_common.exceptions import DbtRuntimeError, CompilationError
from dbt.adapters.events.logging import AdapterLogger

import dbt_common.utils as utils

logger = AdapterLogger("SingleStore")

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
        return "index_" + utils.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["SingleStoreIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            msg = DbtRuntimeError(raw_index).validator_error_message(exc)
            raise CompilationError(f"Could not parse index config: {msg}")
        except TypeError:
            raise CompilationError(f"Invalid index config:\n  Got: {raw_index}\n"
                                 f"  Expected a dictionary with at minimum a \"columns\" key")


class SingleStoreAdapter(SQLAdapter):
    ConnectionManager = SingleStoreConnectionManager
    Relation = SingleStoreRelation
    Column = SingleStoreColumn

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    _capabilities: CapabilityDict = CapabilityDict(
        {
            Capability.SchemaMetadataByRelations: CapabilitySupport(support=Support.Full),
            Capability.TableLastModifiedMetadata: CapabilitySupport(support=Support.Full),
        }
    )

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
        return True

    @classmethod
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

    @classmethod
    def render_raw_model_constraints(cls, raw_constraints: List[Dict[str, Any]], undefined_shard_key: bool = True) -> List[str]:
        partial_render_raw_model_constraint = partial(cls.render_raw_model_constraint, undefined_shard_key=undefined_shard_key)
        return [c for c in map(partial_render_raw_model_constraint, raw_constraints) if c is not None]

    @classmethod
    def render_raw_model_constraint(cls, raw_constraint: Dict[str, Any], undefined_shard_key: bool = True) -> Optional[str]:
        constraint = cls._parse_model_constraint(raw_constraint)
        partial_render_model_constraint = partial(cls.render_model_constraint, undefined_shard_key=undefined_shard_key)
        return cls.process_parsed_constraint(constraint, partial_render_model_constraint)

    @classmethod
    def render_model_constraint(cls, constraint: ModelLevelConstraint, undefined_shard_key: bool = True) -> Optional[str]:
        """We're overriding this method because when the shard key is not defined and the unique key is
           defined, we want them to be the same due to the SingleStoreDB internal restrictions"""
        constraint_prefix = f"constraint {constraint.name} " if constraint.name else ""
        column_list = ", ".join(constraint.columns)
        if constraint.type == ConstraintType.check and constraint.expression:
            return f"{constraint_prefix}check ({constraint.expression})"
        elif constraint.type == ConstraintType.unique:
            constraint_expression = f" {constraint.expression}" if constraint.expression else ""
            shard_key = f"shard key ({column_list})" if undefined_shard_key else ""
            return f"{constraint_prefix}unique key{constraint_expression} ({column_list}),\n {shard_key}"
        elif constraint.type == ConstraintType.primary_key:
            constraint_expression = f" {constraint.expression}" if constraint.expression else ""
            return f"{constraint_prefix}primary key{constraint_expression} ({column_list})"
        elif constraint.type == ConstraintType.foreign_key and constraint.expression:
            return f"{constraint_prefix}foreign key ({column_list}) references {constraint.expression}"
        elif constraint.type == ConstraintType.custom and constraint.expression:
            return f"{constraint_prefix}{constraint.expression}"
        else:
            return None


    @available
    def check_for_constraint(cls, raw_model_constraints: List[Dict[str, Any]], raw_column_constraints: Dict[str, Dict[str, Any]], primary_key: bool):
        constraint_type = ConstraintType.primary_key if primary_key else ConstraintType.unique
        for raw_constraint in raw_model_constraints:
            constraint = cls._parse_model_constraint(raw_constraint)
            if constraint.type == constraint_type:
                return True

        for raw_constraint in raw_column_constraints.values():
            for con in raw_constraint.get("constraints", None):
                constraint = cls._parse_column_constraint(con)
                if constraint.type == constraint_type:
                    return True

        return False


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
        return ["delete+insert", "append"]

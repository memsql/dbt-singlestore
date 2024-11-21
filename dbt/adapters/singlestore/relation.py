from dataclasses import dataclass, field

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt_common.exceptions import DbtRuntimeError


@dataclass
class SingleStoreQuotePolicy(Policy):
    database: bool = True
    schema: bool = False
    identifier: bool = True


@dataclass
class SingleStoreIncludePolicy(Policy):
    database: bool = True
    schema: bool = False
    identifier: bool = True


@dataclass(frozen=True, eq=False, repr=False)
class SingleStoreRelation(BaseRelation):
    quote_policy: Policy = field(default_factory=lambda: SingleStoreQuotePolicy())
    include_policy: Policy = field(default_factory=lambda: SingleStoreIncludePolicy())
    quote_character: str = '`'

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise DbtRuntimeError(
                "Got a relation with schema and database set to True"
                "but only one can be set"
            )
        return super().render()

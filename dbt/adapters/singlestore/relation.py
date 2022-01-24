from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation, Policy
from dbt.exceptions import RuntimeException


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
    quote_policy: SingleStoreQuotePolicy = SingleStoreQuotePolicy()
    include_policy: SingleStoreIncludePolicy = SingleStoreIncludePolicy()
    quote_character: str = '`'

    def render(self):
        if self.include_policy.database and self.include_policy.schema:
            raise RuntimeException(
                "Got a relation with schema and database set to True"
                "but only one can be set"
            )
        return super().render()

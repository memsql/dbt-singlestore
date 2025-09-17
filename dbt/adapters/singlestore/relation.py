from dataclasses import dataclass, field
from datetime import datetime, timezone

from dbt.adapters.base.relation import BaseRelation, Policy, EventTimeFilter
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

    def _norm(ts: str) -> str:
        s = ts.replace('T', ' ').replace('Z', '')
        s = re.sub(r'([+-]\d{2}:\d{2})$', '', s)   # drop timezone
        return s[:19]                              # 'YYYY-MM-DD HH:MM:SS'
    
    def _render_event_time_filtered(self, event_time_filter: EventTimeFilter) -> str:
        """
        Returns "" if start and end are both None
        """
        filter = ""
        if event_time_filter.start and event_time_filter.end:
            filter = f"({event_time_filter.field_name} :> TIMESTAMP) >= '{_norm(event_time_filter.start)}' and ({event_time_filter.field_name} :> TIMESTAMP) < '{_norm(event_time_filter.end)}'"
        elif event_time_filter.start:
            filter = (
                f"({event_time_filter.field_name} :> TIMESTAMP) >= '{_norm(event_time_filter.start)}'"
            )
        elif event_time_filter.end:
            filter = (
                f"({event_time_filter.field_name} :> TIMESTAMP) < '{_norm(event_time_filter.end)}'"
            )

        return filter

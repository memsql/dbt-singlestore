from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import TypeVar, Optional, Dict, Any

from dbt.adapters.base.column import Column

Self = TypeVar('Self', bound='SingleStoreColumn')


@dataclass_json
@dataclass
class SingleStoreColumn(Column):
    dtype: str = ""

    table_database: Optional[str] = None
    table_schema: Optional[str] = None
    table_name: Optional[str] = None
    table_type: Optional[str] = None
    table_owner: Optional[str] = None
    table_stats: Optional[Dict[str, Any]] = None
    column_index: Optional[int] = None

    def __post_init__(self):
        # normalize dtype once after instantiation
        object.__setattr__(self, "dtype", self._normalize(self.dtype))

    @property
    def quoted(self) -> str:
        return '`{}`'.format(self.column)

    def __repr__(self) -> str:
        return f"<SingleStoreColumn {self.name} ({self.data_type})>"

    @staticmethod
    def _normalize(dtype: str) -> str:
        if not dtype:
            return ""
        t = dtype.strip().lower()
        if "(" in t:
            t = t.split("(", 1)[0]
        if t == "integer": t = "int"
        if t == "real": t = "double"
        if t in ("numeric", "fixed", "dec"): t = "decimal"
        return t

    INTEGER = {"tinyint", "smallint", "mediumint", "int", "bigint"}
    FLOAT   = {"float", "double"}
    DEC     = {"decimal"}
    STRING  = {"char", "varchar", "text", "tinytext", "mediumtext", "longtext"}

    def is_integer(self) -> bool: return self.dtype in self.INTEGER
    def is_float(self)   -> bool: return self.dtype in self.FLOAT
    def is_numeric(self) -> bool: return self.dtype in self.DEC
    def is_string(self)  -> bool: return self.dtype in self.STRING

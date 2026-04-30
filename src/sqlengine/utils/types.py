import sqlite3
from typing import Protocol, Self

class CustomType(Protocol):
    @classmethod
    def from_sql(cls, sql : bytes) -> Self: ...
    def to_sql(self) -> str | int | float | None: ...


type SqlValue = str | int | float | CustomType | None
type SqlRow   = tuple[SqlValue, ...]


def register_type(cls : type[CustomType], type_name : str | None = None) -> None:

    if not hasattr(cls, "to_sql") or not hasattr(cls, "from_sql"):
        raise AttributeError(f"Provided class {cls.__name__} does not provide `to_sql` and/or `from_sql` methods")

    type_name = type_name if type_name else cls.__name__
    sqlite3.register_adapter(cls, lambda x: x.to_sql())
    sqlite3.register_converter(type_name, cls.from_sql)
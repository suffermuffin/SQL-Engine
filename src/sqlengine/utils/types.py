import sqlite3
from typing import Protocol, Self, Any, TypeGuard

class CustomType(Protocol):
    @classmethod
    def from_sql(cls, sql : bytes) -> Self: ...
    def to_sql(self) -> str | int | float | None: ...


type SqlValue = str | int | float | str | bytes | None | CustomType
type SqlRow   = tuple[SqlValue, ...]
type SqlType  = type[str | int | float | str | bytes | CustomType]


types_map : dict[str, str] = {
    int.__name__     : "INTEGER",
    float.__name__   : "REAL",
    str.__name__     : "TEXT",
    bytes.__name__   : "BLOB",
}


def is_custom_type(type_: SqlType) -> TypeGuard[type[CustomType]]:
    return (
        type_ not in (str, int, float, bytes)
        and hasattr(type_, "from_sql")
        and hasattr(type_, "to_sql")
    )


def register_type(cls : type[CustomType], type_name : str | None = None) -> None:

    if not is_custom_type(cls):
        raise AttributeError(f"Provided class `{cls.__name__}` does not provide `to_sql` and/or `from_sql` methods")

    type_name = type_name if type_name else cls.__name__
    sqlite3.register_adapter(cls, lambda x: x.to_sql())
    sqlite3.register_converter(type_name, cls.from_sql)
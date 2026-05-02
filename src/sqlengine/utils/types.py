import sqlite3
from typing import Protocol, Self, TypeGuard

class CustomType(Protocol):
    @classmethod
    def from_sql(cls, sql : bytes) -> Self: ...
    def to_sql(self) -> str | int | float | str | bytes | None: ...


type SqlValue = str | int | float | str | bytes | None | CustomType
type SqlRow   = tuple[SqlValue, ...]
type SqlType  = type[str | int | float | str | bytes | CustomType]


# https://docs.python.org/3/library/sqlite3.html#sqlite-and-python-types
types_map : dict[type, str] = {
    int     : "INTEGER",
    float   : "REAL",
    str     : "TEXT",
    bytes   : "BLOB",
}


def is_custom_type(type_: SqlType) -> TypeGuard[type[CustomType]]:
    return (
        type_ not in (str, int, float, bytes)
        and isinstance(type_, type)
        and hasattr(type_, "from_sql")
        and hasattr(type_, "to_sql")
    )


def register_type(cls : type[CustomType], type_name : str | None = None) -> None:

    if not is_custom_type(cls):
        raise AttributeError(f"Provided class `{cls.__name__}` does not provide `to_sql` and/or `from_sql` methods")

    type_name = type_name if type_name else cls.__name__
    sqlite3.register_adapter(cls, lambda x: x.to_sql())
    sqlite3.register_converter(type_name, cls.from_sql)
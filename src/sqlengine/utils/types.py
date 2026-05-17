import sqlite3
from typing import Protocol, Self, TypeGuard, TypedDict


class CustomType(Protocol):
    @classmethod
    def from_sql(cls, sql : bytes) -> Self: 
        """ Method that accepts bytes and returns object instance """
        ...
    def to_sql(self) -> str | int | float | str | bytes | None: 
        """ Method that converts object instance to native sqlite3 value """
        ...


type SqlValue = str | int | float | bytes | None | CustomType
type SqlRow   = tuple[SqlValue, ...]
type SqlType  = type[str | int | float | bytes | CustomType]


class Schema(TypedDict):
    tablename : str
    columns   : list[str]
    types     : list[SqlType | str]
    primary   : list[str]


# https://docs.python.org/3/library/sqlite3.html#sqlite-and-python-types
_TYPES_MAP : dict[type, str] = {
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
    """ 
    Register custom type to be able to store it in tables 
    
    Args:
        cls (CustomType): Class that implements `from_sql(cls, sql : bytes) -> Self` and `to_sql(self) -> str | int | float | str | bytes | None`
        type_name (Optional[str]): Colname that would be linked to this type
    """

    if not is_custom_type(cls):
        raise AttributeError(f"Provided class `{cls.__name__}` does not provide `to_sql` and/or `from_sql` methods")

    type_name = type_name if type_name else cls.__name__
    sqlite3.register_adapter(cls, lambda x: x.to_sql())
    sqlite3.register_converter(type_name, cls.from_sql)


def pytype_to_sqltype(type_ : type) -> str:
    """ Converts python type to sql type """
    if type_ not in _TYPES_MAP:
        raise TypeError(f"{type_} is not natively supported by sqlite3")
    
    return _TYPES_MAP[type_]
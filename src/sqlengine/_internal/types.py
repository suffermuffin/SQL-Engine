import sqlite3
from typing import Protocol, Self, TypeGuard, TypedDict, Any
from types import UnionType

class CustomType(Protocol):
    @classmethod
    def from_sql(cls, sql : bytes) -> Self: 
        """ Method that accepts bytes and returns object instance """
        ...
    def to_sql(self) -> str | int | float | str | bytes | None: 
        """ Method that converts object instance to native sqlite3 value """
        ...


type SqlValue   = str | int | float | bytes | None | CustomType
type SqlRow     = tuple[SqlValue, ...]
type SqlType    = type[str | int | float | bytes | CustomType]
type ColumnType = SqlType | str | UnionType


class Schema(TypedDict):
    tablename : str
    columns   : list[str]
    types     : list[ColumnType]
    primary   : list[str]


class Primary[T]:
    __slots__ = ()


# https://docs.python.org/3/library/sqlite3.html#sqlite-and-python-types
_TYPES_MAP : dict[type | UnionType, str] = {
    int     : "INTEGER NOT NULL",
    float   : "REAL NOT NULL",
    str     : "TEXT NOT NULL",
    bytes   : "BLOB NOT NULL",
    None | int   : "INTEGER",
    None | float : "REAL",
    None | str   : "TEXT",
    None | bytes : "BLOB",
}


def is_custom_type(type_: SqlType | UnionType) -> TypeGuard[type[CustomType]]:
    return (
        type_ not in (str, int, float, bytes)
        and isinstance(type_, type)
        and hasattr(type_, "from_sql")
        and hasattr(type_, "to_sql")
    )


def register_type(cls : type[CustomType], type_name : str | None = None) -> None:
    """ 
    Register custom type into sqlite3 to be able to store it in tables
    
    Args:
        cls (CustomType): Class that implements `from_sql(cls, sql : bytes) -> Self` and `
            to_sql(self) -> str | int | float | str | bytes | None`
        type_name (str | None): Colname that would be linked to this type
    """

    type_name = type_name if type_name else cls.__name__
    sqlite3.register_adapter(cls, lambda x: x.to_sql())
    sqlite3.register_converter(type_name, cls.from_sql)


def pytype_to_sqltype(type_ : type | UnionType) -> str:
    """ Converts python type to sql type """
    if type_ not in _TYPES_MAP:
        raise TypeError(f"{type_} is not natively supported by sqlite3")
    
    return _TYPES_MAP[type_]


def register_resolve_types(types : list[ColumnType], **connection_params) -> tuple[list[str], dict[str, Any]]:
    """ Converts py types to sql types, registers custom types, resolves type names, updates connection params """

    resolved : list[str] = []
    assert_register_types = False

    for type_ in types:
        
        if isinstance(type_, str):
            resolved.append(type_)
            continue
        
        if is_custom_type(type_):
            ctname = type_.__name__.upper()
            
            register_type(type_, ctname)
            resolved.append(ctname)
            
            if not assert_register_types:
                assert_register_types = True
            continue 
        
        sql_type = pytype_to_sqltype(type_)
        resolved.append(sql_type)
    
    if assert_register_types and ("detect_types" not in connection_params):
        connection_params.update(dict(detect_types=sqlite3.PARSE_DECLTYPES))

    return resolved, connection_params
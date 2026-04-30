import os
import sqlite3
from typing import TypedDict, overload

from .sqltable import SqlTableMixin
from .utils.types import CustomType


class Schema(TypedDict):
    tablename : str
    columns   : list[str]
    types     : list[str | type[CustomType]]
    primary   : list[str]


def get_database_tablenames(database : str, cursor : sqlite3.Cursor | None = None) -> list[str]:

    query = (
        "SELECT name FROM sqlite_schema WHERE "
        "type = 'table' AND name NOT LIKE 'sqlite_%';"
    )

    if cursor:
        cursor.execute(query)
        names_raw = cursor.fetchall()

    else:
        with sqlite3.connect(database) as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            names_raw = cursor.fetchall()
    
    return [name[0] for name in names_raw if name[0] is not None]
        

def get_table_schema(database : str, tablename : str, cursor : sqlite3.Cursor | None = None) -> Schema | None:
    
    query = f"PRAGMA table_info({tablename});"

    if cursor:
        cursor.execute(query)
        column_types = cursor.fetchall()
    
    else:
        with sqlite3.connect(database) as conn:
            cur = conn.cursor()
            cur.execute(query)
            column_types = cur.fetchall()
    
    if not column_types:
        return None

    columns = [it[1] for it in column_types]
    types   = [it[2] for it in column_types]
    primary = [it[1] for it in column_types if it[-1] > 0]

    return Schema(tablename=tablename, columns=columns, types=types, primary=primary)


def get_database_schemas(database : str) -> list[Schema]:

    if not os.path.exists(database):
        raise FileNotFoundError(f"File not exists: {database}")
    
    with sqlite3.connect(database) as conn:
        cursor = conn.cursor()
        names  = get_database_tablenames(database, cursor)
        
        schemas : list[Schema] = []
        
        for tablename in names:
            schema = get_table_schema(database, tablename, cursor)
            
            if schema:
                schemas.append(schema)

    return schemas


def table_from_schema(database : str, schema : Schema, **kwargs) -> SqlTableMixin:
    """ Dynamically builds class from provided schema """
    
    new_class = type(
        schema["tablename"],
        (SqlTableMixin,),
        {
            "__tablename__" : schema["tablename"],
            "__columns__"   : schema["columns"],
            "__types__"     : schema["types"],
            "__primary__"   : schema["primary"],
        }
    )
    return new_class(database, **kwargs)


@overload
def table_from_database(database : str, tablename : str, **kwargs) -> SqlTableMixin: ...
@overload
def table_from_database(database : str, tablename : None = None, **kwargs) -> list[SqlTableMixin]: ...

def table_from_database(database : str, tablename : str | None = None, **kwargs) -> list[SqlTableMixin] | SqlTableMixin:
    """ Dynamically builds class from table schema in provided database """
    
    schemas = get_database_schemas(database)
    
    if len(schemas) < 1:
        raise sqlite3.DatabaseError(f"No schemas in {database}")
    
    if not tablename:
        return [table_from_schema(database, schema, **kwargs) for schema in schemas]
    
    schema_map = {schema["tablename"] : schema for schema in schemas}
    target_schema = schema_map.get(tablename, None)
    
    if not target_schema:
        raise sqlite3.DatabaseError(f"No tablename `{tablename}` in {database}. Available tables: {list(schema_map.keys())}")
    
    return table_from_schema(database, target_schema, **kwargs)

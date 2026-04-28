import sqlite3
import os
import warnings
from typing import TypedDict

from ..sqltable import SqlTableMixin, logger

class Schema(TypedDict):
    tablename : str
    columns   : list[str]
    types     : list[str]
    primary   : list[str]


def get_table_schema(database : str, tablename : str | None = None) -> Schema | None:

    warnings.warn("Experimental function `get_table_schema`, might be unreliable")

    if not os.path.exists(database):
        raise FileNotFoundError(f"File not exists: {database}")
    
    def resolve_name(schema : tuple[str]) -> str:
        cr_query = schema[0]
        tokens   = cr_query.split(" ")
        _cr      = tokens[0]
        _tb      = tokens[1]
        name     = tokens[2].replace("\"", "")
        assert _cr.upper() == "CREATE" and _tb.upper() == "TABLE", f"Can't resolve tablename: Unknown schema format `{cr_query}`"
        return name
    
    with sqlite3.connect(database) as conn:
        cur = conn.cursor()

        if tablename is None:
            cur.execute("SELECT sql FROM sqlite_schema;")
            schemas = cur.fetchall()
            names = [resolve_name(schema) for schema in schemas if schema[0] is not None]
            
            if len(names) > 1:
                raise sqlite3.DatabaseError(f"More then one table found in {database} \
                        and tablename not provided. Specify wich one: {names}")

            tablename = names[0]

        cur.execute(f"PRAGMA table_info({tablename});")
        
        column_types = cur.fetchall()
        logger.debug(f"Got column_types: {column_types}")

        if not column_types:
            return None

        columns = [it[1] for it in column_types]
        types   = [it[2] for it in column_types]
        primary = [it[1] for it in column_types if it[-1] > 0]

    return Schema(tablename=tablename, columns=columns, types=types, primary=primary)


def table_from_database(database : str, tablename : str | None = None, **kwargs) -> SqlTableMixin:
    """ Dynamically builds class from table schema in provided database """
    schema = get_table_schema(database, tablename)
    
    if not schema:
        raise ValueError(f"{database} (tablename={tablename}) returned empty schema")

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

from typing import Sequence

from .types import SqlValue


def create_table(
        tablename: str,
        columns  : list[str],
        types    : list[str],
        primary  : list[str]
    ) -> str:

    columns_types = ', '.join(
        [
            f'{col} {dtype}'
            for col, dtype 
            in zip(columns, types)
        ]
    )
    
    primary_keys = ', '.join(primary)
    
    return (
        f"CREATE TABLE IF NOT EXISTS {tablename} "
        f"({columns_types}, PRIMARY KEY ({primary_keys}));"
    )


def format_list(items : list[str], brakets : bool = True) -> str:
    """ Formats list into `(item1, item2, ...)` format """
    items_str = ', '.join(items)
    if not brakets: return items_str
    return f'({items_str})'


def drop_table(tablename : str) -> str:
    return f"DROP TABLE IF EXISTS {tablename}"


def values_placeholder(n_values : int) -> str:
    """ Creates placeholder `(?, ?, ?, ...)` with "?" `n_values` times """
    return format_list(['?']*n_values)


def bulk_placeholder(n_values : int, n_rows : int) -> str:
    """ Creates placeholders `(?, ?, ..), (?, ?, ..), ...` for each row """
    place_holder = values_placeholder(n_values)
    return f"{format_list([place_holder]*n_rows, False)}"


def insert(tablename : str, columns : list[str], placeholder : str):
    return f"INSERT INTO {tablename} {format_list(columns)} VALUES {placeholder};"


def insert_row(tablename : str, columns : list[str]) -> str:
    """ Creates insert query for 1 row """
    n_values = len(columns)
    placeholder = values_placeholder(n_values)
    return insert(tablename, columns, placeholder)


def bulk_insert(tablename : str, columns : list[str], n_rows : int) -> str:
    """ Creates insert query for multiple rows """
    n_values = len(columns)
    placeholder = bulk_placeholder(n_values, n_rows)
    return insert(tablename, columns, placeholder)


def delete_rows(tablename : str, where_clause : str) -> str:
    return f"DELETE FROM {tablename} WHERE {where_clause};"


def where_equals(column : str, equals : SqlValue | Sequence[SqlValue]) -> str:
    """ Generates where clause for provided `equals` values to look for equal values in `column` """
    
    if isinstance(equals, str):
        equals = f"\"{equals}\""
    
    if not isinstance(equals, (list, tuple)):
        return f'{column} = {equals}'

    equals_list = []
    for eq_item in equals:
        
        if isinstance(eq_item, str):
            equals_list.append(f"\"{eq_item}\"")
        else:
            equals_list.append(eq_item)

    return f'{column} in {format_list(equals_list)}'


def select(tablename : str, columns : str | list[str] = "*", where_clause : str | None = None) -> str:
    """ Creates select query """
    _columns = columns if isinstance(columns, str) else format_list(columns, False)
    
    query  = f"SELECT {_columns} FROM {tablename}"
    query += f" WHERE {where_clause}" if where_clause else ""
    query += ";"

    return query

